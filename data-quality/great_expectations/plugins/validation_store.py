"""
Custom validation result stores and utilities for data quality tracking.

Provides PostgreSQL-based storage for validation results to enable
historical tracking and reporting across pipeline runs.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from great_expectations.checkpoint.actions import ValidationAction

log = logging.getLogger(__name__)


class ValidationResultLogger:
    """Log validation results to PostgreSQL for tracking and reporting.

    This class provides methods to store and query Great Expectations
    validation results in a PostgreSQL database.
    """

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS data_platform.validation_results (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(256) NOT NULL,
        checkpoint_name VARCHAR(256),
        expectation_suite_name VARCHAR(256) NOT NULL,
        data_asset_name VARCHAR(256),
        validation_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        success BOOLEAN NOT NULL,
        total_expectations INTEGER,
        successful_expectations INTEGER,
        unsuccessful_expectations INTEGER,
        success_percent NUMERIC(5, 2),
        result_json TEXT,
        execution_date DATE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_validation_results_run_id
        ON data_platform.validation_results(run_id);
    CREATE INDEX IF NOT EXISTS idx_validation_results_suite
        ON data_platform.validation_results(expectation_suite_name);
    CREATE INDEX IF NOT EXISTS idx_validation_results_date
        ON data_platform.validation_results(execution_date);
    """

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        schema: str = "data_platform",
        table: str = "validation_results",
    ):
        """Initialize the validation result logger.

        Args:
            postgres_conn_id: Airflow PostgreSQL connection ID.
            schema: Database schema for the results table.
            table: Table name for storing results.
        """
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table

    def ensure_table_exists(self) -> None:
        """Create the validation results table if it doesn't exist."""
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            hook.run(self.CREATE_TABLE_SQL)
            log.info("Validation results table ensured")
        except Exception as e:
            log.warning(f"Could not create validation results table: {e}")

    def log_result(
        self,
        run_id: str,
        expectation_suite_name: str,
        success: bool,
        total_expectations: int,
        successful_expectations: int,
        checkpoint_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        result_json: Optional[Dict[str, Any]] = None,
        execution_date: Optional[datetime] = None,
    ) -> Optional[int]:
        """Log a validation result to the database.

        Args:
            run_id: Unique identifier for the validation run.
            expectation_suite_name: Name of the expectation suite.
            success: Whether the validation passed.
            total_expectations: Total number of expectations evaluated.
            successful_expectations: Number of expectations that passed.
            checkpoint_name: Name of the checkpoint (optional).
            data_asset_name: Name of the validated data asset (optional).
            result_json: Full result as JSON dict (optional).
            execution_date: Execution date for the validation (optional).

        Returns:
            The ID of the inserted record, or None on failure.
        """
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            unsuccessful = total_expectations - successful_expectations
            success_percent = (
                (successful_expectations / total_expectations * 100)
                if total_expectations > 0
                else 0
            )

            sql = f"""
                INSERT INTO {self.schema}.{self.table} (
                    run_id, checkpoint_name, expectation_suite_name,
                    data_asset_name, success, total_expectations,
                    successful_expectations, unsuccessful_expectations,
                    success_percent, result_json, execution_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            result = hook.get_first(
                sql,
                parameters=(
                    run_id,
                    checkpoint_name,
                    expectation_suite_name,
                    data_asset_name,
                    success,
                    total_expectations,
                    successful_expectations,
                    unsuccessful,
                    success_percent,
                    json.dumps(result_json) if result_json else None,
                    execution_date or datetime.now().date(),
                ),
            )

            log.info(f"Logged validation result with ID: {result[0]}")
            return result[0] if result else None

        except Exception as e:
            log.error(f"Failed to log validation result: {e}")
            return None

    def get_recent_results(
        self,
        suite_name: Optional[str] = None,
        days: int = 7,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query recent validation results.

        Args:
            suite_name: Filter by expectation suite name (optional).
            days: Number of days to look back.
            limit: Maximum number of results to return.

        Returns:
            List of validation result records.
        """
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            sql = f"""
                SELECT
                    id, run_id, checkpoint_name, expectation_suite_name,
                    data_asset_name, validation_time, success,
                    total_expectations, successful_expectations,
                    success_percent, execution_date
                FROM {self.schema}.{self.table}
                WHERE validation_time >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
            """

            if suite_name:
                sql += f" AND expectation_suite_name = '{suite_name}'"

            sql += f" ORDER BY validation_time DESC LIMIT {limit}"

            records = hook.get_records(sql)

            return [
                {
                    "id": r[0],
                    "run_id": r[1],
                    "checkpoint_name": r[2],
                    "expectation_suite_name": r[3],
                    "data_asset_name": r[4],
                    "validation_time": r[5],
                    "success": r[6],
                    "total_expectations": r[7],
                    "successful_expectations": r[8],
                    "success_percent": r[9],
                    "execution_date": r[10],
                }
                for r in records
            ]

        except Exception as e:
            log.error(f"Failed to query validation results: {e}")
            return []

    def get_suite_health(self, days: int = 30) -> Dict[str, Dict[str, Any]]:
        """Calculate health metrics for each expectation suite.

        Args:
            days: Number of days to analyze.

        Returns:
            Dict mapping suite names to health metrics.
        """
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            sql = f"""
                SELECT
                    expectation_suite_name,
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_runs,
                    AVG(success_percent) as avg_success_percent,
                    MAX(validation_time) as last_run
                FROM {self.schema}.{self.table}
                WHERE validation_time >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
                GROUP BY expectation_suite_name
                ORDER BY expectation_suite_name
            """

            records = hook.get_records(sql)

            return {
                r[0]: {
                    "total_runs": r[1],
                    "successful_runs": r[2],
                    "run_success_rate": (r[2] / r[1] * 100) if r[1] > 0 else 0,
                    "avg_expectation_success_percent": float(r[3]) if r[3] else 0,
                    "last_run": r[4],
                }
                for r in records
            }

        except Exception as e:
            log.error(f"Failed to calculate suite health: {e}")
            return {}


class PostgresLogValidationAction(ValidationAction):
    """Great Expectations ValidationAction that logs results to PostgreSQL.

    This action can be used in checkpoint configurations to automatically
    persist validation results to a PostgreSQL database for reporting.

    Configuration in checkpoint YAML:
        action_list:
          - name: log_to_postgres
            action:
              class_name: PostgresLogValidationAction
              module_name: plugins.validation_store
              postgres_conn_id: postgres_default
              schema: data_platform
              table: validation_results
    """

    def __init__(
        self,
        data_context,
        postgres_conn_id: str = "postgres_default",
        schema: str = "data_platform",
        table: str = "validation_results",
    ):
        """Initialize the PostgreSQL logging action.

        Args:
            data_context: Great Expectations DataContext.
            postgres_conn_id: Airflow PostgreSQL connection ID.
            schema: Database schema for results table.
            table: Table name for storing results.
        """
        super().__init__(data_context)
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table
        self._logger = None

    @property
    def logger(self) -> ValidationResultLogger:
        """Lazy-initialize the ValidationResultLogger."""
        if self._logger is None:
            self._logger = ValidationResultLogger(
                postgres_conn_id=self.postgres_conn_id,
                schema=self.schema,
                table=self.table,
            )
        return self._logger

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute the action to log validation results to PostgreSQL.

        Args:
            validation_result_suite: The validation result suite.
            validation_result_suite_identifier: Identifier for the result.
            data_asset: The validated data asset.
            expectation_suite_identifier: Suite identifier (optional).
            checkpoint_identifier: Checkpoint identifier (optional).
            **kwargs: Additional arguments.

        Returns:
            Dict with action status and result ID.
        """
        result = validation_result_suite
        statistics = result.statistics if hasattr(result, "statistics") else {}

        # Extract checkpoint name from identifier if available
        checkpoint_name = None
        if checkpoint_identifier:
            checkpoint_name = str(checkpoint_identifier)

        # Extract run_id
        run_id = str(validation_result_suite_identifier.run_id) if hasattr(
            validation_result_suite_identifier, "run_id"
        ) else str(validation_result_suite_identifier)

        # Extract expectation suite name
        expectation_suite_name = "unknown"
        if hasattr(result, "meta") and isinstance(result.meta, dict):
            expectation_suite_name = result.meta.get("expectation_suite_name", "unknown")
        elif expectation_suite_identifier:
            expectation_suite_name = str(expectation_suite_identifier)

        # Log the result
        result_id = self.logger.log_result(
            run_id=run_id,
            checkpoint_name=checkpoint_name,
            expectation_suite_name=expectation_suite_name,
            success=result.success if hasattr(result, "success") else False,
            total_expectations=statistics.get("evaluated_expectations", 0),
            successful_expectations=statistics.get("successful_expectations", 0),
            data_asset_name=str(data_asset) if data_asset else None,
            result_json=result.to_json_dict() if hasattr(result, "to_json_dict") else None,
        )

        return {
            "class": self.__class__.__name__,
            "result_id": result_id,
            "success": True,
        }


def create_validation_action(logger: ValidationResultLogger):
    """Create a Great Expectations validation action that logs to PostgreSQL.

    This function returns a validation action class that can be used in
    GE checkpoint configurations to automatically log results.

    Args:
        logger: ValidationResultLogger instance to use.

    Returns:
        A ValidationAction class for use in checkpoints.

    Note:
        This is a legacy factory function. Prefer using PostgresLogValidationAction
        directly in checkpoint configurations.
    """

    class LegacyPostgresLogValidationAction:
        """Custom GE action to log validation results to PostgreSQL."""

        def __init__(self, data_context):
            self.data_context = data_context
            self.logger = logger

        def run(
            self,
            validation_result_suite_identifier,
            validation_result_suite,
            data_asset,
            **kwargs,
        ):
            """Execute the action to log results.

            Args:
                validation_result_suite_identifier: Identifier for the result.
                validation_result_suite: The validation results.
                data_asset: The validated data asset.
                **kwargs: Additional arguments.

            Returns:
                Dict with action status.
            """
            result = validation_result_suite

            self.logger.log_result(
                run_id=str(validation_result_suite_identifier.run_id),
                expectation_suite_name=result.meta.get(
                    "expectation_suite_name", "unknown"
                ),
                success=result.success,
                total_expectations=result.statistics.get("evaluated_expectations", 0),
                successful_expectations=result.statistics.get(
                    "successful_expectations", 0
                ),
                data_asset_name=str(data_asset) if data_asset else None,
                result_json=result.to_json_dict(),
            )

            return {"class": self.__class__.__name__}

    return LegacyPostgresLogValidationAction
