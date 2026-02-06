"""Custom operator for running data quality checks against PostgreSQL."""

import logging
from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


class DataQualityCheckOperator(BaseOperator):
    """Run SQL-based data quality checks and fail if thresholds are not met.

    Each check is a dict with keys:
        - sql: The SQL query to execute (must return a single numeric value).
        - description: Human-readable description of the check.
        - min_threshold: Minimum acceptable value (optional).
        - max_threshold: Maximum acceptable value (optional).

    Args:
        sql_checks: List of check dictionaries.
        postgres_conn_id: Airflow connection ID for PostgreSQL.
        min_threshold: Default minimum threshold applied to all checks.
        max_threshold: Default maximum threshold applied to all checks.
    """

    template_fields = ("sql_checks",)

    def __init__(
        self,
        sql_checks: List[dict],
        postgres_conn_id: str = "postgres_default",
        min_threshold: Optional[float] = None,
        max_threshold: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_checks = sql_checks
        self.postgres_conn_id = postgres_conn_id
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    def execute(self, context):
        """Run all data quality checks and raise on failure."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        failures = []

        for i, check in enumerate(self.sql_checks):
            sql = check["sql"]
            description = check.get("description", f"Check #{i + 1}")
            check_min = check.get("min_threshold", self.min_threshold)
            check_max = check.get("max_threshold", self.max_threshold)

            log.info("Running quality check: %s", description)
            log.info("SQL: %s", sql)

            result = hook.get_first(sql)
            if result is None or result[0] is None:
                failures.append(f"{description}: query returned no results")
                continue

            value = float(result[0])
            log.info("Check '%s' returned value: %s", description, value)

            if check_min is not None and value < check_min:
                msg = (
                    f"{description}: value {value} is below "
                    f"minimum threshold {check_min}"
                )
                log.error(msg)
                failures.append(msg)

            if check_max is not None and value > check_max:
                msg = (
                    f"{description}: value {value} is above "
                    f"maximum threshold {check_max}"
                )
                log.error(msg)
                failures.append(msg)

            if check_min is None and check_max is None:
                if value == 0:
                    msg = f"{description}: value is 0 (expected non-zero)"
                    log.error(msg)
                    failures.append(msg)

        if failures:
            failure_summary = "\n".join(failures)
            raise AirflowException(
                f"Data quality checks failed ({len(failures)}/{len(self.sql_checks)}):\n"
                f"{failure_summary}"
            )

        log.info("All %d data quality checks passed", len(self.sql_checks))
