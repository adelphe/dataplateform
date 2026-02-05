"""Custom Airflow operator for running Great Expectations validations."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

log = logging.getLogger(__name__)


class GreatExpectationsOperator(BaseOperator):
    """Run Great Expectations checkpoint validations within Airflow.

    This operator executes GE checkpoints and optionally fails the task
    if validation expectations are not met.

    Args:
        checkpoint_name: Name of the checkpoint to run (without .yml extension).
        ge_root_dir: Path to the great_expectations directory.
        fail_task_on_validation_failure: If True, raise AirflowException on
            validation failure. Default is True.
        return_json_dict: If True, return validation results as dict.
        runtime_configuration: Optional runtime config to pass to checkpoint.
        batch_request: Optional batch request override for the checkpoint.
        expectation_suite_name: Optional suite name override.
        data_context_config: Optional DataContext configuration dict.
        checkpoint_kwargs: Additional kwargs to pass to checkpoint run.
    """

    template_fields = (
        "checkpoint_name",
        "batch_request",
        "expectation_suite_name",
        "runtime_configuration",
        "fail_task_on_validation_failure",
    )

    def __init__(
        self,
        checkpoint_name: str,
        ge_root_dir: Optional[str] = None,
        fail_task_on_validation_failure: bool = True,
        return_json_dict: bool = False,
        runtime_configuration: Optional[Dict[str, Any]] = None,
        batch_request: Optional[Dict[str, Any]] = None,
        expectation_suite_name: Optional[str] = None,
        data_context_config: Optional[Dict[str, Any]] = None,
        checkpoint_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.checkpoint_name = checkpoint_name
        self.ge_root_dir = ge_root_dir
        self.fail_task_on_validation_failure = fail_task_on_validation_failure
        self.return_json_dict = return_json_dict
        self.runtime_configuration = runtime_configuration or {}
        self.batch_request = batch_request
        self.expectation_suite_name = expectation_suite_name
        self.data_context_config = data_context_config
        self.checkpoint_kwargs = checkpoint_kwargs or {}

    def execute(self, context: Dict[str, Any]) -> Union[Dict[str, Any], bool]:
        """Execute the Great Expectations checkpoint validation.

        Args:
            context: Airflow task context.

        Returns:
            Validation results as dict if return_json_dict is True,
            otherwise returns success boolean.

        Raises:
            AirflowException: If validation fails and fail_task_on_validation_failure
                is True.
        """
        # Coerce fail_task_on_validation_failure to boolean (handles templated strings)
        fail_on_failure = self.fail_task_on_validation_failure
        if isinstance(fail_on_failure, str):
            fail_on_failure = fail_on_failure.lower() in ("true", "1", "yes")
        else:
            fail_on_failure = bool(fail_on_failure)

        try:
            import great_expectations as gx
            from great_expectations.checkpoint import CheckpointResult
            from great_expectations.data_context import FileDataContext
        except ImportError as e:
            raise AirflowException(
                "Great Expectations is not installed. "
                "Please install it with: pip install great_expectations"
            ) from e

        # Determine GE root directory
        ge_root = self._get_ge_root_dir()
        log.info("Using Great Expectations root directory: %s", ge_root)

        # Create data context
        try:
            if self.data_context_config:
                data_context = gx.get_context(
                    project_config=self.data_context_config,
                    context_root_dir=ge_root,
                )
            else:
                data_context = FileDataContext(context_root_dir=ge_root)
        except Exception as e:
            raise AirflowException(
                f"Failed to create Great Expectations DataContext: {e}"
            ) from e

        log.info("Running checkpoint: %s", self.checkpoint_name)

        # Build checkpoint run kwargs
        run_kwargs = {
            "checkpoint_name": self.checkpoint_name,
            **self.checkpoint_kwargs,
        }

        # Add optional overrides
        if self.runtime_configuration:
            run_kwargs["runtime_configuration"] = self.runtime_configuration

        if self.batch_request:
            run_kwargs["batch_request"] = self.batch_request

        if self.expectation_suite_name:
            run_kwargs["expectation_suite_name"] = self.expectation_suite_name

        # Run the checkpoint
        try:
            checkpoint_result: CheckpointResult = data_context.run_checkpoint(
                **run_kwargs
            )
        except Exception as e:
            raise AirflowException(
                f"Failed to run checkpoint '{self.checkpoint_name}': {e}"
            ) from e

        # Process results
        success = checkpoint_result.success
        run_id = checkpoint_result.run_id.run_name
        validation_results = self._extract_validation_summary(checkpoint_result)

        log.info("Checkpoint run ID: %s", run_id)
        log.info("Checkpoint success: %s", success)

        # Log validation details
        for result in validation_results:
            status = "PASSED" if result["success"] else "FAILED"
            log.info(
                "[%s] %s - %d/%d expectations passed",
                status,
                result["expectation_suite_name"],
                result["successful_expectations"],
                result["total_expectations"],
            )

        # Push results to XCom
        context["ti"].xcom_push(key="validation_success", value=success)
        context["ti"].xcom_push(key="run_id", value=run_id)
        context["ti"].xcom_push(key="validation_results", value=validation_results)

        # Handle failure
        if not success and fail_on_failure:
            failed_validations = [r for r in validation_results if not r["success"]]
            failure_msg = self._format_failure_message(failed_validations)
            raise AirflowException(failure_msg)

        if self.return_json_dict:
            return {
                "success": success,
                "run_id": run_id,
                "validation_results": validation_results,
                "checkpoint_name": self.checkpoint_name,
            }

        return success

    def _get_ge_root_dir(self) -> str:
        """Determine the Great Expectations root directory.

        Returns:
            Path to the GE root directory.
        """
        if self.ge_root_dir:
            return self.ge_root_dir

        # Try environment variable
        env_root = os.environ.get("GE_ROOT_DIR")
        if env_root:
            return env_root

        # Default to relative path from project root
        default_path = Path(__file__).parent.parent.parent / "data-quality" / "great_expectations"
        if default_path.exists():
            return str(default_path)

        raise AirflowException(
            "Could not determine Great Expectations root directory. "
            "Please set ge_root_dir parameter or GE_ROOT_DIR environment variable."
        )

    def _extract_validation_summary(
        self, checkpoint_result
    ) -> List[Dict[str, Any]]:
        """Extract summary information from checkpoint validation results.

        Args:
            checkpoint_result: Great Expectations CheckpointResult.

        Returns:
            List of validation result summaries.
        """
        summaries = []

        for validation_key, validation_result in checkpoint_result.run_results.items():
            result = validation_result.get("validation_result", {})
            statistics = result.get("statistics", {})

            summary = {
                "expectation_suite_name": result.get(
                    "meta", {}
                ).get("expectation_suite_name", "unknown"),
                "success": result.get("success", False),
                "total_expectations": statistics.get(
                    "evaluated_expectations", 0
                ),
                "successful_expectations": statistics.get(
                    "successful_expectations", 0
                ),
                "unsuccessful_expectations": statistics.get(
                    "unsuccessful_expectations", 0
                ),
                "success_percent": statistics.get("success_percent", 0),
                "data_asset_name": str(validation_key.batch_identifier)
                if hasattr(validation_key, "batch_identifier")
                else "unknown",
            }

            # Extract failed expectation details
            if not summary["success"]:
                failed_expectations = []
                for expectation_result in result.get("results", []):
                    if not expectation_result.get("success", True):
                        failed_expectations.append({
                            "expectation_type": expectation_result.get(
                                "expectation_config", {}
                            ).get("expectation_type", "unknown"),
                            "kwargs": expectation_result.get(
                                "expectation_config", {}
                            ).get("kwargs", {}),
                        })
                summary["failed_expectations"] = failed_expectations

            summaries.append(summary)

        return summaries

    def _format_failure_message(
        self, failed_validations: List[Dict[str, Any]]
    ) -> str:
        """Format a detailed failure message for failed validations.

        Args:
            failed_validations: List of failed validation summaries.

        Returns:
            Formatted failure message string.
        """
        lines = [
            f"Great Expectations validation failed for checkpoint "
            f"'{self.checkpoint_name}':",
            "",
        ]

        for validation in failed_validations:
            lines.append(
                f"  Suite: {validation['expectation_suite_name']}"
            )
            lines.append(
                f"    Passed: {validation['successful_expectations']}/"
                f"{validation['total_expectations']} expectations"
            )

            if "failed_expectations" in validation:
                lines.append("    Failed expectations:")
                for exp in validation["failed_expectations"][:5]:  # Limit to 5
                    lines.append(f"      - {exp['expectation_type']}")

            lines.append("")

        return "\n".join(lines)


class GreatExpectationsValidationOperator(BaseOperator):
    """Run a single Great Expectations expectation suite against a data asset.

    This is a simpler alternative to checkpoint-based validation for cases
    where you want to validate a single dataset.

    Args:
        datasource_name: Name of the datasource to use.
        data_asset_name: Name of the data asset to validate.
        expectation_suite_name: Name of the expectation suite to apply.
        ge_root_dir: Path to the great_expectations directory.
        fail_task_on_validation_failure: If True, raise on validation failure.
        batch_identifiers: Optional batch identifiers for the data asset.
    """

    template_fields = (
        "data_asset_name",
        "expectation_suite_name",
        "batch_identifiers",
    )

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        expectation_suite_name: str,
        ge_root_dir: Optional[str] = None,
        fail_task_on_validation_failure: bool = True,
        batch_identifiers: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.datasource_name = datasource_name
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        self.ge_root_dir = ge_root_dir
        self.fail_task_on_validation_failure = fail_task_on_validation_failure
        self.batch_identifiers = batch_identifiers or {}

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the validation against the specified data asset.

        Args:
            context: Airflow task context.

        Returns:
            Validation result summary dict.
        """
        try:
            import great_expectations as gx
            from great_expectations.data_context import FileDataContext
        except ImportError as e:
            raise AirflowException(
                "Great Expectations is not installed."
            ) from e

        # Get GE root directory
        ge_root = self.ge_root_dir or os.environ.get("GE_ROOT_DIR")
        if not ge_root:
            default_path = Path(__file__).parent.parent.parent / "data-quality" / "great_expectations"
            if default_path.exists():
                ge_root = str(default_path)
            else:
                raise AirflowException("Could not find GE root directory")

        log.info("Validating %s with suite %s", self.data_asset_name, self.expectation_suite_name)

        # Create context and run validation
        data_context = FileDataContext(context_root_dir=ge_root)

        # Build batch request
        batch_request = {
            "datasource_name": self.datasource_name,
            "data_connector_name": "default_runtime_connector",
            "data_asset_name": self.data_asset_name,
            "batch_identifiers": {
                "default_identifier_name": "validation_batch",
                **self.batch_identifiers,
            },
        }

        # Get validator and run
        validator = data_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=self.expectation_suite_name,
        )

        validation_result = validator.validate()

        success = validation_result.success
        statistics = validation_result.statistics

        result_summary = {
            "success": success,
            "data_asset_name": self.data_asset_name,
            "expectation_suite_name": self.expectation_suite_name,
            "total_expectations": statistics.get("evaluated_expectations", 0),
            "successful_expectations": statistics.get("successful_expectations", 0),
            "success_percent": statistics.get("success_percent", 0),
        }

        context["ti"].xcom_push(key="validation_result", value=result_summary)

        if not success and self.fail_task_on_validation_failure:
            raise AirflowException(
                f"Validation failed for {self.data_asset_name}: "
                f"{result_summary['successful_expectations']}/"
                f"{result_summary['total_expectations']} expectations passed"
            )

        return result_summary
