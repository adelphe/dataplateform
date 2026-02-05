"""Data Quality Checks DAG.

Orchestrates comprehensive data quality validation using Great Expectations
across all medallion architecture layers:

1. Raw/Bronze Layer: Validates incoming data integrity and schema compliance
2. Staging/Silver Layer: Validates transformed and cleaned data
3. Marts/Gold Layer: Validates analytics-ready tables

The DAG can run:
- After the transformation pipeline completes (sensor-triggered)
- On a schedule for continuous quality monitoring
- Manually for ad-hoc validation

Key features:
- Checkpoint-based validation for efficient batch processing
- Validation result storage for historical tracking
- Data Docs generation for interactive reports
- Alerting on validation failures
"""

import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/pipelines")

from operators.ge_operator import GreatExpectationsOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

# Great Expectations configuration
GE_ROOT_DIR = "/opt/airflow/data-quality/great_expectations"


def _on_failure_callback(context: Dict[str, Any]) -> None:
    """Handle data quality check failure with alerting.

    Sends notifications via Slack and email using the alerting system,
    including validation failure details and affected datasets.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "Unknown error")

    print(
        f"ALERT: Data quality check failed!\n"
        f"  DAG: {dag_id}\n"
        f"  Task: {task_id}\n"
        f"  Execution Date: {execution_date}\n"
        f"  Error: {exception}"
    )

    try:
        from utils.alerting import (
            format_task_failure_alert,
            send_alert,
            AlertSeverity,
        )

        # Format and send alert
        alert_data = format_task_failure_alert(context)

        # Add data quality specific context
        layer_mapping = {
            "validate_raw_layer": "Raw/Bronze",
            "validate_staging_layer": "Staging/Silver",
            "validate_marts_layer": "Marts/Gold",
        }
        if task_id in layer_mapping:
            alert_data["fields"].append({
                "title": "Data Layer",
                "value": layer_mapping[task_id],
                "short": True,
            })

        # Query for unacknowledged alerts count
        ti = context.get("task_instance")
        if ti:
            validation_results = ti.xcom_pull(task_ids=task_id, key="validation_results")
            if validation_results:
                alert_data["fields"].append({
                    "title": "Validation Results",
                    "value": f"{len(validation_results)} suite(s) checked",
                    "short": True,
                })

        # Data quality failures are critical
        alert_data["severity"] = AlertSeverity.CRITICAL

        send_alert(alert_data)
    except Exception as e:
        print(f"Warning: Failed to send failure alert: {e}")


def _decide_validation_scope(**context) -> List[str]:
    """Decide which validation path to take based on parameters.

    Returns:
        List of task IDs to execute based on validation scope.
    """
    params = context.get("params", {})
    scope = params.get("validation_scope", "full")

    if scope == "raw_only":
        return ["validate_raw_layer"]
    elif scope == "staging_only":
        return ["validate_staging_layer"]
    elif scope == "marts_only":
        return ["validate_marts_layer"]
    else:
        # Full scope: run all validation layers
        return ["validate_raw_layer", "validate_staging_layer", "validate_marts_layer"]


def _generate_quality_report(**context) -> None:
    """Generate a summary report from all validation results."""
    ti = context["ti"]

    # Collect results from all validation tasks
    validation_tasks = [
        "validate_raw_layer",
        "validate_staging_layer",
        "validate_marts_layer",
    ]

    all_results: List[Dict[str, Any]] = []
    for task_id in validation_tasks:
        result = ti.xcom_pull(task_ids=task_id, key="validation_results")
        if result:
            all_results.extend(result)

    # Calculate summary statistics
    total_validations = len(all_results)
    passed_validations = sum(1 for r in all_results if r.get("success", False))
    failed_validations = total_validations - passed_validations

    total_expectations = sum(r.get("total_expectations", 0) for r in all_results)
    passed_expectations = sum(
        r.get("successful_expectations", 0) for r in all_results
    )

    # Build report
    report_lines = [
        "=" * 60,
        " DATA QUALITY VALIDATION REPORT",
        "=" * 60,
        f" Execution Date:     {context['ds']}",
        f" Run ID:             {context['run_id']}",
        "",
        " SUMMARY",
        "-" * 60,
        f" Total Validations:  {total_validations}",
        f" Passed:             {passed_validations}",
        f" Failed:             {failed_validations}",
        f" Success Rate:       {(passed_validations/total_validations*100) if total_validations else 0:.1f}%",
        "",
        f" Total Expectations: {total_expectations}",
        f" Passed:             {passed_expectations}",
        f" Failed:             {total_expectations - passed_expectations}",
        f" Success Rate:       {(passed_expectations/total_expectations*100) if total_expectations else 0:.1f}%",
        "",
        " LAYER DETAILS",
        "-" * 60,
    ]

    # Group results by layer
    layers = {"raw": [], "staging": [], "marts": []}
    for result in all_results:
        suite_name = result.get("expectation_suite_name", "")
        if "raw" in suite_name.lower():
            layers["raw"].append(result)
        elif "staging" in suite_name.lower():
            layers["staging"].append(result)
        elif "mart" in suite_name.lower():
            layers["marts"].append(result)

    for layer_name, layer_results in layers.items():
        if layer_results:
            layer_passed = sum(1 for r in layer_results if r.get("success", False))
            layer_total = len(layer_results)
            status = "PASSED" if layer_passed == layer_total else "FAILED"
            report_lines.append(
                f" {layer_name.upper():10s} [{status}] "
                f"{layer_passed}/{layer_total} validations passed"
            )

    report_lines.extend(["", "=" * 60])

    # Print report
    report = "\n".join(report_lines)
    print(report)

    # Store report in XCom for downstream use
    ti.xcom_push(key="quality_report", value=report)
    ti.xcom_push(key="overall_success", value=(failed_validations == 0))


def _build_data_docs(**context) -> None:
    """Build Great Expectations Data Docs."""
    try:
        import great_expectations as gx
        from great_expectations.data_context import FileDataContext

        data_context = FileDataContext(context_root_dir=GE_ROOT_DIR)
        data_context.build_data_docs()
        print("Data Docs successfully generated")
        print(f"Local site: {GE_ROOT_DIR}/uncommitted/data_docs/local_site/index.html")
    except Exception as e:
        print(f"Warning: Failed to build Data Docs: {e}")


with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description=(
        "Great Expectations data quality validation across "
        "Bronze, Silver, and Gold layers"
    ),
    schedule="0 4 * * *",  # Run at 04:00 UTC daily, after transformation
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality", "great-expectations", "validation"],
    on_failure_callback=_on_failure_callback,
    params={
        "validation_scope": "full",  # Options: full, raw_only, staging_only, marts_only
        "fail_on_validation_error": True,
        "skip_sensor": False,
    },
) as dag:

    # ------------------------------------------------------------------
    # 1. Wait for transformation pipeline to complete (optional)
    # ------------------------------------------------------------------
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="dbt_transform",
        external_task_id=None,  # Wait for entire DAG
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        timeout=3600,
        poke_interval=120,
    )

    # ------------------------------------------------------------------
    # 2. Decide validation scope based on parameters
    # ------------------------------------------------------------------
    decide_scope = BranchPythonOperator(
        task_id="decide_scope",
        python_callable=_decide_validation_scope,
    )

    # ------------------------------------------------------------------
    # 3. Raw/Bronze Layer Validation
    # ------------------------------------------------------------------
    validate_raw_layer = GreatExpectationsOperator(
        task_id="validate_raw_layer",
        checkpoint_name="raw_data_checkpoint",
        ge_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure="{{ params.fail_on_validation_error }}",
        return_json_dict=True,
    )

    # ------------------------------------------------------------------
    # 4. Staging/Silver Layer Validation
    # ------------------------------------------------------------------
    validate_staging_layer = GreatExpectationsOperator(
        task_id="validate_staging_layer",
        checkpoint_name="staging_checkpoint",
        ge_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure="{{ params.fail_on_validation_error }}",
        return_json_dict=True,
    )

    # ------------------------------------------------------------------
    # 5. Marts/Gold Layer Validation
    # ------------------------------------------------------------------
    validate_marts_layer = GreatExpectationsOperator(
        task_id="validate_marts_layer",
        checkpoint_name="marts_checkpoint",
        ge_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure="{{ params.fail_on_validation_error }}",
        return_json_dict=True,
    )

    # ------------------------------------------------------------------
    # 6. Generate quality report
    # ------------------------------------------------------------------
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_quality_report,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if validations fail
    )

    # ------------------------------------------------------------------
    # 7. Build Data Docs
    # ------------------------------------------------------------------
    build_data_docs = PythonOperator(
        task_id="build_data_docs",
        python_callable=_build_data_docs,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ------------------------------------------------------------------
    # Task Dependencies
    # ------------------------------------------------------------------
    # Wait for transformation -> decide scope
    wait_for_transformation >> decide_scope

    # Branch to all validation tasks (BranchPythonOperator will skip non-selected)
    decide_scope >> [validate_raw_layer, validate_staging_layer, validate_marts_layer]

    # Generate report and docs after all validations complete (or are skipped)
    [validate_raw_layer, validate_staging_layer, validate_marts_layer] >> generate_report
    [validate_raw_layer, validate_staging_layer, validate_marts_layer] >> build_data_docs
