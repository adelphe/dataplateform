"""dbt Transformation DAG.

Orchestrates the full transformation pipeline through the medallion
architecture layers, with dependency management on the upstream
ingestion DAG:

1. Waits for the ingestion DAG to complete (ExternalTaskSensor)
2. Loads Parquet data from MinIO Bronze layer into PostgreSQL raw schema
3. Installs dbt dependencies
4. Seeds reference data
5. Runs dbt models layer-by-layer (staging -> intermediate -> marts)
   with per-layer tests as quality gates
6. Supports incremental and full-refresh execution via DAG params
7. Generates dbt documentation
8. Logs a transformation summary

Schedule: Runs daily at 02:00 UTC, one hour after the default
ingestion window, to allow upstream data to land.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow/pipelines")

from operators.dbt_operator import (
    DbtDocsOperator,
    DbtOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)
from operators.loader_operator import MinIOToPostgresOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _on_failure_callback(context):
    """Handle dbt transformation failure with alerting.

    Sends notifications via Slack and email using the alerting system,
    including dbt model failure details and data lineage context.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]

    # Log failure
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")

    try:
        from utils.alerting import (
            format_task_failure_alert,
            send_alert,
            AlertSeverity,
        )

        # Format and send alert
        alert_data = format_task_failure_alert(context)

        # Add dbt-specific context
        dbt_layers = {
            "dbt_run_staging": "staging",
            "dbt_run_intermediate": "intermediate",
            "dbt_run_marts": "marts",
            "dbt_test_staging": "staging tests",
            "dbt_test_marts": "marts tests",
        }
        if task_id in dbt_layers:
            alert_data["fields"].append({
                "title": "dbt Layer",
                "value": dbt_layers[task_id],
                "short": True,
            })
            alert_data["fields"].append({
                "title": "Downstream Impact",
                "value": "Analytics tables may be stale or incorrect",
                "short": False,
            })

        # Elevate severity for marts failures
        if "marts" in task_id:
            alert_data["severity"] = AlertSeverity.CRITICAL

        send_alert(alert_data)
    except Exception as e:
        print(f"Warning: Failed to send failure alert: {e}")


def _log_transformation_summary(**context):
    """Log a summary of the transformation pipeline run."""
    ti = context["ti"]
    row_count = ti.xcom_pull(task_ids="load_bronze_to_raw", key="row_count") or 0

    summary = (
        "========================================\n"
        " dbt Transformation Summary\n"
        "========================================\n"
        f" Execution Date:  {context['ds']}\n"
        f" Rows Loaded:     {row_count}\n"
        f" Full Refresh:    {context['params'].get('full_refresh', False)}\n"
        " Layers Executed: staging -> intermediate -> marts\n"
        " Tests:           staging, marts\n"
        " Docs:            generated\n"
        "========================================\n"
    )
    print(summary)


with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description=(
        "dbt transformation pipeline: "
        "Bronze -> Staging -> Intermediate -> Marts"
    ),
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "dbt", "medallion"],
    on_failure_callback=_on_failure_callback,
    params={
        "full_refresh": False,
    },
) as dag:

    # ------------------------------------------------------------------
    # 1. Dependency management: wait for ingestion to finish
    # ------------------------------------------------------------------
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="sample_batch_ingestion",
        external_task_id=None,  # wait for the entire DAG
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        timeout=3600,
        poke_interval=120,
    )

    # ------------------------------------------------------------------
    # 2. Load Bronze Parquet data into PostgreSQL raw schema
    # ------------------------------------------------------------------
    load_bronze_to_raw = MinIOToPostgresOperator(
        task_id="load_bronze_to_raw",
        bucket="raw",
        object_key=(
            "{{ var.value.get("
            "'transactions_object_key', "
            "'postgres/transactions/latest/transactions.parquet'"
            ") }}"
        ),
        target_table="transactions",
        target_schema="raw",
        load_mode="replace",
    )

    # ------------------------------------------------------------------
    # 3. Install dbt packages
    # ------------------------------------------------------------------
    dbt_deps = DbtOperator(
        task_id="dbt_deps",
        dbt_command="deps",
    )

    # ------------------------------------------------------------------
    # 4. Load reference seed data
    # ------------------------------------------------------------------
    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        full_refresh="{{ params.full_refresh }}",
    )

    # ------------------------------------------------------------------
    # 5. Run staging models
    # ------------------------------------------------------------------
    dbt_run_staging = DbtRunOperator(
        task_id="dbt_run_staging",
        select="staging",
    )

    # ------------------------------------------------------------------
    # 6. Test staging models (quality gate before downstream layers)
    # ------------------------------------------------------------------
    dbt_test_staging = DbtTestOperator(
        task_id="dbt_test_staging",
        select="staging",
    )

    # ------------------------------------------------------------------
    # 7. Run intermediate models
    # ------------------------------------------------------------------
    dbt_run_intermediate = DbtRunOperator(
        task_id="dbt_run_intermediate",
        select="intermediate",
    )

    # ------------------------------------------------------------------
    # 8. Run mart models (incremental by default, full-refresh via param)
    # ------------------------------------------------------------------
    dbt_run_marts = DbtRunOperator(
        task_id="dbt_run_marts",
        select="marts",
        full_refresh="{{ params.full_refresh }}",
    )

    # ------------------------------------------------------------------
    # 9. Test mart models (quality gate on analytics-ready tables)
    # ------------------------------------------------------------------
    dbt_test_marts = DbtTestOperator(
        task_id="dbt_test_marts",
        select="marts",
        warn_error=True,
    )

    # ------------------------------------------------------------------
    # 10. Generate dbt documentation
    # ------------------------------------------------------------------
    dbt_docs_generate = DbtDocsOperator(
        task_id="dbt_docs_generate",
    )

    # ------------------------------------------------------------------
    # 11. Transformation summary
    # ------------------------------------------------------------------
    transformation_summary = PythonOperator(
        task_id="transformation_summary",
        python_callable=_log_transformation_summary,
    )

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    # Ingestion gate -> load -> dbt setup
    wait_for_ingestion >> load_bronze_to_raw >> dbt_deps >> dbt_seed

    # Layer-by-layer with test gates
    dbt_seed >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_intermediate
    dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts

    # Docs and summary after all tests pass
    dbt_test_marts >> [dbt_docs_generate, transformation_summary]
