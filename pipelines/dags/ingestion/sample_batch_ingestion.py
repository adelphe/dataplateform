"""Sample Batch Ingestion DAG.

Demonstrates a complete batch ingestion pipeline that:
1. Initializes tracking tables (metadata, watermarks, schema history)
2. Ingests CSV and JSON files from the local filesystem to the Bronze layer
3. Extracts PostgreSQL tables (full snapshot and incremental with watermarking)
4. Validates ingestion results using data quality checks
5. Sends a completion summary notification

This DAG serves as a reference implementation for building production
ingestion pipelines using the ``pipelines/ingestion/`` modules.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/pipelines")

from operators.data_quality_operator import DataQualityCheckOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Directories for sample input data
INPUT_DIR = "/opt/airflow/data/input"


def _on_failure_callback(context):
    """Log failure details for alerting."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _setup_metadata_tables(**context):
    """Ensure all ingestion tracking tables exist in PostgreSQL."""
    from ingestion.metadata_logger import ensure_metadata_tables
    from ingestion.watermark_manager import ensure_watermark_table

    ensure_metadata_tables()
    ensure_watermark_table()
    print("Metadata and watermark tables are ready")


def _ingest_csv_files(**context):
    """Ingest CSV files from the input directory into the Bronze layer."""
    from ingestion.file_ingestor import batch_ingest_files

    ds = context["ds"]
    results = batch_ingest_files(INPUT_DIR, "*.csv", ds)

    uris = [r["uri"] for r in results if "uri" in r]
    errors = [r for r in results if "error" in r]

    print(f"CSV ingestion: {len(uris)} files uploaded, {len(errors)} errors")
    for uri in uris:
        print(f"  - {uri}")
    for err in errors:
        print(f"  - ERROR {err['file']}: {err['error']}")

    context["ti"].xcom_push(key="csv_uris", value=uris)
    context["ti"].xcom_push(key="csv_count", value=len(uris))
    return uris


def _ingest_json_files(**context):
    """Ingest JSON files from the input directory into the Bronze layer."""
    from ingestion.file_ingestor import batch_ingest_files

    ds = context["ds"]
    results = batch_ingest_files(INPUT_DIR, "*.json", ds)

    uris = [r["uri"] for r in results if "uri" in r]
    errors = [r for r in results if "error" in r]

    print(f"JSON ingestion: {len(uris)} files uploaded, {len(errors)} errors")
    for uri in uris:
        print(f"  - {uri}")
    for err in errors:
        print(f"  - ERROR {err['file']}: {err['error']}")

    context["ti"].xcom_push(key="json_uris", value=uris)
    context["ti"].xcom_push(key="json_count", value=len(uris))
    return uris


def _extract_postgres_full(**context):
    """Extract a full snapshot of information_schema.tables to the Bronze layer."""
    from ingestion.database_ingestor import extract_table_full

    ds = context["ds"]
    object_key = f"postgres/information_schema_tables/{ds}/tables.parquet"

    result = extract_table_full(
        table_name="tables",
        schema="information_schema",
        object_key=object_key,
        execution_date=ds,
    )

    print(f"Full extraction: {result['uri']} ({result['row_count']} rows)")
    context["ti"].xcom_push(key="full_extract_uri", value=result["uri"])
    context["ti"].xcom_push(key="full_extract_rows", value=result["row_count"])
    return result


def _extract_postgres_incremental(**context):
    """Extract incremental data from the dag_run table using execution_date watermark."""
    from ingestion.database_ingestor import extract_table_incremental

    ds = context["ds"]
    object_key = f"postgres/dag_run/{ds}/dag_run_incremental.parquet"

    result = extract_table_incremental(
        table_name="dag_run",
        schema="public",
        watermark_column="execution_date",
        object_key=object_key,
        execution_date=ds,
        watermark_type="timestamp",
    )

    row_count = result["row_count"]
    watermark = result.get("watermark", "N/A")
    print(f"Incremental extraction: {row_count} new rows, watermark={watermark}")

    context["ti"].xcom_push(key="incremental_uri", value=result.get("uri"))
    context["ti"].xcom_push(key="incremental_rows", value=row_count)
    context["ti"].xcom_push(key="watermark", value=watermark)
    return result


def _send_completion_notification(**context):
    """Log a completion summary with ingestion statistics."""
    ti = context["ti"]

    csv_count = ti.xcom_pull(task_ids="ingest_csv_files", key="csv_count") or 0
    json_count = ti.xcom_pull(task_ids="ingest_json_files", key="json_count") or 0
    full_rows = ti.xcom_pull(task_ids="extract_postgres_full", key="full_extract_rows") or 0
    incr_rows = ti.xcom_pull(task_ids="extract_postgres_incremental", key="incremental_rows") or 0

    summary = (
        "========================================\n"
        " Batch Ingestion Summary\n"
        "========================================\n"
        f" Execution Date: {context['ds']}\n"
        f" CSV Files Ingested:  {csv_count}\n"
        f" JSON Files Ingested: {json_count}\n"
        f" Full Extract Rows:   {full_rows}\n"
        f" Incremental Rows:    {incr_rows}\n"
        "========================================\n"
    )
    print(summary)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="sample_batch_ingestion",
    default_args=default_args,
    description="Sample batch ingestion pipeline: files and PostgreSQL to Bronze layer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "batch", "sample"],
    on_failure_callback=_on_failure_callback,
) as dag:

    setup_metadata_tables = PythonOperator(
        task_id="setup_metadata_tables",
        python_callable=_setup_metadata_tables,
    )

    ingest_csv_files = PythonOperator(
        task_id="ingest_csv_files",
        python_callable=_ingest_csv_files,
    )

    ingest_json_files = PythonOperator(
        task_id="ingest_json_files",
        python_callable=_ingest_json_files,
    )

    extract_postgres_full = PythonOperator(
        task_id="extract_postgres_full",
        python_callable=_extract_postgres_full,
    )

    extract_postgres_incremental = PythonOperator(
        task_id="extract_postgres_incremental",
        python_callable=_extract_postgres_incremental,
    )

    validate_ingestion = DataQualityCheckOperator(
        task_id="validate_ingestion",
        sql_checks=[
            {
                "sql": (
                    "SELECT COUNT(*) FROM data_platform.ingestion_metadata "
                    "WHERE execution_date = '{{ ds }}' AND status = 'success'"
                ),
                "description": "At least one successful ingestion run today",
                "min_threshold": 1,
            },
            {
                "sql": (
                    "SELECT COUNT(*) FROM data_platform.watermarks "
                    "WHERE updated_at >= NOW() - INTERVAL '24 hours'"
                ),
                "description": "Watermarks updated within 24 hours",
                "min_threshold": 0,
            },
        ],
        postgres_conn_id="postgres_default",
    )

    send_completion_notification = PythonOperator(
        task_id="send_completion_notification",
        python_callable=_send_completion_notification,
    )

    # Task dependencies: fan-out / fan-in pattern
    setup_metadata_tables >> [
        ingest_csv_files,
        ingest_json_files,
        extract_postgres_full,
        extract_postgres_incremental,
    ] >> validate_ingestion >> send_completion_notification
