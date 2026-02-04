"""Example DAG demonstrating data extraction from PostgreSQL to MinIO.

This DAG extracts data from a PostgreSQL table, saves it as a CSV file,
and uploads it to a MinIO bucket. It includes a sensor to wait for the
source table and error handling callbacks.
"""

import csv
import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
sys.path.insert(0, "/opt/airflow")

from operators.minio_operator import MinIOUploadOperator
from operators.postgres_operator import PostgresTableSensorOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _on_failure_callback(context):
    """Log failure details for alerting."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")


def _extract_to_csv(**context):
    """Extract data from PostgreSQL and save as CSV."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    ds = context["ds"]

    sql = """
        SELECT table_name, table_schema, table_type
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    output_dir = tempfile.mkdtemp()
    output_path = os.path.join(output_dir, f"tables_export_{ds}.csv")

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    cursor.close()
    conn.close()

    print(f"Exported {len(rows)} rows to {output_path}")
    context["ti"].xcom_push(key="csv_path", value=output_path)
    return output_path


def _get_csv_path(**context):
    """Retrieve the CSV path from XCom for the upload task."""
    return context["ti"].xcom_pull(task_ids="extract_to_csv", key="csv_path")


with DAG(
    dag_id="example_postgres_to_minio",
    default_args=default_args,
    description="Extract data from PostgreSQL and upload to MinIO",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "etl", "postgres", "minio"],
    on_failure_callback=_on_failure_callback,
) as dag:

    wait_for_table = PostgresTableSensorOperator(
        task_id="wait_for_table",
        table_name="dag_run",
        postgres_conn_id="postgres_default",
        schema="public",
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    extract_to_csv = PythonOperator(
        task_id="extract_to_csv",
        python_callable=_extract_to_csv,
    )

    upload_to_minio = MinIOUploadOperator(
        task_id="upload_to_minio",
        bucket_name="raw",
        object_name="postgres/tables_export_{{ ds }}.csv",
        file_path="{{ ti.xcom_pull(task_ids='extract_to_csv', key='csv_path') }}",
        minio_conn_id="minio_default",
    )

    wait_for_table >> extract_to_csv >> upload_to_minio
