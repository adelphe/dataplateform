"""dbt Transformation DAG.

Orchestrates the full transformation pipeline:
1. Loads Parquet data from MinIO Bronze layer into PostgreSQL raw schema
2. Installs dbt dependencies
3. Seeds reference data
4. Runs dbt staging models (cleaning and standardization)
5. Runs dbt intermediate models (business logic)
6. Runs dbt mart models (analytics-ready tables)
7. Executes dbt tests for data quality validation
8. Generates dbt documentation

This DAG should be triggered after ingestion pipelines complete
to transform raw data through the medallion architecture layers.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from operators.dbt_operator import DbtOperator
from operators.loader_operator import MinIOToPostgresOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _on_failure_callback(context):
    """Log failure details for alerting."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")


with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description="dbt transformation pipeline: Bronze -> Staging -> Intermediate -> Marts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "dbt", "medallion"],
    on_failure_callback=_on_failure_callback,
) as dag:

    # Step 1: Load Bronze Parquet data into PostgreSQL raw schema
    load_bronze_to_raw = MinIOToPostgresOperator(
        task_id="load_bronze_to_raw",
        bucket="raw",
        object_key="{{ var.value.get('transactions_object_key', 'postgres/transactions/latest/transactions.parquet') }}",
        target_table="transactions",
        target_schema="raw",
        load_mode="replace",
    )

    # Step 2: Install dbt packages
    dbt_deps = DbtOperator(
        task_id="dbt_deps",
        dbt_command="deps",
    )

    # Step 3: Load reference seed data
    dbt_seed = DbtOperator(
        task_id="dbt_seed",
        dbt_command="seed",
    )

    # Step 4: Run staging models
    dbt_run_staging = DbtOperator(
        task_id="dbt_run_staging",
        dbt_command="run --select staging",
    )

    # Step 5: Run intermediate models
    dbt_run_intermediate = DbtOperator(
        task_id="dbt_run_intermediate",
        dbt_command="run --select intermediate",
    )

    # Step 6: Run mart models
    dbt_run_marts = DbtOperator(
        task_id="dbt_run_marts",
        dbt_command="run --select marts",
    )

    # Step 7: Run dbt tests
    dbt_test = DbtOperator(
        task_id="dbt_test",
        dbt_command="test",
    )

    # Step 8: Generate dbt documentation
    dbt_docs_generate = DbtOperator(
        task_id="dbt_docs_generate",
        dbt_command="docs generate",
    )

    # Task dependencies: sequential pipeline
    (
        load_bronze_to_raw
        >> dbt_deps
        >> dbt_seed
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> dbt_docs_generate
    )
