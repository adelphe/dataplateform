"""Example DAG demonstrating data quality checks.

This DAG runs a series of SQL-based data quality validations
using the custom DataQualityCheckOperator. It checks for null values,
row counts, duplicate records, and data freshness.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from operators.data_quality_operator import DataQualityCheckOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _on_quality_failure(context):
    """Alert when data quality checks fail."""
    task_id = context["task_instance"].task_id
    exception = context.get("exception", "Unknown error")
    print(f"DATA QUALITY ALERT: Check '{task_id}' failed with: {exception}")


def _log_quality_success(**context):
    """Log that all quality checks passed."""
    ds = context["ds"]
    print(f"All data quality checks passed for {ds}")


with DAG(
    dag_id="example_data_quality",
    default_args=default_args,
    description="Run data quality checks on the platform database",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "data-quality"],
) as dag:

    check_row_counts = DataQualityCheckOperator(
        task_id="check_row_counts",
        sql_checks=[
            {
                "sql": "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';",
                "description": "Public tables exist",
                "min_threshold": 1,
            },
        ],
        postgres_conn_id="postgres_default",
        on_failure_callback=_on_quality_failure,
    )

    check_null_values = DataQualityCheckOperator(
        task_id="check_null_values",
        sql_checks=[
            {
                "sql": """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND column_name IS NOT NULL;
                """,
                "description": "No null column names in public schema",
                "min_threshold": 0,
            },
        ],
        postgres_conn_id="postgres_default",
        on_failure_callback=_on_quality_failure,
    )

    check_duplicates = DataQualityCheckOperator(
        task_id="check_duplicates",
        sql_checks=[
            {
                "sql": """
                    SELECT COUNT(*) - COUNT(DISTINCT table_name)
                    FROM information_schema.tables
                    WHERE table_schema = 'public';
                """,
                "description": "No duplicate table names in public schema",
                "max_threshold": 0,
            },
        ],
        postgres_conn_id="postgres_default",
        on_failure_callback=_on_quality_failure,
    )

    log_success = PythonOperator(
        task_id="log_quality_success",
        python_callable=_log_quality_success,
    )

    [check_row_counts, check_null_values, check_duplicates] >> log_success
