"""Example DAG demonstrating basic Airflow concepts.

This DAG shows how to use BashOperator and PythonOperator
with task dependencies. It is set to manual trigger only.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
}


def _print_greeting(**context):
    """Print a greeting with the execution date."""
    execution_date = context["ds"]
    print(f"Hello from the Data Platform! Execution date: {execution_date}")
    return "greeting_complete"


def _print_summary(**context):
    """Print a summary using XCom from the previous task."""
    ti = context["ti"]
    greeting_result = ti.xcom_pull(task_ids="print_greeting")
    print(f"Previous task result: {greeting_result}")
    print("Pipeline execution completed successfully!")


with DAG(
    dag_id="example_hello_world",
    default_args=default_args,
    description="Simple DAG demonstrating basic Airflow concepts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "getting-started"],
) as dag:

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command='echo "Airflow home: $AIRFLOW_HOME" && echo "Python: $(python --version)"',
    )

    print_greeting = PythonOperator(
        task_id="print_greeting",
        python_callable=_print_greeting,
    )

    print_date = BashOperator(
        task_id="print_date",
        bash_command='echo "Current date: $(date)"',
    )

    print_summary = PythonOperator(
        task_id="print_summary",
        python_callable=_print_summary,
    )

    check_environment >> print_greeting >> print_date >> print_summary
