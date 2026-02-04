"""Example DAG for monitoring Airflow health metrics.

This DAG periodically checks the Airflow metadata database for
scheduler heartbeat, DAG parsing errors, recent task failures,
and overall system health.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _check_scheduler_heartbeat(**context):
    """Check if the Airflow scheduler has sent a recent heartbeat."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
        SELECT MAX(latest_heartbeat)
        FROM job
        WHERE job_type = 'SchedulerJob'
          AND state = 'running';
    """
    result = hook.get_first(sql)

    if result and result[0]:
        last_heartbeat = result[0]
        print(f"Scheduler last heartbeat: {last_heartbeat}")
        age = datetime.utcnow() - last_heartbeat.replace(tzinfo=None)
        if age > timedelta(minutes=5):
            print(f"WARNING: Scheduler heartbeat is {age} old")
        else:
            print("Scheduler heartbeat is healthy")
    else:
        print("WARNING: No scheduler heartbeat found")


def _check_dag_parsing_errors(**context):
    """Check for DAG parsing errors in the import_error table."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
        SELECT filename, stacktrace, timestamp
        FROM import_error
        ORDER BY timestamp DESC
        LIMIT 10;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    errors = cursor.fetchall()
    cursor.close()
    conn.close()

    if errors:
        print(f"Found {len(errors)} DAG parsing error(s):")
        for filename, stacktrace, timestamp in errors:
            print(f"  - {filename} ({timestamp}): {stacktrace[:200]}")
    else:
        print("No DAG parsing errors found")


def _check_recent_failures(**context):
    """Check for task failures in the last 24 hours."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
        SELECT dag_id, task_id, state, start_date, end_date
        FROM task_instance
        WHERE state = 'failed'
          AND start_date > NOW() - INTERVAL '24 hours'
        ORDER BY start_date DESC
        LIMIT 20;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    failures = cursor.fetchall()
    cursor.close()
    conn.close()

    if failures:
        print(f"Found {len(failures)} task failure(s) in the last 24 hours:")
        for dag_id, task_id, state, start_date, end_date in failures:
            print(f"  - {dag_id}.{task_id}: {state} ({start_date})")
    else:
        print("No task failures in the last 24 hours")


def _check_system_stats(**context):
    """Collect overall system statistics."""
    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Count active DAGs
    dag_count = hook.get_first("SELECT COUNT(*) FROM dag WHERE is_active = true;")
    print(f"Active DAGs: {dag_count[0] if dag_count else 0}")

    # Count running tasks
    running = hook.get_first(
        "SELECT COUNT(*) FROM task_instance WHERE state = 'running';"
    )
    print(f"Running tasks: {running[0] if running else 0}")

    # Count queued tasks
    queued = hook.get_first(
        "SELECT COUNT(*) FROM task_instance WHERE state = 'queued';"
    )
    print(f"Queued tasks: {queued[0] if queued else 0}")


with DAG(
    dag_id="example_monitoring",
    default_args=default_args,
    description="Monitor Airflow health metrics",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "monitoring", "ops"],
) as dag:

    check_scheduler = PythonOperator(
        task_id="check_scheduler_heartbeat",
        python_callable=_check_scheduler_heartbeat,
    )

    check_parsing = PythonOperator(
        task_id="check_dag_parsing_errors",
        python_callable=_check_dag_parsing_errors,
    )

    check_failures = PythonOperator(
        task_id="check_recent_failures",
        python_callable=_check_recent_failures,
    )

    check_stats = PythonOperator(
        task_id="check_system_stats",
        python_callable=_check_system_stats,
    )

    [check_scheduler, check_parsing, check_failures] >> check_stats
