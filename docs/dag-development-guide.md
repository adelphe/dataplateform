# DAG Development Guide

## Quick Start

1. Create a new Python file in `pipelines/dags/`
2. Define a DAG with required metadata
3. Add tasks using operators
4. Set up task dependencies
5. The scheduler will pick up the new DAG within 60 seconds

## DAG Template

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _my_task(**context):
    """Task logic goes here."""
    ds = context["ds"]
    print(f"Processing data for {ds}")

with DAG(
    dag_id="my_pipeline",
    default_args=default_args,
    description="Brief description of what this pipeline does",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["team-name", "domain"],
) as dag:

    task = PythonOperator(
        task_id="my_task",
        python_callable=_my_task,
    )
```

## Naming Conventions

- **DAG IDs**: `snake_case`, descriptive of the pipeline's purpose
  - `ingest_api_users`, `transform_daily_metrics`, `export_report_to_s3`
- **Task IDs**: `snake_case`, describe the action
  - `extract_data`, `validate_schema`, `upload_to_minio`
- **File names**: Match the DAG ID: `pipelines/dags/ingest_api_users.py`

## File Organization

```
pipelines/dags/
├── examples/              # Example patterns (reference only)
│   ├── example_hello_world.py
│   └── ...
├── ingestion/             # Data ingestion DAGs
│   ├── ingest_api_users.py
│   └── ingest_db_orders.py
├── transformation/        # Data transformation DAGs
│   └── transform_daily_metrics.py
├── quality/               # Data quality DAGs
│   └── quality_check_users.py
└── export/                # Data export DAGs
    └── export_report.py
```

## Task Dependency Patterns

### Sequential

```python
task_a >> task_b >> task_c
```

### Fan-out / Fan-in

```python
start >> [branch_a, branch_b, branch_c] >> join
```

### Conditional

```python
from airflow.operators.python import BranchPythonOperator

def _choose_branch(**context):
    if condition:
        return "task_a"
    return "task_b"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=_choose_branch,
)
branch >> [task_a, task_b]
```

## Using Custom Operators

Custom operators are mounted at `/opt/airflow/operators/`. Import them with:

```python
import sys
sys.path.insert(0, "/opt/airflow")

from operators.minio_operator import MinIOUploadOperator
from operators.data_quality_operator import DataQualityCheckOperator
```

## TaskFlow API (Recommended)

For Python-heavy DAGs, use the TaskFlow API with `@task` decorators:

```python
from airflow.decorators import dag, task

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def my_pipeline():

    @task()
    def extract():
        return {"data": [1, 2, 3]}

    @task()
    def transform(data):
        return [x * 2 for x in data["data"]]

    @task()
    def load(transformed):
        print(f"Loading {len(transformed)} records")

    raw = extract()
    transformed = transform(raw)
    load(transformed)

my_pipeline()
```

## Error Handling

### Retries

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}
```

### Failure Callbacks

```python
def _on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    # Send alert (email, Slack, etc.)
    print(f"ALERT: {dag_id}.{task_id} failed")

with DAG(..., on_failure_callback=_on_failure):
    ...
```

### SLA Monitoring

```python
task = PythonOperator(
    task_id="critical_task",
    python_callable=my_function,
    sla=timedelta(hours=2),  # Alert if task takes longer than 2 hours
)
```

## Testing DAGs

### Syntax Validation

```bash
# Test a specific DAG
make airflow-test-dag DAG=my_pipeline

# List all DAGs and check for import errors
make airflow-cli CMD="dags list"
make airflow-cli CMD="dags list-import-errors"
```

### Unit Tests

```bash
# Run all pipeline tests
python -m pytest pipelines/tests -v

# Run specific test file
python -m pytest pipelines/tests/test_dags.py -v
```

### Local Testing

Test a DAG run for a specific date:

```bash
make airflow-cli CMD="dags test my_pipeline 2024-01-01"
```

Test a single task:

```bash
make airflow-cli CMD="tasks test my_pipeline my_task 2024-01-01"
```

## Best Practices

1. **Keep DAGs simple**: Each DAG should represent one logical pipeline
2. **Use catchup=False**: Unless you explicitly need backfilling
3. **Set retries**: Always configure retries for production DAGs
4. **Tag DAGs**: Use tags for filtering in the Airflow UI
5. **Avoid top-level code**: DAG files are parsed frequently; keep imports and logic minimal at the module level
6. **Use templates**: Leverage Jinja templating (`{{ ds }}`, `{{ params }}`) for dynamic values
7. **Document DAGs**: Add docstrings and descriptions for discoverability
8. **Test before deploying**: Run `airflow dags test` and unit tests before merging
