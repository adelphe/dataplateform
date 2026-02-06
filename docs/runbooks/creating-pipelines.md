# Runbook: Creating Data Pipelines

This runbook covers creating and deploying data pipelines in the platform.

## Pipeline Types

| Type | Use Case | Schedule |
|------|----------|----------|
| Ingestion | Extract data from sources | Hourly/Daily |
| Transformation | dbt model runs | After ingestion |
| Quality | Data validation | After transformation |
| Export | Send data downstream | On-demand/Scheduled |

## Quick Start: Create a Basic Pipeline

### Step 1: Create DAG File

Create a new file at `pipelines/dags/<category>/<dag_name>.py`:

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _my_task(**context):
    """Task implementation."""
    ds = context["ds"]
    print(f"Processing for {ds}")


with DAG(
    dag_id="my_pipeline",
    default_args=default_args,
    description="Pipeline description",
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

### Step 2: Test Locally

```bash
# Check for syntax errors
make airflow-cli CMD="dags list-import-errors"

# Test the DAG
make airflow-cli CMD="dags test my_pipeline 2024-01-01"

# Test a single task
make airflow-cli CMD="tasks test my_pipeline my_task 2024-01-01"
```

### Step 3: Deploy

The DAG will be automatically picked up within 60 seconds after saving the file.

---

## Creating an Ingestion Pipeline

### Full Pipeline Example

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.minio_operator import MinIOUploadOperator
from operators.data_quality_operator import DataQualityCheckOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract_data(**context):
    """Extract data from source."""
    from ingestion.database_ingestor import extract_table_incremental

    ds = context["ds"]
    result = extract_table_incremental(
        table_name="orders",
        schema="public",
        watermark_column="updated_at",
        object_key=f"postgres/orders/{ds}/orders.parquet",
        execution_date=ds,
    )

    context["ti"].xcom_push(key="row_count", value=result.get("row_count", 0))
    context["ti"].xcom_push(key="object_key", value=f"postgres/orders/{ds}/orders.parquet")


def _log_metadata(**context):
    """Log ingestion metadata."""
    from ingestion.metadata_logger import log_ingestion_success

    ti = context["ti"]
    row_count = ti.xcom_pull(key="row_count", task_ids="extract_data")

    log_ingestion_success(
        source_name="postgres.orders",
        row_count=row_count,
    )


with DAG(
    dag_id="ingest_orders",
    default_args=default_args,
    description="Incremental ingestion of orders table",
    schedule="0 */4 * * *",  # Every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "orders"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    validate = DataQualityCheckOperator(
        task_id="validate_row_count",
        sql="SELECT COUNT(*) FROM raw.orders WHERE DATE(created_at) = '{{ ds }}'",
        conn_id="postgres_default",
        min_threshold=1,
        description="Ensure at least one row was loaded",
    )

    log = PythonOperator(
        task_id="log_metadata",
        python_callable=_log_metadata,
    )

    extract >> validate >> log
```

---

## Creating a Transformation Pipeline

### dbt Pipeline Example

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from operators.dbt_operator import DbtRunOperator, DbtTestOperator
from operators.loader_operator import MinIOToPostgresOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transform_orders",
    default_args=default_args,
    description="Transform orders data through medallion layers",
    schedule="0 8 * * *",  # Daily at 8 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "dbt", "orders"],
) as dag:

    # Wait for ingestion to complete
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="ingest_orders",
        external_task_id=None,  # Wait for entire DAG
        timeout=3600,
        poke_interval=60,
    )

    # Load from MinIO to PostgreSQL raw schema
    load_to_raw = MinIOToPostgresOperator(
        task_id="load_to_raw",
        bucket_name="raw",
        object_key="postgres/orders/{{ ds }}/orders.parquet",
        table_name="orders",
        schema="raw",
        load_mode="replace",
    )

    # Run dbt staging models
    dbt_staging = DbtRunOperator(
        task_id="dbt_run_staging",
        project_dir="/opt/airflow/transformations/dbt_project",
        select="staging.stg_orders",
    )

    # Test staging models
    dbt_test_staging = DbtTestOperator(
        task_id="dbt_test_staging",
        project_dir="/opt/airflow/transformations/dbt_project",
        select="staging.stg_orders",
    )

    # Run dbt mart models
    dbt_marts = DbtRunOperator(
        task_id="dbt_run_marts",
        project_dir="/opt/airflow/transformations/dbt_project",
        select="marts.mart_order_analytics",
    )

    # Test mart models
    dbt_test_marts = DbtTestOperator(
        task_id="dbt_test_marts",
        project_dir="/opt/airflow/transformations/dbt_project",
        select="marts.mart_order_analytics",
    )

    (
        wait_for_ingestion
        >> load_to_raw
        >> dbt_staging
        >> dbt_test_staging
        >> dbt_marts
        >> dbt_test_marts
    )
```

---

## Creating a Quality Pipeline

### Great Expectations Pipeline

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.ge_operator import GreatExpectationsValidationOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _generate_data_docs(**context):
    """Generate Great Expectations Data Docs."""
    import great_expectations as gx

    context_gx = gx.get_context(
        context_root_dir="/opt/airflow/data-quality"
    )
    context_gx.build_data_docs()


with DAG(
    dag_id="quality_full_pipeline",
    default_args=default_args,
    description="Full pipeline data quality validation",
    schedule="0 10 * * *",  # Daily at 10 AM (after transforms)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "great-expectations"],
) as dag:

    validate_raw = GreatExpectationsValidationOperator(
        task_id="validate_raw_layer",
        checkpoint_name="raw_data_checkpoint",
        data_context_root_dir="/opt/airflow/data-quality",
    )

    validate_staging = GreatExpectationsValidationOperator(
        task_id="validate_staging_layer",
        checkpoint_name="staging_checkpoint",
        data_context_root_dir="/opt/airflow/data-quality",
    )

    validate_marts = GreatExpectationsValidationOperator(
        task_id="validate_marts_layer",
        checkpoint_name="marts_checkpoint",
        data_context_root_dir="/opt/airflow/data-quality",
    )

    generate_docs = PythonOperator(
        task_id="generate_data_docs",
        python_callable=_generate_data_docs,
    )

    [validate_raw, validate_staging, validate_marts] >> generate_docs
```

---

## Using Custom Operators

### Available Operators

| Operator | Import | Purpose |
|----------|--------|---------|
| `MinIOUploadOperator` | `operators.minio_operator` | Upload files to MinIO |
| `MinIOFileSensorOperator` | `operators.file_sensor_operator` | Wait for files in MinIO |
| `PostgresTableSensorOperator` | `operators.postgres_operator` | Wait for table readiness |
| `DataQualityCheckOperator` | `operators.data_quality_operator` | SQL-based quality checks |
| `GreatExpectationsValidationOperator` | `operators.ge_operator` | Run GE checkpoints |
| `DbtRunOperator` | `operators.dbt_operator` | Execute dbt run |
| `DbtTestOperator` | `operators.dbt_operator` | Execute dbt test |
| `MinIOToPostgresOperator` | `operators.loader_operator` | Load Parquet to PostgreSQL |

### Import Pattern

```python
import sys
sys.path.insert(0, "/opt/airflow")

from operators.minio_operator import MinIOUploadOperator
from operators.dbt_operator import DbtRunOperator, DbtTestOperator
```

---

## TaskFlow API Pattern

For Python-heavy pipelines, use the TaskFlow API:

```python
from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="taskflow_example",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
)
def taskflow_pipeline():

    @task()
    def extract():
        """Extract data from source."""
        return {"records": [1, 2, 3, 4, 5]}

    @task()
    def transform(data: dict):
        """Transform the data."""
        records = data["records"]
        return [x * 2 for x in records]

    @task()
    def load(transformed: list):
        """Load transformed data."""
        print(f"Loading {len(transformed)} records")
        return {"loaded": len(transformed)}

    raw = extract()
    transformed = transform(raw)
    load(transformed)


taskflow_pipeline()
```

---

## Error Handling and Alerts

### Configure Failure Callbacks

```python
from utils.alerting import format_task_failure_alert, send_slack_alert


def _on_failure(context):
    """Handle task failure."""
    alert = format_task_failure_alert(context)
    send_slack_alert(
        subject=f"Task Failed: {context['task_instance'].task_id}",
        message=alert,
    )


with DAG(
    ...,
    on_failure_callback=_on_failure,
):
    ...
```

### SLA Monitoring

```python
task = PythonOperator(
    task_id="critical_task",
    python_callable=my_function,
    sla=timedelta(hours=2),  # Alert if exceeds 2 hours
)
```

---

## Testing Pipelines

### Unit Tests

Create tests at `tests/unit/dags/test_<dag_name>.py`:

```python
import pytest
from airflow.models import DagBag


def test_dag_loads():
    """Test that DAG loads without errors."""
    dagbag = DagBag(dag_folder="pipelines/dags", include_examples=False)
    assert "my_pipeline" in dagbag.dags
    assert dagbag.import_errors == {}


def test_dag_structure():
    """Test DAG structure."""
    dagbag = DagBag(dag_folder="pipelines/dags", include_examples=False)
    dag = dagbag.get_dag("my_pipeline")

    assert dag is not None
    assert len(dag.tasks) > 0
    assert dag.schedule_interval is not None
```

### Integration Tests

```bash
# Test full DAG run
make airflow-cli CMD="dags test my_pipeline 2024-01-01"

# Run pytest integration tests
python -m pytest tests/integration/ -v
```

---

## Deployment Checklist

- [ ] DAG file created in correct directory
- [ ] No import errors (`make airflow-cli CMD="dags list-import-errors"`)
- [ ] Local test passes (`dags test <dag_id> <date>`)
- [ ] Unit tests written and passing
- [ ] Tags added for filtering
- [ ] Description provided
- [ ] Retries and error handling configured
- [ ] SLA set for critical tasks (if applicable)
- [ ] Failure callbacks configured (if applicable)
- [ ] Documentation updated

## Troubleshooting

### DAG not appearing

```bash
# Check import errors
make airflow-cli CMD="dags list-import-errors"

# Force refresh
make airflow-cli CMD="dags reserialize"
```

### Task failing

1. Check task logs in Airflow UI
2. Test task individually: `make airflow-cli CMD="tasks test <dag> <task> <date>"`
3. Check operator documentation

### Dependencies not resolving

```bash
# Verify task dependencies
make airflow-cli CMD="dags show <dag_id>"
```

## Related Documentation

- [DAG Development Guide](../dag-development-guide.md)
- [Adding Data Sources](adding-data-sources.md)
- [Troubleshooting](troubleshooting.md)
