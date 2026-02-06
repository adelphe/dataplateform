# Developer Onboarding Guide

Welcome to the Data Platform! This guide will help you get started with development.

## Quick Start

### Prerequisites

- Docker Desktop (4.0+)
- Docker Compose (2.0+)
- Python 3.11+
- Git
- Make (optional, for convenience commands)

### 1. Clone and Configure

```bash
# Clone the repository
git clone <repository-url>
cd dataplateform

# Copy environment template
cp .env.example .env

# Review and customize settings (optional for local dev)
vim .env
```

### 2. Start the Platform

```bash
# Build and start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Access Services

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Superset | http://localhost:8088 | admin / admin |
| OpenMetadata | http://localhost:8585 | admin / admin |
| PostgreSQL | localhost:5432 | airflow / airflow |

### 4. Run Sample Pipeline

```bash
# Trigger the example DAG
make airflow-cli CMD="dags trigger example_hello_world"

# Check DAG status
make airflow-cli CMD="dags list-runs -d example_hello_world"
```

---

## Project Structure

```
dataplateform/
├── .github/workflows/      # CI/CD pipelines
├── data-quality/           # Great Expectations configuration
│   ├── expectations/       # Expectation suites
│   ├── checkpoints/        # Validation checkpoints
│   └── great_expectations.yml
├── docs/                   # Documentation
├── infrastructure/         # Infrastructure configuration
│   ├── airflow/           # Airflow Dockerfile and requirements
│   ├── alerting/          # Alert templates and config
│   ├── openmetadata/      # OpenMetadata ingestion configs
│   ├── postgres/          # PostgreSQL init scripts
│   └── superset/          # Superset setup scripts
├── pipelines/             # Airflow DAGs and modules
│   ├── dags/              # DAG definitions
│   │   ├── examples/      # Reference implementations
│   │   ├── ingestion/     # Ingestion DAGs
│   │   ├── transformation/# Transformation DAGs
│   │   ├── quality/       # Quality check DAGs
│   │   ├── monitoring/    # Monitoring DAGs
│   │   └── governance/    # Governance DAGs
│   ├── operators/         # Custom Airflow operators
│   ├── utils/             # Shared utilities
│   ├── ingestion/         # Ingestion modules
│   └── loaders/           # Data loading modules
├── scripts/               # Utility scripts
├── tests/                 # Test suites
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── transformations/       # dbt project
│   └── dbt_project/
│       ├── models/        # dbt models
│       ├── macros/        # dbt macros
│       ├── tests/         # dbt tests
│       └── dbt_project.yml
├── docker-compose.yml     # Service definitions
├── Makefile               # Development commands
├── pyproject.toml         # Python project config
└── .env.example           # Environment template
```

---

## Development Workflow

### Creating a New DAG

1. **Create DAG file**:
   ```bash
   touch pipelines/dags/<category>/<dag_name>.py
   ```

2. **Write DAG code** (see template below)

3. **Verify no import errors**:
   ```bash
   make airflow-cli CMD="dags list-import-errors"
   ```

4. **Test locally**:
   ```bash
   make airflow-cli CMD="dags test <dag_id> 2024-01-01"
   ```

5. **Write tests**:
   ```bash
   touch tests/unit/dags/test_<dag_name>.py
   ```

6. **Run tests**:
   ```bash
   pytest tests/unit/dags/test_<dag_name>.py -v
   ```

### DAG Template

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

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
    # Your logic here


with DAG(
    dag_id="my_dag_name",
    default_args=default_args,
    description="What this DAG does",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["team", "domain"],
) as dag:

    task = PythonOperator(
        task_id="my_task",
        python_callable=_my_task,
    )
```

---

## Custom Operators API Reference

### MinIOUploadOperator

Upload files to MinIO buckets.

```python
from operators.minio_operator import MinIOUploadOperator

upload = MinIOUploadOperator(
    task_id="upload_file",
    local_file_path="/path/to/file.parquet",
    bucket_name="raw",
    object_key="source/table/{{ ds }}/data.parquet",
    conn_id="minio_default",  # optional
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `local_file_path` | str | Yes | Path to local file |
| `bucket_name` | str | Yes | Target MinIO bucket |
| `object_key` | str | Yes | Object key in bucket |
| `conn_id` | str | No | Airflow connection ID |

### MinIOFileSensorOperator

Wait for files to appear in MinIO.

```python
from operators.file_sensor_operator import MinIOFileSensorOperator

sensor = MinIOFileSensorOperator(
    task_id="wait_for_file",
    bucket_name="raw",
    prefix="source/table/{{ ds }}/",
    wildcard_pattern="*.parquet",
    timeout=3600,
    poke_interval=60,
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket_name` | str | Yes | MinIO bucket to watch |
| `prefix` | str | No | Object prefix to filter |
| `wildcard_pattern` | str | No | Pattern for matching |
| `timeout` | int | No | Max wait time (seconds) |
| `poke_interval` | int | No | Check interval (seconds) |

### DataQualityCheckOperator

Execute SQL-based quality checks.

```python
from operators.data_quality_operator import DataQualityCheckOperator

check = DataQualityCheckOperator(
    task_id="check_row_count",
    sql="SELECT COUNT(*) FROM raw.orders WHERE date = '{{ ds }}'",
    conn_id="postgres_default",
    min_threshold=1,
    max_threshold=1000000,
    description="Verify order count is within expected range",
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | str | Yes | SQL query returning single value |
| `conn_id` | str | Yes | Database connection ID |
| `min_threshold` | float | No | Minimum acceptable value |
| `max_threshold` | float | No | Maximum acceptable value |
| `description` | str | No | Check description |

### GreatExpectationsValidationOperator

Run Great Expectations checkpoints.

```python
from operators.ge_operator import GreatExpectationsValidationOperator

validate = GreatExpectationsValidationOperator(
    task_id="validate_data",
    checkpoint_name="raw_data_checkpoint",
    data_context_root_dir="/opt/airflow/data-quality",
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `checkpoint_name` | str | Yes | GE checkpoint name |
| `data_context_root_dir` | str | Yes | Path to GE context |

### DbtRunOperator / DbtTestOperator

Execute dbt commands.

```python
from operators.dbt_operator import DbtRunOperator, DbtTestOperator

run = DbtRunOperator(
    task_id="dbt_run",
    project_dir="/opt/airflow/transformations/dbt_project",
    select="staging.stg_orders",  # optional
    exclude="staging.stg_temp",   # optional
)

test = DbtTestOperator(
    task_id="dbt_test",
    project_dir="/opt/airflow/transformations/dbt_project",
    select="staging",
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_dir` | str | Yes | Path to dbt project |
| `select` | str | No | dbt select expression |
| `exclude` | str | No | dbt exclude expression |

### MinIOToPostgresOperator

Load Parquet files from MinIO to PostgreSQL.

```python
from operators.loader_operator import MinIOToPostgresOperator

load = MinIOToPostgresOperator(
    task_id="load_data",
    bucket_name="raw",
    object_key="source/table/{{ ds }}/data.parquet",
    table_name="orders",
    schema="raw",
    load_mode="replace",  # or "append"
)
```

**Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket_name` | str | Yes | Source MinIO bucket |
| `object_key` | str | Yes | Object key to load |
| `table_name` | str | Yes | Target PostgreSQL table |
| `schema` | str | Yes | Target schema |
| `load_mode` | str | No | "replace" or "append" |

---

## Utility Modules API Reference

### Storage Utilities (`utils/storage.py`)

```python
from utils.storage import StorageLayer, upload_to_layer, download_from_layer, promote_data

# Upload to a layer
upload_to_layer(
    local_path="/tmp/data.parquet",
    layer=StorageLayer.BRONZE,
    object_key="source/table/data.parquet"
)

# Download from a layer
download_from_layer(
    layer=StorageLayer.SILVER,
    object_key="cleaned/table/data.parquet",
    local_path="/tmp/data.parquet"
)

# Promote between layers
promote_data(
    source_key="cleaned/table/data.parquet",
    source_layer=StorageLayer.SILVER,
    target_layer=StorageLayer.GOLD,
    target_key="reports/table/data.parquet"
)
```

### MinIO Client (`utils/minio_client.py`)

```python
from utils.minio_client import (
    get_minio_client,
    upload_file,
    download_file,
    list_objects,
    bucket_exists,
    create_bucket,
)

# Get client
client = get_minio_client()

# Upload file
upload_file(
    bucket_name="raw",
    object_key="path/to/file.parquet",
    file_path="/local/path/file.parquet"
)

# Download file
download_file(
    bucket_name="raw",
    object_key="path/to/file.parquet",
    file_path="/local/destination.parquet"
)

# List objects
objects = list_objects(bucket_name="raw", prefix="source/")
```

### PostgreSQL Client (`utils/postgres_client.py`)

```python
from utils.postgres_client import (
    get_postgres_connection,
    execute_query,
    fetch_dataframe,
    bulk_insert,
)

# Execute query
execute_query("CREATE TABLE IF NOT EXISTS raw.new_table (...)")

# Fetch as DataFrame
df = fetch_dataframe("SELECT * FROM raw.orders LIMIT 100")

# Bulk insert
bulk_insert(df, table_name="orders", schema="raw")
```

### Alerting (`utils/alerting.py`)

```python
from utils.alerting import (
    AlertSeverity,
    send_email_alert,
    send_slack_alert,
    format_task_failure_alert,
)

# Send Slack alert
send_slack_alert(
    subject="Pipeline Failed",
    message="The orders pipeline failed during extraction.",
    severity=AlertSeverity.HIGH,
)

# Send email alert
send_email_alert(
    subject="Data Quality Alert",
    message="Row count below threshold in orders table.",
    recipients=["data-team@example.com"],
)
```

---

## Ingestion Modules API Reference

### File Ingestor (`ingestion/file_ingestor.py`)

```python
from ingestion.file_ingestor import ingest_csv_file, ingest_json_file, batch_ingest_files

# Ingest single CSV
result = ingest_csv_file(
    file_path="/data/input/orders.csv",
    object_key="files/orders/2024-01-15/orders.parquet",
    execution_date="2024-01-15"
)

# Ingest single JSON
result = ingest_json_file(
    file_path="/data/input/events.json",
    object_key="api/events/2024-01-15/events.parquet"
)

# Batch ingest directory
results = batch_ingest_files(
    directory="/data/input/sales",
    pattern="*.csv",
    execution_date="2024-01-15"
)
```

### Database Ingestor (`ingestion/database_ingestor.py`)

```python
from ingestion.database_ingestor import extract_table_full, extract_table_incremental

# Full table extract
result = extract_table_full(
    table_name="customers",
    schema="public",
    object_key="postgres/customers/2024-01-15/customers.parquet"
)

# Incremental extract
result = extract_table_incremental(
    table_name="orders",
    schema="public",
    watermark_column="updated_at",
    object_key="postgres/orders/2024-01-15/orders.parquet",
    execution_date="2024-01-15"
)
```

### Schema Detector (`ingestion/schema_detector.py`)

```python
from ingestion.schema_detector import detect_csv_schema, detect_json_schema, log_schema_change

# Detect schema from CSV
schema = detect_csv_schema("/path/to/file.csv")
# Returns: {"columns": [{"name": "id", "type": "integer"}, ...]}

# Log schema change
log_schema_change(
    source_name="postgres.orders",
    schema=schema,
    change_type="new"  # or "modified"
)
```

### Watermark Manager (`ingestion/watermark_manager.py`)

```python
from ingestion.watermark_manager import get_watermark, set_watermark

# Get current watermark
watermark = get_watermark(
    source_name="postgres.orders",
    watermark_column="updated_at"
)

# Set new watermark
set_watermark(
    source_name="postgres.orders",
    watermark_column="updated_at",
    watermark_value="2024-01-15T10:30:00"
)
```

### Metadata Logger (`ingestion/metadata_logger.py`)

```python
from ingestion.metadata_logger import (
    log_ingestion_start,
    log_ingestion_success,
    log_ingestion_failure,
    compute_file_checksum,
)

# Log start
run_id = log_ingestion_start(source_name="postgres.orders")

# Log success
log_ingestion_success(
    source_name="postgres.orders",
    row_count=1500,
    file_checksum="abc123...",
    watermark_value="2024-01-15T10:30:00"
)

# Log failure
log_ingestion_failure(
    source_name="postgres.orders",
    error_message="Connection timeout"
)
```

---

## Testing

### Running Tests

```bash
# All tests
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# With coverage
pytest tests/ --cov=pipelines --cov-report=html
```

### Writing Unit Tests

```python
# tests/unit/operators/test_minio_operator.py
import pytest
from unittest.mock import Mock, patch

from operators.minio_operator import MinIOUploadOperator


class TestMinIOUploadOperator:
    def test_init(self):
        op = MinIOUploadOperator(
            task_id="test",
            local_file_path="/tmp/test.parquet",
            bucket_name="raw",
            object_key="test/data.parquet",
        )
        assert op.bucket_name == "raw"

    @patch("operators.minio_operator.get_minio_client")
    def test_execute(self, mock_client):
        op = MinIOUploadOperator(
            task_id="test",
            local_file_path="/tmp/test.parquet",
            bucket_name="raw",
            object_key="test/data.parquet",
        )
        op.execute(context={})
        mock_client.return_value.upload_file.assert_called_once()
```

---

## Code Quality

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit
pre-commit install

# Run manually
pre-commit run --all-files
```

### Linting

```bash
# Ruff (Python linting)
ruff check pipelines/

# Black (formatting)
black pipelines/

# isort (imports)
isort pipelines/
```

### Type Checking

```bash
# mypy
mypy pipelines/
```

---

## Common Make Commands

```bash
# Start/stop platform
make up
make down

# View logs
make logs

# Airflow CLI
make airflow-cli CMD="dags list"
make airflow-cli CMD="dags test <dag_id> <date>"

# dbt commands
make dbt-run
make dbt-test
make dbt-docs

# Run tests
make test
make test-unit
make test-integration
```

---

## Environment Variables

Key environment variables (see `.env.example` for full list):

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_USER` | PostgreSQL username | airflow |
| `POSTGRES_PASSWORD` | PostgreSQL password | airflow |
| `MINIO_ROOT_USER` | MinIO access key | minio |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | minio123 |
| `AIRFLOW_ADMIN_USER` | Airflow admin username | admin |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow admin password | admin |
| `AIRFLOW__CORE__PARALLELISM` | Max concurrent tasks | 32 |
| `ALERT_SLACK_ENABLED` | Enable Slack alerts | false |
| `SLACK_WEBHOOK_URL` | Slack webhook URL | (empty) |

---

## Troubleshooting

See [Troubleshooting Runbook](runbooks/troubleshooting.md) for common issues.

### Quick Checks

```bash
# Service status
docker-compose ps

# DAG import errors
make airflow-cli CMD="dags list-import-errors"

# View logs
docker-compose logs <service> --tail=100

# Test database connection
docker-compose exec postgres pg_isready -U airflow
```

## Related Documentation

- [Architecture](architecture.md)
- [Data Flow](data-flow.md)
- [DAG Development Guide](dag-development-guide.md)
- [Runbooks](runbooks/)
