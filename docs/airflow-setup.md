# Airflow Setup Guide

## Architecture Overview

The data platform uses Apache Airflow with the **LocalExecutor**, backed by PostgreSQL as the metadata database. This provides parallel task execution on a single node without requiring a separate message broker (unlike CeleryExecutor).

```
┌──────────────────────────────────────────────────┐
│                 Docker Compose                    │
│                                                   │
│  ┌─────────────┐  ┌─────────────┐                │
│  │  Scheduler   │  │  Webserver   │  port 8080    │
│  │  (parses &   │  │  (UI &       │               │
│  │   triggers)  │  │   REST API)  │               │
│  └──────┬───────┘  └──────┬──────┘               │
│         │                 │                       │
│         └────────┬────────┘                       │
│                  │                                │
│         ┌────────▼────────┐                       │
│         │   PostgreSQL     │  Metadata DB          │
│         │   (airflow DB)   │  port 5432            │
│         └─────────────────┘                       │
│                                                   │
│         ┌─────────────────┐                       │
│         │     MinIO        │  Object Storage       │
│         │  (S3-compatible) │  port 9000/9001       │
│         └─────────────────┘                       │
└──────────────────────────────────────────────────┘
```

## LocalExecutor vs Other Executors

| Executor | Use Case | Requirements |
|----------|----------|-------------|
| **LocalExecutor** (current) | Single-node, moderate parallelism | PostgreSQL |
| SequentialExecutor | Testing only, no parallelism | SQLite |
| CeleryExecutor | Multi-node, high parallelism | Redis/RabbitMQ + PostgreSQL |
| KubernetesExecutor | Cloud-native, dynamic scaling | Kubernetes cluster |

LocalExecutor is suitable for development and moderate production workloads. For high-throughput production use, consider migrating to CeleryExecutor or KubernetesExecutor.

## Custom Docker Image

The custom Airflow image (`infrastructure/airflow/Dockerfile`) extends the official `apache/airflow:2.9.3` image with:

- System packages: `postgresql-client`, `curl`, `jq`
- Python packages defined in `infrastructure/airflow/requirements.txt`
- Pre-installed providers for Amazon S3 and PostgreSQL

### Adding New Dependencies

1. Add the package to `infrastructure/airflow/requirements.txt`
2. Rebuild the image: `docker compose build`
3. Restart services: `make restart`

## Connection Management

Connections are initialized automatically during `airflow-init` via `infrastructure/airflow/init-connections.sh`. The script is idempotent and checks for existing connections before creating them.

### Pre-configured Connections

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `postgres_default` | PostgreSQL | Primary database |
| `minio_default` | AWS (S3) | MinIO object storage |

### Adding a New Connection

1. Add the connection definition to `pipelines/config/connections.yaml`
2. Add the `airflow connections add` command to `init-connections.sh`
3. Add required environment variables to `.env` and `.env.example`
4. Restart the airflow-init service: `docker compose restart airflow-init`

## Custom Operators

Custom operators are in `pipelines/operators/` and are mounted into the Airflow container at `/opt/airflow/operators/`.

### MinIOUploadOperator

Uploads a local file to a MinIO bucket with automatic bucket creation and retry logic.

```python
from operators.minio_operator import MinIOUploadOperator

upload = MinIOUploadOperator(
    task_id="upload_data",
    bucket_name="raw",
    object_name="data/{{ ds }}/export.csv",
    file_path="/tmp/export.csv",
)
```

### PostgresTableSensorOperator

Waits for a PostgreSQL table to exist and optionally checks minimum row count.

```python
from operators.postgres_operator import PostgresTableSensorOperator

wait = PostgresTableSensorOperator(
    task_id="wait_for_table",
    table_name="users",
    min_row_count=100,
    poke_interval=30,
    timeout=600,
)
```

### DataQualityCheckOperator

Runs SQL-based data quality checks with configurable thresholds.

```python
from operators.data_quality_operator import DataQualityCheckOperator

quality = DataQualityCheckOperator(
    task_id="check_quality",
    sql_checks=[
        {"sql": "SELECT COUNT(*) FROM users;", "description": "Users exist", "min_threshold": 1},
        {"sql": "SELECT COUNT(*) - COUNT(DISTINCT email) FROM users;", "description": "No duplicate emails", "max_threshold": 0},
    ],
)
```

### MinIOFileSensorOperator

Waits for files to appear in a MinIO bucket with optional wildcard pattern matching.

```python
from operators.file_sensor_operator import MinIOFileSensorOperator

wait = MinIOFileSensorOperator(
    task_id="wait_for_file",
    bucket_name="raw",
    prefix="data/2024-01-01/*.csv",
    wildcard_match=True,
    poke_interval=60,
    timeout=3600,
)
```

## Performance Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `AIRFLOW__CORE__PARALLELISM` | 32 | Max concurrent tasks |
| `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG` | 3 | Max concurrent DAG runs |
| `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` | 60 | DAG scan interval (seconds) |
| `AIRFLOW__LOGGING__LOGGING_LEVEL` | INFO | Log verbosity |

Adjust these in `.env` based on your workload and available resources.

## Monitoring and Alerting

The `example_monitoring` DAG provides basic health monitoring:

- Scheduler heartbeat checks
- DAG parsing error detection
- Recent task failure reporting
- System statistics collection

For production use, consider enabling StatsD metrics (`AIRFLOW__METRICS__STATSD_ON=True`) and connecting to Prometheus/Grafana.
