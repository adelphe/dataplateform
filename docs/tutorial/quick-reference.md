# Data Platform Quick Reference

A one-page reference for common commands and URLs.

## Service URLs and Credentials

| Service | URL | Username | Password |
|---------|-----|----------|----------|
| Airflow | http://localhost:8080 | admin | admin |
| Superset | http://localhost:8088 | admin | admin |
| MinIO Console | http://localhost:9001 | minio | minio123 |
| OpenMetadata | http://localhost:8585 | admin | admin |
| PostgreSQL | localhost:5432 | airflow | airflow |

## Essential Commands

### Platform Management

```bash
# Start the platform
make start

# Stop the platform
make stop

# Restart all services
make restart

# Check status
make status

# View all logs
make logs

# Clean everything (removes data!)
make clean
```

### Airflow Commands

```bash
# List all DAGs
make airflow-cli CMD="dags list"

# Enable a DAG
make airflow-cli CMD="dags unpause <dag_id>"

# Disable a DAG
make airflow-cli CMD="dags pause <dag_id>"

# Trigger a DAG
make airflow-cli CMD="dags trigger <dag_id>"

# View task logs
make airflow-cli CMD="tasks logs <dag_id> <task_id> <execution_date>"

# Test a task
make airflow-cli CMD="tasks test <dag_id> <task_id> 2024-01-01"

# List DAG import errors
make airflow-cli CMD="dags list-import-errors"

# List connections
make airflow-connections
```

### MinIO Commands

```bash
# List buckets
make minio-buckets

# Upload sample data
make minio-upload-sample

# Verify bucket structure
make minio-verify

# MinIO CLI (mc) commands inside container
docker compose exec minio mc ls local/
docker compose exec minio mc ls local/raw/
docker compose exec minio mc cp <source> local/<bucket>/<path>
```

### PostgreSQL Commands

```bash
# Open database shell
make db-shell

# Run a query
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM table LIMIT 5;"

# List schemas
docker compose exec postgres psql -U airflow -d airflow -c "\dn"

# List tables in a schema
docker compose exec postgres psql -U airflow -d airflow -c "\dt staging.*"

# Describe a table
docker compose exec postgres psql -U airflow -d airflow -c "\d staging.stg_customers"
```

### dbt Commands

```bash
# Run all models
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt run"

# Run specific model
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt run --select model_name"

# Run tests
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt test"

# Generate docs
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt docs generate"

# Check connections
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt debug"
```

### Docker Commands

```bash
# View running containers
docker compose ps

# View logs for a service
docker compose logs <service>

# Follow logs
docker compose logs -f <service>

# Restart a service
docker compose restart <service>

# Execute command in container
docker compose exec <service> <command>

# Remove all containers and volumes
docker compose down -v
```

## Data Flow Overview

```
Source Data → Bronze (MinIO raw) → Silver (PostgreSQL staging) → Gold (PostgreSQL marts) → Dashboards
```

## Storage Layers

| Layer | MinIO Bucket | PostgreSQL Schema | Purpose |
|-------|--------------|-------------------|---------|
| Bronze | raw | - | Raw, unprocessed data |
| Silver | staging | staging | Cleaned, validated data |
| Gold | curated | marts | Business-ready analytics |

## Key File Locations

| Component | Location |
|-----------|----------|
| DAGs | `pipelines/dags/` |
| Custom Operators | `pipelines/operators/` |
| Utilities | `pipelines/utils/` |
| dbt Models | `transformations/dbt_project/models/` |
| GE Expectations | `data-quality/great_expectations/expectations/` |
| Docker Config | `docker-compose.yml` |
| Environment | `.env` |

## Cron Schedule Reference

| Expression | Meaning |
|------------|---------|
| `None` | Manual trigger only |
| `@once` | Run once |
| `@hourly` | Every hour |
| `@daily` | Every day at midnight |
| `@weekly` | Every Sunday at midnight |
| `@monthly` | First day of month |
| `0 6 * * *` | Every day at 6 AM |
| `0 */2 * * *` | Every 2 hours |
| `0 0 * * 1-5` | Weekdays at midnight |

## Common Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | Check `dags list-import-errors` |
| Service unhealthy | Check `docker compose logs <service>` |
| Port in use | Change port in `docker-compose.yml` or stop conflicting process |
| Out of memory | Increase Docker memory or stop unused services |
| dbt failing | Run `dbt debug` to check connection |

## Useful SQL Queries

```sql
-- Check ingestion log
SELECT * FROM metadata.ingestion_log ORDER BY ingested_at DESC LIMIT 10;

-- Check data quality results
SELECT * FROM data_quality.validation_results ORDER BY run_time DESC LIMIT 10;

-- Count rows in staging
SELECT
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'staging';

-- Check recent Airflow task runs
SELECT
    dag_id,
    task_id,
    state,
    start_date
FROM task_instance
ORDER BY start_date DESC
LIMIT 20;
```
