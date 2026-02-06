# Runbook: Troubleshooting

This runbook covers common issues and their resolutions.

## Quick Diagnostics

### Check Platform Health

```bash
# View all service status
docker-compose ps

# Check Airflow scheduler health
make airflow-cli CMD="jobs check"

# List DAG import errors
make airflow-cli CMD="dags list-import-errors"

# Check recent task failures
make airflow-cli CMD="tasks failed-deps"
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin/admin |
| MinIO Console | http://localhost:9001 | minio/minio123 |
| Superset | http://localhost:8088 | admin/admin |
| OpenMetadata | http://localhost:8585 | admin/admin |

---

## Airflow Issues

### DAG Not Appearing in UI

**Symptoms**: New DAG file not visible in Airflow UI

**Diagnosis**:
```bash
# Check for import errors
make airflow-cli CMD="dags list-import-errors"

# Verify file is in correct location
ls -la pipelines/dags/

# Check file permissions
ls -la pipelines/dags/<your_dag>.py
```

**Solutions**:

1. **Fix Python syntax errors**: Check the import error output
2. **Fix import paths**: Ensure custom modules use correct paths:
   ```python
   import sys
   sys.path.insert(0, "/opt/airflow")
   ```
3. **Force DAG refresh**:
   ```bash
   make airflow-cli CMD="dags reserialize"
   ```
4. **Restart scheduler**:
   ```bash
   docker-compose restart airflow-scheduler
   ```

### DAG Stuck in Running State

**Symptoms**: DAG shows as running but tasks aren't progressing

**Diagnosis**:
```bash
# Check active DAG runs
make airflow-cli CMD="dags list-runs -d <dag_id> -s running"

# Check task instances
make airflow-cli CMD="tasks list <dag_id> -S"
```

**Solutions**:

1. **Clear stuck tasks**:
   ```bash
   make airflow-cli CMD="tasks clear <dag_id> -s <start_date> -e <end_date>"
   ```

2. **Mark DAG run as failed** (via UI):
   - Browse to DAG
   - Click on the stuck run
   - Select "Mark Failed" from actions

3. **Kill zombie processes**:
   ```bash
   docker-compose exec airflow-scheduler airflow celery stop
   ```

### Scheduler Not Running

**Symptoms**: DAGs not triggering on schedule

**Diagnosis**:
```bash
# Check scheduler process
docker-compose ps airflow-scheduler

# View scheduler logs
docker-compose logs airflow-scheduler --tail=100
```

**Solutions**:

1. **Restart scheduler**:
   ```bash
   docker-compose restart airflow-scheduler
   ```

2. **Check database connectivity**:
   ```bash
   docker-compose exec airflow-scheduler airflow db check
   ```

3. **Reinitialize database** (caution: may lose metadata):
   ```bash
   docker-compose exec airflow-scheduler airflow db upgrade
   ```

### Task Failures

**Symptoms**: Individual tasks failing

**Diagnosis**:
```bash
# View task logs
make airflow-cli CMD="tasks logs <dag_id> <task_id> <execution_date>"

# Test task in isolation
make airflow-cli CMD="tasks test <dag_id> <task_id> <execution_date>"
```

**Common Causes and Solutions**:

| Cause | Solution |
|-------|----------|
| Connection error | Verify connection in Admin > Connections |
| Out of memory | Increase Docker memory limits |
| Timeout | Increase task timeout parameter |
| Missing dependency | Check operator imports |
| Permission denied | Check file/bucket permissions |

---

## MinIO Issues

### Cannot Connect to MinIO

**Symptoms**: Tasks fail with connection errors to MinIO

**Diagnosis**:
```bash
# Check MinIO status
docker-compose ps minio

# Test connectivity
docker-compose exec airflow-scheduler curl http://minio:9000/minio/health/live
```

**Solutions**:

1. **Restart MinIO**:
   ```bash
   docker-compose restart minio
   ```

2. **Verify credentials**:
   ```bash
   # Check environment variables
   grep MINIO .env
   ```

3. **Check Airflow connection**:
   ```bash
   make airflow-cli CMD="connections get minio_default"
   ```

### Bucket Not Found

**Symptoms**: "Bucket does not exist" errors

**Diagnosis**:
```bash
# List buckets
docker-compose exec minio mc ls minio/
```

**Solutions**:

1. **Re-run bucket initialization**:
   ```bash
   docker-compose restart minio-init
   ```

2. **Create bucket manually**:
   ```bash
   docker-compose exec minio mc mb minio/raw
   docker-compose exec minio mc mb minio/staging
   docker-compose exec minio mc mb minio/curated
   ```

### File Upload Failures

**Symptoms**: MinIOUploadOperator fails

**Diagnosis**:
```bash
# Check bucket policy
docker-compose exec minio mc policy get minio/raw

# Verify file exists locally
docker-compose exec airflow-scheduler ls -la /tmp/<filename>
```

**Solutions**:

1. **Set bucket policy**:
   ```bash
   docker-compose exec minio mc policy set download minio/raw
   ```

2. **Check file permissions in container**
3. **Verify disk space**:
   ```bash
   docker-compose exec minio df -h
   ```

---

## PostgreSQL Issues

### Connection Refused

**Symptoms**: "Connection refused" errors to PostgreSQL

**Diagnosis**:
```bash
# Check PostgreSQL status
docker-compose ps postgres

# View logs
docker-compose logs postgres --tail=50
```

**Solutions**:

1. **Restart PostgreSQL**:
   ```bash
   docker-compose restart postgres
   ```

2. **Check port availability**:
   ```bash
   lsof -i :5432
   ```

3. **Verify connection settings**:
   ```bash
   docker-compose exec postgres pg_isready -U airflow
   ```

### Database Full

**Symptoms**: "No space left on device" or slow queries

**Diagnosis**:
```bash
# Check disk usage
docker-compose exec postgres df -h

# Check database size
docker-compose exec postgres psql -U airflow -c "
    SELECT pg_database.datname,
           pg_size_pretty(pg_database_size(pg_database.datname)) AS size
    FROM pg_database;"
```

**Solutions**:

1. **Vacuum database**:
   ```bash
   docker-compose exec postgres psql -U airflow -c "VACUUM FULL;"
   ```

2. **Clear old Airflow logs**:
   ```bash
   make airflow-cli CMD="db clean --clean-before-timestamp 2024-01-01"
   ```

3. **Increase volume size** (if using cloud storage)

### Query Performance Issues

**Symptoms**: Slow queries, timeouts

**Diagnosis**:
```bash
# Check active queries
docker-compose exec postgres psql -U airflow -c "
    SELECT pid, now() - pg_stat_activity.query_start AS duration, query
    FROM pg_stat_activity
    WHERE state = 'active';"
```

**Solutions**:

1. **Add missing indexes**
2. **Analyze tables**:
   ```bash
   docker-compose exec postgres psql -U airflow -c "ANALYZE;"
   ```
3. **Kill long-running queries**:
   ```bash
   docker-compose exec postgres psql -U airflow -c "SELECT pg_terminate_backend(<pid>);"
   ```

---

## dbt Issues

### dbt run Fails

**Symptoms**: DbtRunOperator fails

**Diagnosis**:
```bash
# Run dbt debug
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/transformations/dbt_project && dbt debug"

# Check model compilation
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/transformations/dbt_project && dbt compile --select <model>"
```

**Common Errors**:

| Error | Cause | Solution |
|-------|-------|----------|
| "Profile not found" | Missing profiles.yml | Verify file exists and has correct target |
| "Database error" | Invalid SQL | Check model SQL syntax |
| "Dependency error" | Missing upstream model | Run `dbt run --select +<model>` |
| "Permission denied" | Schema access | Grant permissions to dbt user |

### dbt test Fails

**Symptoms**: Data quality tests failing

**Diagnosis**:
```bash
# Run specific test with details
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/transformations/dbt_project && dbt test --select <model> --store-failures"

# Query failed test results
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM dbt_test__audit.<test_name> LIMIT 10;"
```

**Solutions**:

1. **Review failing rows in test results**
2. **Fix source data quality issues**
3. **Adjust test thresholds if business rules changed**

---

## Great Expectations Issues

### Checkpoint Fails to Run

**Symptoms**: GreatExpectationsValidationOperator fails

**Diagnosis**:
```bash
# List checkpoints
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/data-quality && great_expectations checkpoint list"

# Run checkpoint manually
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/data-quality && great_expectations checkpoint run <checkpoint_name>"
```

**Solutions**:

1. **Verify checkpoint exists**
2. **Check datasource configuration**
3. **Regenerate checkpoint**:
   ```bash
   great_expectations checkpoint delete <name>
   great_expectations checkpoint new <name>
   ```

### Data Docs Not Generating

**Symptoms**: Data Docs not updating after validation

**Diagnosis**:
```bash
# Check data docs site config
cat data-quality/great_expectations.yml | grep -A 10 data_docs_sites
```

**Solutions**:

1. **Manually build docs**:
   ```bash
   docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/data-quality && great_expectations docs build"
   ```

2. **Check write permissions to output directory**

---

## Superset Issues

### Dashboard Not Loading

**Symptoms**: Dashboards show "No results" or timeout

**Diagnosis**:
1. Check database connection in Superset settings
2. Run query in SQL Lab to test connectivity
3. Check query execution time

**Solutions**:

1. **Refresh metadata**:
   - Go to Data > Datasets
   - Click "Refresh" icon

2. **Clear cache**:
   - Settings > Security > Clear Cache

3. **Check database connection**:
   - Data > Database Connections > Test Connection

### Chart Query Timeout

**Symptoms**: Charts fail with timeout errors

**Solutions**:

1. **Increase query timeout** in database settings
2. **Optimize underlying query** (add indexes, simplify joins)
3. **Enable async queries** in Superset configuration

---

## OpenMetadata Issues

### Ingestion Failing

**Symptoms**: Metadata not syncing from sources

**Diagnosis**:
```bash
# Check OpenMetadata logs
docker-compose logs openmetadata-server --tail=100

# Verify service connectivity
curl http://localhost:8585/api/v1/services
```

**Solutions**:

1. **Check ingestion workflow logs** in OpenMetadata UI
2. **Verify credentials** in ingestion configuration
3. **Test source connectivity** from OpenMetadata container

### Elasticsearch Connection Issues

**Symptoms**: Search not working in OpenMetadata

**Diagnosis**:
```bash
# Check Elasticsearch status
docker-compose ps elasticsearch

# Verify connectivity
curl http://localhost:9200/_cluster/health
```

**Solutions**:

1. **Restart Elasticsearch**:
   ```bash
   docker-compose restart elasticsearch
   ```

2. **Reindex data** via OpenMetadata admin panel

---

## General Docker Issues

### Container Out of Memory

**Symptoms**: Containers crashing, OOM errors

**Diagnosis**:
```bash
# Check container stats
docker stats

# View OOM killed containers
docker inspect <container> | grep -i oom
```

**Solutions**:

1. **Increase Docker memory limits** in Docker Desktop settings
2. **Add memory limits to docker-compose.yml**:
   ```yaml
   services:
     airflow-scheduler:
       deploy:
         resources:
           limits:
             memory: 4G
   ```

### Disk Space Full

**Symptoms**: Services failing to start, "no space left" errors

**Diagnosis**:
```bash
# Check Docker disk usage
docker system df

# List large images/volumes
docker system df -v
```

**Solutions**:

1. **Prune unused resources**:
   ```bash
   docker system prune -a --volumes
   ```

2. **Remove old images**:
   ```bash
   docker image prune -a
   ```

### Network Issues

**Symptoms**: Services cannot communicate

**Diagnosis**:
```bash
# List networks
docker network ls

# Inspect network
docker network inspect data-platform_default
```

**Solutions**:

1. **Recreate network**:
   ```bash
   docker-compose down
   docker network prune
   docker-compose up -d
   ```

---

## Emergency Procedures

### Complete Platform Restart

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes data)
# docker-compose down -v

# Rebuild and start
docker-compose up -d --build
```

### Restore from Backup

```bash
# Restore PostgreSQL
docker-compose exec -T postgres pg_restore -U airflow -d airflow < backup.dump

# Restore MinIO
docker-compose exec minio mc cp --recursive /backup/ minio/
```

### Rollback DAG Changes

```bash
# Revert DAG file from git
git checkout HEAD~1 -- pipelines/dags/<dag_file>.py

# Clear any stuck runs
make airflow-cli CMD="dags delete-dag <dag_id>"
```

## Related Documentation

- [Monitoring Guide](../monitoring-guide.md)
- [Architecture](../architecture.md)
- [Developer Guide](../developer-guide.md)
