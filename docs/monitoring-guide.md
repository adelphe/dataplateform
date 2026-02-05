# Data Platform Monitoring Guide

This guide covers the monitoring and alerting infrastructure for the Data Platform.

## Overview

The monitoring system provides comprehensive observability across all platform components:

- **Airflow Health**: Scheduler status, task execution, DAG parsing errors
- **Data Freshness**: Tracking data staleness across Bronze/Silver/Gold layers
- **SLA Compliance**: Monitoring pipeline completion times against defined SLAs
- **Data Quality**: Integration with Great Expectations validation results
- **Alerting**: Multi-channel notifications via email and Slack

## Architecture

```
+-------------------+     +-------------------+     +-------------------+
|  Platform Health  |     |   Data Quality    |     |   Pipeline DAGs   |
|       DAG         |     |      Checks       |     | (ingestion, dbt)  |
+--------+----------+     +--------+----------+     +--------+----------+
         |                         |                         |
         v                         v                         v
+--------+-------------------------+-------------------------+----------+
|                          Alerting System                              |
|  (pipelines/utils/alerting.py)                                       |
+--------+-------------------------+-------------------------+----------+
         |                         |                         |
         v                         v                         v
+--------+----------+     +--------+----------+     +--------+----------+
|   Slack Webhook   |     |    Email/SMTP     |     |    PostgreSQL     |
+-------------------+     +-------------------+     | (metrics storage) |
                                                   +-------------------+
                                                            |
                                                            v
                                                   +-------------------+
                                                   | Superset Dashboard|
                                                   +-------------------+
```

## Monitoring Components

### 1. Platform Health DAG

**Location**: `pipelines/dags/monitoring/platform_health.py`
**Schedule**: Every 15 minutes

The platform health DAG performs comprehensive health checks:

| Task | Description |
|------|-------------|
| `check_scheduler_health` | Verifies Airflow scheduler heartbeat is recent |
| `check_dag_parsing_errors` | Detects DAG import/parsing failures |
| `monitor_task_failures` | Calculates 24-hour task failure rates |
| `check_data_freshness` | Validates data is being updated on schedule |
| `monitor_sla_compliance` | Tracks SLA misses and compliance percentage |
| `check_data_volumes` | Detects anomalies in data row counts |
| `check_system_resources` | Monitors queue depth and resource utilization |
| `generate_health_report` | Aggregates metrics and calculates health score |

### 2. Alerting System

**Location**: `pipelines/utils/alerting.py`

The alerting system supports:

- **Email Alerts**: SMTP-based email notifications
- **Slack Alerts**: Webhook-based Slack messages with rich formatting
- **Severity Levels**: CRITICAL, HIGH, MEDIUM, LOW
- **Retry Logic**: Automatic retries for failed notifications
- **Alert Suppression**: Configurable rules to reduce noise

### 3. Monitoring Operators

**Location**: `pipelines/operators/monitoring_operator.py`

Custom operators for monitoring tasks:

- `DataFreshnessCheckOperator`: Validates data freshness against SLA
- `SLACheckOperator`: Checks task/DAG completion times
- `VolumeAnomalyCheckOperator`: Detects unusual data volumes
- `HealthScoreOperator`: Retrieves current health score

## Configuration

### Environment Variables

Add these to your `.env` file:

```bash
# Enable alerting channels
ALERT_EMAIL_ENABLED=true
ALERT_SLACK_ENABLED=true

# SMTP configuration
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=alerts@example.com
SMTP_PASSWORD=your-password
SMTP_FROM_EMAIL=alerts@dataplatform.local

# Email recipients
ALERT_EMAIL_RECIPIENTS=ops@example.com,data-team@example.com

# Slack configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_ALERT_CHANNEL=#data-platform-alerts

# Alert thresholds
ALERT_SEVERITY_THRESHOLD=MEDIUM
SLA_MONITORING_ENABLED=true
```

### Alert Configuration

**Location**: `infrastructure/alerting/alert_config.yaml`

This file defines:

- Alert routing rules by DAG tag or pattern
- Severity level definitions and colors
- Suppression rules (e.g., don't alert on retries)
- Threshold values for various checks

## Severity Levels

| Level | Color | When to Use |
|-------|-------|-------------|
| CRITICAL | Red | Platform down, data loss risk, immediate action required |
| HIGH | Orange | Pipeline failures, SLA misses, data quality failures |
| MEDIUM | Yellow | Warnings, approaching thresholds, non-critical issues |
| LOW | Green | Informational, successful completions, status updates |

## Dashboards

### Platform Operations Dashboard

**URL**: `http://localhost:8088/superset/dashboard/platform-operations/`

Charts included:

1. **Platform Health Score**: Real-time health score (0-100)
2. **Total DAGs / Active DAGs**: DAG counts
3. **Failed Tasks (24h)**: Recent failure count
4. **SLA Compliance**: Percentage of on-time completions
5. **Data Freshness Heatmap**: Visual freshness by source/layer
6. **Task Failure Trends**: 30-day failure history
7. **Data Quality Metrics**: Validation success rates
8. **SLA Compliance Trends**: Historical compliance
9. **Unacknowledged Alerts**: Open issues requiring attention
10. **Ingestion Statistics**: Data volume trends

## Alert Types

### Task Failure Alert

Triggered when an Airflow task fails after all retries.

**Includes**:
- DAG and task identifiers
- Execution date and try number
- Exception details
- Link to task logs

### Data Quality Alert

Triggered when Great Expectations validations fail.

**Includes**:
- Dataset name and layer
- Failed expectation count
- Failure rate percentage
- Link to Data Docs

### SLA Miss Alert

Triggered when a task/DAG misses its SLA deadline.

**Includes**:
- DAG and task identifiers
- Expected vs actual completion time
- Delay duration

### Platform Health Alert

Triggered when health score drops below threshold.

**Includes**:
- Current health score
- Component status breakdown
- List of detected issues

## Troubleshooting

### Alerts Not Sending

1. Verify alerting is enabled:
   ```bash
   echo $ALERT_EMAIL_ENABLED
   echo $ALERT_SLACK_ENABLED
   ```

2. Check SMTP connectivity:
   ```bash
   make test-alerting
   ```

3. Verify Slack webhook URL is correct

4. Check severity threshold isn't filtering alerts:
   ```bash
   echo $ALERT_SEVERITY_THRESHOLD
   ```

### Dashboard Not Loading

1. Verify Superset is running:
   ```bash
   docker compose ps superset
   ```

2. Check database connections are configured:
   ```bash
   make superset-shell
   # Then: superset list-databases
   ```

3. Re-run dashboard setup:
   ```bash
   make superset-init
   ```

### Health Score Always 100

This usually means monitoring tables haven't been populated yet:

1. Trigger the platform health DAG manually:
   ```bash
   make airflow-cli CMD="dags trigger platform_health"
   ```

2. Wait for the DAG to complete (check Airflow UI)

3. Verify data in monitoring tables:
   ```sql
   SELECT * FROM data_platform.platform_health_metrics
   ORDER BY metric_time DESC LIMIT 5;
   ```

## Adding Custom Monitoring

### Custom Freshness Check

```python
from operators.monitoring_operator import DataFreshnessCheckOperator

check_my_source = DataFreshnessCheckOperator(
    task_id="check_my_source_freshness",
    source_name="my_data_source",
    max_staleness_hours=12,
    layer="staging",
)
```

### Custom Alert

```python
from utils.alerting import send_alert, AlertSeverity

send_alert({
    "title": "Custom Alert",
    "message": "Something important happened",
    "fields": [
        {"title": "Metric", "value": "123", "short": True}
    ],
    "severity": AlertSeverity.MEDIUM,
})
```

### Custom Health Check

Add a new task to the `platform_health.py` DAG:

```python
def _check_custom_metric(**context):
    # Your check logic here
    result = {"value": 42, "healthy": True}
    context["ti"].xcom_push(key="custom_metric", value=result)
    return result

check_custom = PythonOperator(
    task_id="check_custom_metric",
    python_callable=_check_custom_metric,
)

# Add to task dependencies
[..., check_custom] >> generate_health_report
```

## Health Score Calculation

The health score (0-100) is calculated based on:

| Component | Weight | Scoring |
|-----------|--------|---------|
| Scheduler Health | 20 pts | -20 if unhealthy |
| Task Failures | 25 pts | -5 to -25 based on failure count |
| SLA Compliance | 25 pts | -5 to -25 based on compliance % |
| Data Freshness | 20 pts | -5 to -20 based on stale source count |
| Queue Health | 10 pts | -5 to -10 based on queue depth |

## Runbook: Common Alert Responses

### "Scheduler heartbeat is stale"

1. Check scheduler container:
   ```bash
   docker compose logs airflow-scheduler --tail=50
   ```

2. Restart scheduler if needed:
   ```bash
   docker compose restart airflow-scheduler
   ```

### "High failure rate detected"

1. Check recent failures:
   ```bash
   make airflow-cli CMD="tasks failed-deps -S"
   ```

2. Review task logs in Airflow UI

3. Check for resource constraints

### "Data source X is stale"

1. Check upstream pipeline status
2. Verify source system connectivity
3. Check for data availability at source
4. Trigger manual ingestion if needed

### "SLA compliance below threshold"

1. Review SLA miss history in dashboard
2. Identify bottleneck tasks
3. Consider:
   - Increasing resources
   - Adjusting SLA expectations
   - Optimizing slow tasks

## Metrics Reference

### Tables

| Table | Description |
|-------|-------------|
| `data_platform.data_freshness_metrics` | Historical freshness checks |
| `data_platform.platform_health_metrics` | Aggregated health metrics |
| `data_platform.alert_history` | All alerts sent |
| `data_platform.sla_tracking` | SLA compliance records |

### Views

| View | Description |
|------|-------------|
| `vw_data_freshness_status` | Current freshness by source |
| `vw_platform_health_summary` | Latest health snapshot |
| `vw_daily_health_trends` | Daily aggregated metrics |
| `vw_unacknowledged_alerts` | Open alerts |
