"""Platform Health Monitoring DAG.

Comprehensive monitoring DAG that tracks platform health metrics including:
- Airflow scheduler health
- DAG parsing errors
- Task failure rates
- Data freshness across layers
- SLA compliance
- Data volume anomalies
- System resource utilization

Schedule: Every 15 minutes
"""

import json
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/pipelines")

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def _get_db_connection():
    """Get PostgreSQL database connection."""
    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_conn()


def _on_failure_callback(context: Dict[str, Any]) -> None:
    """Alert on monitoring DAG failures."""
    try:
        from utils.alerting import format_task_failure_alert, send_alert, AlertSeverity

        alert_data = format_task_failure_alert(context)
        alert_data["severity"] = AlertSeverity.CRITICAL
        send_alert(alert_data)
    except Exception as e:
        print(f"Failed to send alert: {e}")


def _check_scheduler_health(**context) -> Dict[str, Any]:
    """Check Airflow scheduler heartbeat and health.

    Returns:
        Dictionary with scheduler health metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    # Check scheduler heartbeat from job table
    cursor.execute("""
        SELECT
            latest_heartbeat,
            EXTRACT(EPOCH FROM (NOW() - latest_heartbeat)) AS age_seconds,
            state
        FROM job
        WHERE job_type = 'SchedulerJob'
        ORDER BY latest_heartbeat DESC
        LIMIT 1
    """)

    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if not result:
        metrics = {
            "scheduler_healthy": False,
            "heartbeat_age_seconds": None,
            "issue": "No scheduler heartbeat found",
        }
    else:
        heartbeat, age_seconds, state = result
        is_healthy = age_seconds < 300  # 5 minutes
        metrics = {
            "scheduler_healthy": is_healthy,
            "heartbeat_age_seconds": int(age_seconds) if age_seconds else None,
            "last_heartbeat": str(heartbeat),
            "state": state,
        }

        if not is_healthy:
            metrics["issue"] = f"Scheduler heartbeat is {int(age_seconds)}s old (threshold: 300s)"

    context["ti"].xcom_push(key="scheduler_metrics", value=metrics)
    print(f"Scheduler health: {metrics}")
    return metrics


def _check_dag_parsing_errors(**context) -> Dict[str, Any]:
    """Check for DAG parsing/import errors.

    Returns:
        Dictionary with parsing error details
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            filename,
            timestamp,
            stacktrace
        FROM import_error
        ORDER BY timestamp DESC
        LIMIT 10
    """)

    errors = cursor.fetchall()
    cursor.close()
    conn.close()

    error_list = [
        {"filename": e[0], "timestamp": str(e[1]), "stacktrace": e[2][:500]}
        for e in errors
    ]

    metrics = {
        "error_count": len(errors),
        "errors": error_list,
        "has_errors": len(errors) > 0,
    }

    if metrics["has_errors"]:
        metrics["issue"] = f"{len(errors)} DAG parsing error(s) detected"

    context["ti"].xcom_push(key="parsing_errors", value=metrics)
    print(f"DAG parsing errors: {metrics['error_count']}")
    return metrics


def _monitor_task_failures(**context) -> Dict[str, Any]:
    """Monitor task failures in the last 24 hours.

    Returns:
        Dictionary with task failure metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    # Get failure counts by DAG
    cursor.execute("""
        SELECT
            dag_id,
            COUNT(*) as failure_count,
            COUNT(*) * 100.0 / NULLIF(
                (SELECT COUNT(*) FROM task_instance
                 WHERE start_date >= NOW() - INTERVAL '24 hours'
                 AND dag_id = ti.dag_id), 0
            ) as failure_rate
        FROM task_instance ti
        WHERE state = 'failed'
        AND start_date >= NOW() - INTERVAL '24 hours'
        GROUP BY dag_id
        ORDER BY failure_count DESC
        LIMIT 10
    """)

    failures_by_dag = cursor.fetchall()

    # Get total counts
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE state = 'failed') as failed,
            COUNT(*) FILTER (WHERE state = 'success') as success,
            COUNT(*) FILTER (WHERE state = 'running') as running,
            COUNT(*) FILTER (WHERE state = 'queued') as queued,
            COUNT(*) as total
        FROM task_instance
        WHERE start_date >= NOW() - INTERVAL '24 hours'
    """)

    totals = cursor.fetchone()
    cursor.close()
    conn.close()

    failed, success, running, queued, total = totals or (0, 0, 0, 0, 0)
    failure_rate = (failed / total * 100) if total > 0 else 0

    metrics = {
        "failed_tasks_24h": failed,
        "successful_tasks_24h": success,
        "running_tasks": running,
        "queued_tasks": queued,
        "total_tasks_24h": total,
        "failure_rate_percent": round(failure_rate, 2),
        "failures_by_dag": [
            {"dag_id": r[0], "failures": r[1], "rate": round(r[2] or 0, 2)}
            for r in failures_by_dag
        ],
    }

    # Check thresholds
    if failure_rate > 10:
        metrics["issue"] = f"High failure rate: {failure_rate:.1f}% (threshold: 10%)"
    if queued > 100:
        metrics["queue_issue"] = f"Task queue buildup: {queued} tasks queued (threshold: 100)"

    context["ti"].xcom_push(key="task_metrics", value=metrics)
    print(f"Task failures (24h): {failed}, Rate: {failure_rate:.1f}%")
    return metrics


def _check_data_freshness(**context) -> Dict[str, Any]:
    """Check data freshness across all layers.

    Checks freshness for:
    - Raw layer: ingestion metadata and watermarks (24h threshold)
    - Staging layer: staging tables (12h threshold)
    - Marts layer: business marts tables (6h threshold)

    Returns:
        Dictionary with freshness metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    freshness_results = []

    # Layer-specific thresholds (in hours)
    LAYER_THRESHOLDS = {
        "raw": 24,
        "staging": 12,
        "marts": 6,
    }

    # ======================
    # RAW LAYER CHECKS
    # ======================

    # Check ingestion metadata freshness
    cursor.execute("""
        SELECT
            source_name,
            MAX(updated_at) as last_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) / 3600 as hours_since_update,
            SUM(row_count) as total_rows
        FROM data_platform.ingestion_metadata
        WHERE status = 'success'
        GROUP BY source_name
    """)

    for row in cursor.fetchall():
        source, last_update, hours, row_count = row
        threshold = LAYER_THRESHOLDS["raw"]
        is_fresh = hours < threshold if hours is not None else False
        freshness_results.append({
            "source_name": source,
            "layer": "raw",
            "last_update": str(last_update) if last_update else None,
            "staleness_hours": round(hours, 2) if hours else None,
            "is_fresh": is_fresh,
            "row_count": int(row_count) if row_count else 0,
            "threshold_hours": threshold,
        })

    # Check watermarks freshness
    cursor.execute("""
        SELECT
            source_name,
            watermark_column,
            watermark_value,
            updated_at,
            EXTRACT(EPOCH FROM (NOW() - updated_at)) / 3600 as hours_since_update
        FROM data_platform.watermarks
    """)

    for row in cursor.fetchall():
        source, col, val, updated, hours = row
        threshold = LAYER_THRESHOLDS["raw"]
        is_fresh = hours < threshold if hours is not None else False
        freshness_results.append({
            "source_name": f"{source}.{col}",
            "layer": "raw",
            "last_update": str(updated) if updated else None,
            "staleness_hours": round(hours, 2) if hours else None,
            "is_fresh": is_fresh,
            "row_count": 0,  # Watermarks don't track row counts
            "threshold_hours": threshold,
        })

    # ======================
    # STAGING LAYER CHECKS
    # ======================

    # Check staging tables via data quality metrics or direct table inspection
    # First try to get staging table info from validation results
    cursor.execute("""
        SELECT
            table_name,
            MAX(validation_time) as last_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(validation_time))) / 3600 as hours_since_update
        FROM data_platform.validation_results
        WHERE layer = 'staging'
        AND success = true
        GROUP BY table_name
    """)

    staging_from_validation = cursor.fetchall()

    for row in staging_from_validation:
        table_name, last_update, hours = row
        threshold = LAYER_THRESHOLDS["staging"]
        is_fresh = hours < threshold if hours is not None else False

        # Try to get row count for this staging table
        row_count = 0
        try:
            cursor.execute(f"""
                SELECT COUNT(*) FROM staging.{table_name.split('.')[-1]}
            """)
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
        except Exception:
            pass  # Table may not exist or different schema

        freshness_results.append({
            "source_name": f"staging.{table_name}",
            "layer": "staging",
            "last_update": str(last_update) if last_update else None,
            "staleness_hours": round(hours, 2) if hours else None,
            "is_fresh": is_fresh,
            "row_count": row_count,
            "threshold_hours": threshold,
        })

    # Also check for staging tables directly if they have updated_at columns
    cursor.execute("""
        SELECT
            table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
        AND table_type = 'BASE TABLE'
        LIMIT 20
    """)

    staging_tables = [row[0] for row in cursor.fetchall()]
    checked_staging = {r["source_name"].split(".")[-1] for r in freshness_results if r["layer"] == "staging"}

    for table_name in staging_tables:
        if table_name in checked_staging:
            continue

        try:
            # Check if table has updated_at or a timestamp column
            cursor.execute(f"""
                SELECT
                    MAX(COALESCE(updated_at, created_at)) as last_update,
                    EXTRACT(EPOCH FROM (NOW() - MAX(COALESCE(updated_at, created_at)))) / 3600 as hours_since,
                    COUNT(*) as row_count
                FROM staging.{table_name}
            """)
            result = cursor.fetchone()
            if result and result[0]:
                last_update, hours, row_count = result
                threshold = LAYER_THRESHOLDS["staging"]
                is_fresh = hours < threshold if hours is not None else False
                freshness_results.append({
                    "source_name": f"staging.{table_name}",
                    "layer": "staging",
                    "last_update": str(last_update),
                    "staleness_hours": round(hours, 2) if hours else None,
                    "is_fresh": is_fresh,
                    "row_count": int(row_count) if row_count else 0,
                    "threshold_hours": threshold,
                })
        except Exception:
            pass  # Column may not exist

    # ======================
    # MARTS LAYER CHECKS
    # ======================

    # Check marts tables via data quality metrics first
    cursor.execute("""
        SELECT
            table_name,
            MAX(validation_time) as last_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(validation_time))) / 3600 as hours_since_update
        FROM data_platform.validation_results
        WHERE layer = 'marts'
        AND success = true
        GROUP BY table_name
    """)

    marts_from_validation = cursor.fetchall()

    for row in marts_from_validation:
        table_name, last_update, hours = row
        threshold = LAYER_THRESHOLDS["marts"]
        is_fresh = hours < threshold if hours is not None else False

        # Try to get row count for this marts table
        row_count = 0
        try:
            cursor.execute(f"""
                SELECT COUNT(*) FROM marts.{table_name.split('.')[-1]}
            """)
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
        except Exception:
            pass

        freshness_results.append({
            "source_name": f"marts.{table_name}",
            "layer": "marts",
            "last_update": str(last_update) if last_update else None,
            "staleness_hours": round(hours, 2) if hours else None,
            "is_fresh": is_fresh,
            "row_count": row_count,
            "threshold_hours": threshold,
        })

    # Check marts tables directly
    cursor.execute("""
        SELECT
            table_name
        FROM information_schema.tables
        WHERE table_schema = 'marts'
        AND table_type = 'BASE TABLE'
        LIMIT 20
    """)

    marts_tables = [row[0] for row in cursor.fetchall()]
    checked_marts = {r["source_name"].split(".")[-1] for r in freshness_results if r["layer"] == "marts"}

    for table_name in marts_tables:
        if table_name in checked_marts:
            continue

        try:
            cursor.execute(f"""
                SELECT
                    MAX(COALESCE(updated_at, created_at)) as last_update,
                    EXTRACT(EPOCH FROM (NOW() - MAX(COALESCE(updated_at, created_at)))) / 3600 as hours_since,
                    COUNT(*) as row_count
                FROM marts.{table_name}
            """)
            result = cursor.fetchone()
            if result and result[0]:
                last_update, hours, row_count = result
                threshold = LAYER_THRESHOLDS["marts"]
                is_fresh = hours < threshold if hours is not None else False
                freshness_results.append({
                    "source_name": f"marts.{table_name}",
                    "layer": "marts",
                    "last_update": str(last_update),
                    "staleness_hours": round(hours, 2) if hours else None,
                    "is_fresh": is_fresh,
                    "row_count": int(row_count) if row_count else 0,
                    "threshold_hours": threshold,
                })
        except Exception:
            pass

    # ======================
    # STORE FRESHNESS METRICS
    # ======================

    for result in freshness_results:
        cursor.execute("""
            INSERT INTO data_platform.data_freshness_metrics
            (source_name, layer, last_update_time, staleness_hours, is_fresh,
             row_count, expected_freshness_hours, check_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            result["source_name"],
            result["layer"],
            result["last_update"],
            result["staleness_hours"],
            result["is_fresh"],
            result.get("row_count", 0),
            result.get("threshold_hours", 24),
        ))

    conn.commit()
    cursor.close()
    conn.close()

    # ======================
    # BUILD METRICS SUMMARY
    # ======================

    stale_sources = [r for r in freshness_results if not r["is_fresh"]]
    avg_staleness = (
        sum(r["staleness_hours"] for r in freshness_results if r["staleness_hours"] is not None)
        / len([r for r in freshness_results if r["staleness_hours"] is not None])
        if freshness_results else 0
    )

    # Group by layer for detailed reporting
    layer_summary = {}
    for layer in ["raw", "staging", "marts"]:
        layer_results = [r for r in freshness_results if r["layer"] == layer]
        layer_stale = [r for r in layer_results if not r["is_fresh"]]
        layer_summary[layer] = {
            "total": len(layer_results),
            "fresh": len(layer_results) - len(layer_stale),
            "stale": len(layer_stale),
            "threshold_hours": LAYER_THRESHOLDS[layer],
        }

    metrics = {
        "sources_checked": len(freshness_results),
        "fresh_sources": len(freshness_results) - len(stale_sources),
        "stale_sources": len(stale_sources),
        "avg_staleness_hours": round(avg_staleness, 2),
        "stale_source_details": stale_sources[:5],
        "layer_summary": layer_summary,
    }

    if stale_sources:
        # Include which layers have stale data
        stale_layers = list(set(r["layer"] for r in stale_sources))
        metrics["issue"] = f"{len(stale_sources)} stale data source(s) detected in layers: {', '.join(stale_layers)}"

    context["ti"].xcom_push(key="freshness_metrics", value=metrics)
    print(f"Data freshness: {metrics['fresh_sources']}/{metrics['sources_checked']} fresh")
    for layer, summary in layer_summary.items():
        print(f"  {layer}: {summary['fresh']}/{summary['total']} fresh (threshold: {summary['threshold_hours']}h)")
    return metrics


def _monitor_sla_compliance(**context) -> Dict[str, Any]:
    """Monitor SLA compliance across DAGs.

    Returns:
        Dictionary with SLA metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    # Check SLA misses
    cursor.execute("""
        SELECT
            dag_id,
            task_id,
            execution_date,
            timestamp
        FROM sla_miss
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 20
    """)

    sla_misses = cursor.fetchall()

    # Calculate compliance (requires custom tracking)
    # For now, estimate based on task completion times
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE state = 'success') as completed,
            COUNT(*) as total
        FROM task_instance
        WHERE start_date >= NOW() - INTERVAL '24 hours'
    """)

    result = cursor.fetchone()
    completed, total = result or (0, 0)

    cursor.close()
    conn.close()

    compliance_percent = (completed / total * 100) if total > 0 else 100

    metrics = {
        "sla_misses_24h": len(sla_misses),
        "sla_compliance_percent": round(compliance_percent, 2),
        "recent_misses": [
            {"dag_id": m[0], "task_id": m[1], "execution_date": str(m[2])}
            for m in sla_misses[:5]
        ],
    }

    if len(sla_misses) > 0:
        metrics["issue"] = f"{len(sla_misses)} SLA miss(es) in last 24 hours"

    if compliance_percent < 95:
        metrics["compliance_issue"] = f"SLA compliance below threshold: {compliance_percent:.1f}%"

    context["ti"].xcom_push(key="sla_metrics", value=metrics)
    print(f"SLA compliance: {compliance_percent:.1f}%, Misses: {len(sla_misses)}")
    return metrics


def _check_data_volumes(**context) -> Dict[str, Any]:
    """Check data volumes for anomalies.

    Returns:
        Dictionary with volume metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    volume_results = []

    # Check ingestion volumes
    cursor.execute("""
        SELECT
            source_name,
            SUM(row_count) as total_rows,
            AVG(row_count) as avg_rows,
            MAX(row_count) as max_rows,
            MIN(row_count) as min_rows,
            COUNT(*) as ingestion_count
        FROM data_platform.ingestion_metadata
        WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
        AND status = 'success'
        GROUP BY source_name
    """)

    for row in cursor.fetchall():
        source, total, avg, max_rows, min_rows, count = row
        volume_results.append({
            "source_name": source,
            "total_rows_7d": int(total or 0),
            "avg_rows": round(float(avg or 0), 2),
            "max_rows": int(max_rows or 0),
            "min_rows": int(min_rows or 0),
            "ingestion_count": int(count or 0),
        })

    cursor.close()
    conn.close()

    # Detect anomalies (simplified check for zero rows)
    anomalies = [
        v for v in volume_results
        if v["min_rows"] == 0 or (v["avg_rows"] > 0 and v["min_rows"] < v["avg_rows"] * 0.5)
    ]

    metrics = {
        "sources_checked": len(volume_results),
        "volume_details": volume_results[:10],
        "anomalies_detected": len(anomalies),
        "anomaly_details": anomalies[:5],
    }

    if anomalies:
        metrics["issue"] = f"{len(anomalies)} volume anomaly(ies) detected"

    context["ti"].xcom_push(key="volume_metrics", value=metrics)
    print(f"Volume check: {len(volume_results)} sources, {len(anomalies)} anomalies")
    return metrics


def _check_system_resources(**context) -> Dict[str, Any]:
    """Check system resource utilization.

    Returns:
        Dictionary with resource metrics
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    # Get queue status
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE state = 'queued') as queued,
            COUNT(*) FILTER (WHERE state = 'running') as running,
            COUNT(*) FILTER (WHERE state = 'scheduled') as scheduled
        FROM task_instance
        WHERE start_date >= NOW() - INTERVAL '1 hour'
        OR state IN ('queued', 'running', 'scheduled')
    """)

    result = cursor.fetchone()
    queued, running, scheduled = result or (0, 0, 0)

    # Get DAG counts
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_active = true AND is_paused = false) as active,
            COUNT(*) FILTER (WHERE is_paused = true) as paused
        FROM dag
    """)

    dag_result = cursor.fetchone()
    total_dags, active_dags, paused_dags = dag_result or (0, 0, 0)

    cursor.close()
    conn.close()

    metrics = {
        "queued_tasks": queued,
        "running_tasks": running,
        "scheduled_tasks": scheduled,
        "total_dags": total_dags,
        "active_dags": active_dags,
        "paused_dags": paused_dags,
    }

    if queued > 100:
        metrics["issue"] = f"High queue depth: {queued} tasks (threshold: 100)"

    context["ti"].xcom_push(key="resource_metrics", value=metrics)
    print(f"Resources: {running} running, {queued} queued, {total_dags} DAGs")
    return metrics


def _generate_health_report(**context) -> Dict[str, Any]:
    """Generate comprehensive health report and store metrics.

    Returns:
        Dictionary with overall health report
    """
    ti = context["ti"]

    # Collect all metrics
    scheduler = ti.xcom_pull(task_ids="check_scheduler_health", key="scheduler_metrics") or {}
    parsing = ti.xcom_pull(task_ids="check_dag_parsing_errors", key="parsing_errors") or {}
    tasks = ti.xcom_pull(task_ids="monitor_task_failures", key="task_metrics") or {}
    freshness = ti.xcom_pull(task_ids="check_data_freshness", key="freshness_metrics") or {}
    sla = ti.xcom_pull(task_ids="monitor_sla_compliance", key="sla_metrics") or {}
    volumes = ti.xcom_pull(task_ids="check_data_volumes", key="volume_metrics") or {}
    resources = ti.xcom_pull(task_ids="check_system_resources", key="resource_metrics") or {}

    # Collect issues
    issues = []
    for metrics in [scheduler, parsing, tasks, freshness, sla, volumes, resources]:
        if "issue" in metrics:
            issues.append(metrics["issue"])
        if "queue_issue" in metrics:
            issues.append(metrics["queue_issue"])
        if "compliance_issue" in metrics:
            issues.append(metrics["compliance_issue"])

    # Calculate health score
    conn = _get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT data_platform.calculate_health_score(%s, %s, %s, %s, %s)
    """, (
        scheduler.get("scheduler_healthy", True),
        tasks.get("failed_tasks_24h", 0),
        sla.get("sla_compliance_percent", 100),
        freshness.get("stale_sources", 0),
        resources.get("queued_tasks", 0),
    ))

    health_score = cursor.fetchone()[0]

    # Store health metrics
    cursor.execute("""
        INSERT INTO data_platform.platform_health_metrics (
            metric_date, metric_time,
            scheduler_healthy, scheduler_heartbeat_age_seconds,
            total_dags, active_dags, paused_dags,
            failed_tasks_24h, successful_tasks_24h,
            running_tasks, queued_tasks,
            sla_compliance_percent, sla_misses_24h,
            avg_data_freshness_hours, stale_sources_count,
            health_score, issues
        ) VALUES (
            CURRENT_DATE, NOW(),
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        scheduler.get("scheduler_healthy", True),
        scheduler.get("heartbeat_age_seconds"),
        resources.get("total_dags", 0),
        resources.get("active_dags", 0),
        resources.get("paused_dags", 0),
        tasks.get("failed_tasks_24h", 0),
        tasks.get("successful_tasks_24h", 0),
        resources.get("running_tasks", 0),
        resources.get("queued_tasks", 0),
        sla.get("sla_compliance_percent", 100),
        sla.get("sla_misses_24h", 0),
        freshness.get("avg_staleness_hours", 0),
        freshness.get("stale_sources", 0),
        health_score,
        json.dumps(issues),
    ))

    conn.commit()
    cursor.close()
    conn.close()

    # Build report
    report = {
        "health_score": health_score,
        "check_time": datetime.utcnow().isoformat(),
        "scheduler_healthy": scheduler.get("scheduler_healthy", True),
        "failed_tasks_24h": tasks.get("failed_tasks_24h", 0),
        "sla_compliance_percent": sla.get("sla_compliance_percent", 100),
        "avg_data_freshness_hours": freshness.get("avg_staleness_hours", 0),
        "stale_sources": freshness.get("stale_sources", 0),
        "queued_tasks": resources.get("queued_tasks", 0),
        "total_dags": resources.get("total_dags", 0),
        "issues": issues,
    }

    # Print summary
    print("=" * 60)
    print(" PLATFORM HEALTH REPORT")
    print("=" * 60)
    print(f" Health Score:          {health_score}%")
    print(f" Scheduler Healthy:     {scheduler.get('scheduler_healthy', 'N/A')}")
    print(f" Failed Tasks (24h):    {tasks.get('failed_tasks_24h', 'N/A')}")
    print(f" SLA Compliance:        {sla.get('sla_compliance_percent', 'N/A')}%")
    print(f" Avg Data Freshness:    {freshness.get('avg_staleness_hours', 'N/A')}h")
    print(f" Stale Sources:         {freshness.get('stale_sources', 'N/A')}")
    print(f" Queued Tasks:          {resources.get('queued_tasks', 'N/A')}")
    print(f" Active DAGs:           {resources.get('active_dags', 'N/A')}")
    if issues:
        print("-" * 60)
        print(" ISSUES DETECTED:")
        for issue in issues:
            print(f"   - {issue}")
    print("=" * 60)

    # Send alert if health score is low
    if health_score < 70 or issues:
        try:
            from utils.alerting import (
                format_platform_health_alert,
                send_alert,
                AlertSeverity,
            )

            alert_data = format_platform_health_alert(
                check_name="Platform Health",
                status="unhealthy" if health_score < 70 else "degraded",
                details="; ".join(issues) if issues else "Health score below threshold",
                severity=AlertSeverity.CRITICAL if health_score < 50 else AlertSeverity.HIGH,
            )
            send_alert(alert_data)
        except Exception as e:
            print(f"Warning: Failed to send health alert: {e}")

    context["ti"].xcom_push(key="health_report", value=report)
    return report


with DAG(
    dag_id="platform_health",
    default_args=default_args,
    description="Platform health monitoring: scheduler, tasks, data freshness, SLA",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "ops", "platform-health"],
    on_failure_callback=_on_failure_callback,
    max_active_runs=1,
) as dag:

    check_scheduler_health = PythonOperator(
        task_id="check_scheduler_health",
        python_callable=_check_scheduler_health,
    )

    check_dag_parsing_errors = PythonOperator(
        task_id="check_dag_parsing_errors",
        python_callable=_check_dag_parsing_errors,
    )

    monitor_task_failures = PythonOperator(
        task_id="monitor_task_failures",
        python_callable=_monitor_task_failures,
    )

    check_data_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=_check_data_freshness,
    )

    monitor_sla_compliance = PythonOperator(
        task_id="monitor_sla_compliance",
        python_callable=_monitor_sla_compliance,
    )

    check_data_volumes = PythonOperator(
        task_id="check_data_volumes",
        python_callable=_check_data_volumes,
    )

    check_system_resources = PythonOperator(
        task_id="check_system_resources",
        python_callable=_check_system_resources,
    )

    generate_health_report = PythonOperator(
        task_id="generate_health_report",
        python_callable=_generate_health_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task dependencies: parallel health checks, then aggregate
    [
        check_scheduler_health,
        check_dag_parsing_errors,
        monitor_task_failures,
        check_data_freshness,
        monitor_sla_compliance,
        check_data_volumes,
        check_system_resources,
    ] >> generate_health_report
