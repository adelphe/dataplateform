"""Monitoring Operators for Data Platform.

Custom Airflow operators for data freshness, SLA, and volume monitoring.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataFreshnessCheckOperator(BaseOperator):
    """Operator to check data freshness based on configurable SLA.

    This operator queries metadata tables to verify that data sources
    are being updated within expected timeframes.

    Attributes:
        source_name: Name of the data source to check
        max_staleness_hours: Maximum acceptable staleness in hours
        query_template: Optional custom SQL query template
        postgres_conn_id: Airflow connection ID for PostgreSQL
        alert_on_stale: Whether to trigger alerts when data is stale
    """

    template_fields = ["source_name", "query_template"]
    ui_color = "#4CAF50"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        source_name: str,
        max_staleness_hours: int = 24,
        query_template: Optional[str] = None,
        postgres_conn_id: str = "postgres_default",
        alert_on_stale: bool = True,
        layer: str = "raw",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_name = source_name
        self.max_staleness_hours = max_staleness_hours
        self.query_template = query_template
        self.postgres_conn_id = postgres_conn_id
        self.alert_on_stale = alert_on_stale
        self.layer = layer

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the freshness check.

        Args:
            context: Airflow task context

        Returns:
            Dictionary with freshness check results

        Raises:
            AirflowException: If data is stale and alert_on_stale is True
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Use custom query or default
        if self.query_template:
            query = self.query_template
        else:
            query = """
                SELECT
                    MAX(updated_at) as last_update,
                    EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) / 3600 as hours_since_update
                FROM data_platform.ingestion_metadata
                WHERE source_name = %s
                AND status = 'success'
            """

        cursor.execute(query, (self.source_name,))
        result = cursor.fetchone()

        if result and result[0]:
            last_update, hours_since = result
            is_fresh = hours_since <= self.max_staleness_hours
        else:
            last_update = None
            hours_since = None
            is_fresh = False

        # Store freshness metric
        cursor.execute("""
            INSERT INTO data_platform.data_freshness_metrics
            (source_name, layer, last_update_time, staleness_hours, is_fresh,
             expected_freshness_hours, check_time)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            self.source_name,
            self.layer,
            last_update,
            hours_since,
            is_fresh,
            self.max_staleness_hours,
        ))

        conn.commit()
        cursor.close()
        conn.close()

        result = {
            "source_name": self.source_name,
            "layer": self.layer,
            "last_update": str(last_update) if last_update else None,
            "staleness_hours": round(hours_since, 2) if hours_since else None,
            "is_fresh": is_fresh,
            "threshold_hours": self.max_staleness_hours,
        }

        self.log.info(f"Freshness check for {self.source_name}: {'FRESH' if is_fresh else 'STALE'}")

        if not is_fresh and self.alert_on_stale:
            self._send_alert(result)
            from airflow.exceptions import AirflowException
            raise AirflowException(
                f"Data source {self.source_name} is stale: "
                f"{hours_since:.1f}h (threshold: {self.max_staleness_hours}h)"
            )

        return result

    def _send_alert(self, result: Dict[str, Any]) -> None:
        """Send alert for stale data."""
        try:
            from utils.alerting import send_slack_alert, AlertSeverity

            message = (
                f"Data freshness alert: `{result['source_name']}` is stale\n"
                f"Last update: {result['last_update']}\n"
                f"Staleness: {result['staleness_hours']}h (threshold: {result['threshold_hours']}h)"
            )
            send_slack_alert(
                message=message,
                severity=AlertSeverity.HIGH,
                title=f"Data Freshness Alert: {result['source_name']}",
            )
        except Exception as e:
            self.log.warning(f"Failed to send freshness alert: {e}")


class SLACheckOperator(BaseOperator):
    """Operator to validate SLA compliance for specific DAG/task.

    This operator checks whether a DAG or task completed within
    its expected SLA timeframe.

    Attributes:
        target_dag_id: DAG ID to check SLA for
        target_task_id: Optional task ID (if None, checks DAG-level SLA)
        expected_duration_minutes: Expected maximum duration in minutes
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """

    template_fields = ["target_dag_id", "target_task_id"]
    ui_color = "#FF9800"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        target_dag_id: str,
        target_task_id: Optional[str] = None,
        expected_duration_minutes: int = 60,
        postgres_conn_id: str = "postgres_default",
        check_window_hours: int = 24,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.target_dag_id = target_dag_id
        self.target_task_id = target_task_id
        self.expected_duration_minutes = expected_duration_minutes
        self.postgres_conn_id = postgres_conn_id
        self.check_window_hours = check_window_hours

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the SLA check.

        Args:
            context: Airflow task context

        Returns:
            Dictionary with SLA compliance results
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        if self.target_task_id:
            # Task-level SLA check
            cursor.execute("""
                SELECT
                    execution_date,
                    start_date,
                    end_date,
                    EXTRACT(EPOCH FROM (end_date - start_date)) / 60 as duration_minutes,
                    state
                FROM task_instance
                WHERE dag_id = %s
                AND task_id = %s
                AND start_date >= NOW() - INTERVAL '%s hours'
                AND state IN ('success', 'failed')
                ORDER BY execution_date DESC
                LIMIT 10
            """, (self.target_dag_id, self.target_task_id, self.check_window_hours))
        else:
            # DAG-level SLA check
            cursor.execute("""
                SELECT
                    execution_date,
                    start_date,
                    end_date,
                    EXTRACT(EPOCH FROM (end_date - start_date)) / 60 as duration_minutes,
                    state
                FROM dag_run
                WHERE dag_id = %s
                AND start_date >= NOW() - INTERVAL '%s hours'
                AND state IN ('success', 'failed')
                ORDER BY execution_date DESC
                LIMIT 10
            """, (self.target_dag_id, self.check_window_hours))

        runs = cursor.fetchall()

        # Calculate SLA compliance
        sla_met_count = 0
        sla_missed_count = 0
        run_details = []

        for run in runs:
            exec_date, start, end, duration, state = run
            if duration is not None:
                sla_met = duration <= self.expected_duration_minutes
                if sla_met:
                    sla_met_count += 1
                else:
                    sla_missed_count += 1

                run_details.append({
                    "execution_date": str(exec_date),
                    "duration_minutes": round(duration, 2),
                    "sla_met": sla_met,
                    "state": state,
                })

                # Record SLA tracking
                cursor.execute("""
                    INSERT INTO data_platform.sla_tracking
                    (dag_id, task_id, execution_date, expected_completion, actual_completion,
                     sla_met, delay_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dag_id, task_id, execution_date) DO UPDATE
                    SET actual_completion = EXCLUDED.actual_completion,
                        sla_met = EXCLUDED.sla_met,
                        delay_seconds = EXCLUDED.delay_seconds
                """, (
                    self.target_dag_id,
                    self.target_task_id or "__dag__",
                    exec_date,
                    start + timedelta(minutes=self.expected_duration_minutes),
                    end,
                    sla_met,
                    int((duration - self.expected_duration_minutes) * 60) if not sla_met else 0,
                ))

        conn.commit()
        cursor.close()
        conn.close()

        total_runs = sla_met_count + sla_missed_count
        compliance_percent = (sla_met_count / total_runs * 100) if total_runs > 0 else 100

        result = {
            "dag_id": self.target_dag_id,
            "task_id": self.target_task_id,
            "expected_duration_minutes": self.expected_duration_minutes,
            "runs_checked": total_runs,
            "sla_met": sla_met_count,
            "sla_missed": sla_missed_count,
            "compliance_percent": round(compliance_percent, 2),
            "recent_runs": run_details[:5],
        }

        self.log.info(
            f"SLA check for {self.target_dag_id}: "
            f"{compliance_percent:.1f}% compliance ({sla_met_count}/{total_runs})"
        )

        return result


class VolumeAnomalyCheckOperator(BaseOperator):
    """Operator to detect data volume anomalies.

    This operator compares current data volumes against historical
    averages to detect unexpected changes.

    Attributes:
        table_name: Name of the table to check
        schema_name: Schema containing the table
        deviation_threshold_percent: Threshold for anomaly detection
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """

    template_fields = ["table_name", "schema_name"]
    ui_color = "#9C27B0"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        table_name: str,
        schema_name: str = "public",
        deviation_threshold_percent: float = 50.0,
        postgres_conn_id: str = "postgres_default",
        historical_window_days: int = 7,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.schema_name = schema_name
        self.deviation_threshold_percent = deviation_threshold_percent
        self.postgres_conn_id = postgres_conn_id
        self.historical_window_days = historical_window_days

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the volume anomaly check.

        Args:
            context: Airflow task context

        Returns:
            Dictionary with volume check results
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Get current row count
        cursor.execute(f"""
            SELECT COUNT(*) FROM {self.schema_name}.{self.table_name}
        """)
        current_count = cursor.fetchone()[0]

        # Get historical data from ingestion metadata
        cursor.execute("""
            SELECT
                AVG(row_count) as avg_rows,
                STDDEV(row_count) as stddev_rows,
                MIN(row_count) as min_rows,
                MAX(row_count) as max_rows,
                COUNT(*) as sample_count
            FROM data_platform.ingestion_metadata
            WHERE source_name = %s
            AND execution_date >= CURRENT_DATE - INTERVAL '%s days'
            AND status = 'success'
        """, (f"{self.schema_name}.{self.table_name}", self.historical_window_days))

        hist = cursor.fetchone()
        avg_rows, stddev, min_rows, max_rows, samples = hist or (None, None, None, None, 0)

        cursor.close()
        conn.close()

        # Calculate deviation
        is_anomaly = False
        deviation_percent = None

        if avg_rows and avg_rows > 0:
            deviation_percent = abs(current_count - avg_rows) / avg_rows * 100
            is_anomaly = deviation_percent > self.deviation_threshold_percent

        # Check for zero rows (always an anomaly if historically had data)
        if current_count == 0 and avg_rows and avg_rows > 0:
            is_anomaly = True

        result = {
            "table": f"{self.schema_name}.{self.table_name}",
            "current_count": current_count,
            "historical_avg": round(avg_rows, 2) if avg_rows else None,
            "historical_stddev": round(stddev, 2) if stddev else None,
            "historical_min": min_rows,
            "historical_max": max_rows,
            "sample_count": samples,
            "deviation_percent": round(deviation_percent, 2) if deviation_percent else None,
            "threshold_percent": self.deviation_threshold_percent,
            "is_anomaly": is_anomaly,
        }

        deviation_str = f"{deviation_percent:.1f}%" if deviation_percent is not None else "N/A"
        self.log.info(
            f"Volume check for {self.schema_name}.{self.table_name}: "
            f"{current_count} rows, deviation: {deviation_str} "
            f"({'ANOMALY' if is_anomaly else 'NORMAL'})"
        )

        if is_anomaly:
            self._send_alert(result)

        return result

    def _send_alert(self, result: Dict[str, Any]) -> None:
        """Send alert for volume anomaly."""
        try:
            from utils.alerting import send_slack_alert, AlertSeverity

            message = (
                f"Volume anomaly detected for `{result['table']}`\n"
                f"Current: {result['current_count']} rows\n"
                f"Historical avg: {result['historical_avg']} rows\n"
                f"Deviation: {result['deviation_percent']}%"
            )
            send_slack_alert(
                message=message,
                severity=AlertSeverity.MEDIUM,
                title=f"Volume Anomaly: {result['table']}",
            )
        except Exception as e:
            self.log.warning(f"Failed to send volume alert: {e}")


class HealthScoreOperator(BaseOperator):
    """Operator to calculate and store platform health score.

    Aggregates various health metrics into a single score.
    """

    ui_color = "#2196F3"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate and store health score.

        Args:
            context: Airflow task context

        Returns:
            Dictionary with health score and components
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Get latest metrics
        cursor.execute("""
            SELECT
                scheduler_healthy,
                failed_tasks_24h,
                sla_compliance_percent,
                stale_sources_count,
                queued_tasks,
                health_score
            FROM data_platform.platform_health_metrics
            ORDER BY metric_time DESC
            LIMIT 1
        """)

        result = cursor.fetchone()

        if result:
            scheduler, failed, sla, stale, queued, score = result
            health_data = {
                "health_score": score,
                "scheduler_healthy": scheduler,
                "failed_tasks_24h": failed,
                "sla_compliance_percent": float(sla),
                "stale_sources": stale,
                "queued_tasks": queued,
            }
        else:
            health_data = {
                "health_score": 100,
                "message": "No health metrics available",
            }

        cursor.close()
        conn.close()

        self.log.info(f"Platform health score: {health_data.get('health_score', 'N/A')}")

        return health_data
