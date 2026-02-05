"""Unit tests for monitoring utilities and operators.

These tests verify monitoring functionality including data freshness
calculation, SLA compliance, volume anomaly detection, and health scoring.
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, "/opt/airflow/pipelines")


class TestDataFreshnessLogic(unittest.TestCase):
    """Tests for data freshness calculation logic."""

    def test_freshness_within_threshold(self):
        """Test data within freshness threshold."""
        last_update = datetime.utcnow() - timedelta(hours=6)
        max_staleness_hours = 24

        hours_since_update = (datetime.utcnow() - last_update).total_seconds() / 3600
        is_fresh = hours_since_update <= max_staleness_hours

        self.assertTrue(is_fresh)
        self.assertLess(hours_since_update, 7)

    def test_freshness_exceeds_threshold(self):
        """Test stale data detection."""
        last_update = datetime.utcnow() - timedelta(hours=30)
        max_staleness_hours = 24

        hours_since_update = (datetime.utcnow() - last_update).total_seconds() / 3600
        is_fresh = hours_since_update <= max_staleness_hours

        self.assertFalse(is_fresh)
        self.assertGreater(hours_since_update, 24)

    def test_freshness_color_coding(self):
        """Test freshness color coding logic."""
        def get_freshness_color(staleness_hours):
            if staleness_hours < 6:
                return "green"
            elif staleness_hours < 24:
                return "yellow"
            else:
                return "red"

        self.assertEqual(get_freshness_color(3), "green")
        self.assertEqual(get_freshness_color(12), "yellow")
        self.assertEqual(get_freshness_color(48), "red")


class TestSLAComplianceLogic(unittest.TestCase):
    """Tests for SLA compliance calculation logic."""

    def test_sla_met(self):
        """Test SLA compliance when task completes on time."""
        expected_duration_minutes = 60
        actual_duration_minutes = 45

        sla_met = actual_duration_minutes <= expected_duration_minutes
        self.assertTrue(sla_met)

    def test_sla_missed(self):
        """Test SLA miss detection."""
        expected_duration_minutes = 60
        actual_duration_minutes = 90

        sla_met = actual_duration_minutes <= expected_duration_minutes
        delay_minutes = actual_duration_minutes - expected_duration_minutes

        self.assertFalse(sla_met)
        self.assertEqual(delay_minutes, 30)

    def test_sla_compliance_percentage(self):
        """Test SLA compliance percentage calculation."""
        runs = [
            {"duration": 45, "expected": 60},  # Met
            {"duration": 55, "expected": 60},  # Met
            {"duration": 75, "expected": 60},  # Missed
            {"duration": 30, "expected": 60},  # Met
            {"duration": 90, "expected": 60},  # Missed
        ]

        met_count = sum(1 for r in runs if r["duration"] <= r["expected"])
        total = len(runs)
        compliance_percent = (met_count / total) * 100

        self.assertEqual(met_count, 3)
        self.assertEqual(compliance_percent, 60.0)


class TestVolumeAnomalyLogic(unittest.TestCase):
    """Tests for volume anomaly detection logic."""

    def test_no_anomaly_within_threshold(self):
        """Test normal volume within threshold."""
        current_count = 1050
        historical_avg = 1000
        threshold_percent = 50

        deviation_percent = abs(current_count - historical_avg) / historical_avg * 100
        is_anomaly = deviation_percent > threshold_percent

        self.assertFalse(is_anomaly)
        self.assertEqual(deviation_percent, 5.0)

    def test_anomaly_exceeds_threshold(self):
        """Test volume anomaly detection."""
        current_count = 200
        historical_avg = 1000
        threshold_percent = 50

        deviation_percent = abs(current_count - historical_avg) / historical_avg * 100
        is_anomaly = deviation_percent > threshold_percent

        self.assertTrue(is_anomaly)
        self.assertEqual(deviation_percent, 80.0)

    def test_zero_row_anomaly(self):
        """Test zero rows always flagged as anomaly."""
        current_count = 0
        historical_avg = 1000

        # Zero rows with historical data is always an anomaly
        is_anomaly = current_count == 0 and historical_avg > 0

        self.assertTrue(is_anomaly)


class TestHealthScoreLogic(unittest.TestCase):
    """Tests for health score calculation logic."""

    def calculate_health_score(
        self,
        scheduler_healthy,
        failed_tasks_24h,
        sla_compliance,
        stale_sources,
        queued_tasks,
    ):
        """Calculate health score based on metrics."""
        score = 100

        # Scheduler health (20 points)
        if not scheduler_healthy:
            score -= 20

        # Task failures (25 points)
        if failed_tasks_24h > 10:
            score -= 25
        elif failed_tasks_24h > 5:
            score -= 15
        elif failed_tasks_24h > 0:
            score -= 5

        # SLA compliance (25 points)
        if sla_compliance < 80:
            score -= 25
        elif sla_compliance < 90:
            score -= 15
        elif sla_compliance < 95:
            score -= 5

        # Data freshness (20 points)
        if stale_sources > 5:
            score -= 20
        elif stale_sources > 2:
            score -= 10
        elif stale_sources > 0:
            score -= 5

        # Queue health (10 points)
        if queued_tasks > 100:
            score -= 10
        elif queued_tasks > 50:
            score -= 5

        return max(score, 0)

    def test_perfect_health_score(self):
        """Test perfect health score calculation."""
        score = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=10,
        )
        self.assertEqual(score, 100)

    def test_degraded_health_score(self):
        """Test degraded health score."""
        score = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=3,
            sla_compliance=92,
            stale_sources=1,
            queued_tasks=30,
        )
        # -5 (failures) -5 (sla) -5 (freshness) = 85
        self.assertEqual(score, 85)

    def test_critical_health_score(self):
        """Test critical health conditions."""
        score = self.calculate_health_score(
            scheduler_healthy=False,
            failed_tasks_24h=15,
            sla_compliance=70,
            stale_sources=10,
            queued_tasks=150,
        )
        # -20 -25 -25 -20 -10 = 0
        self.assertEqual(score, 0)

    def test_health_status_from_score(self):
        """Test health status determination from score."""
        def get_health_status(score):
            if score >= 90:
                return "HEALTHY"
            elif score >= 70:
                return "DEGRADED"
            else:
                return "UNHEALTHY"

        self.assertEqual(get_health_status(95), "HEALTHY")
        self.assertEqual(get_health_status(85), "DEGRADED")
        self.assertEqual(get_health_status(50), "UNHEALTHY")


class TestMonitoringOperators(unittest.TestCase):
    """Tests for monitoring operator classes."""

    @patch("operators.monitoring_operator.PostgresHook")
    def test_data_freshness_operator_fresh(self, mock_hook):
        """Test DataFreshnessCheckOperator with fresh data."""
        from operators.monitoring_operator import DataFreshnessCheckOperator

        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime.utcnow() - timedelta(hours=6), 6.0)
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="test_source",
            max_staleness_hours=24,
            alert_on_stale=False,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertTrue(result["is_fresh"])
        self.assertEqual(result["source_name"], "test_source")

    @patch("operators.monitoring_operator.PostgresHook")
    def test_data_freshness_operator_stale(self, mock_hook):
        """Test DataFreshnessCheckOperator with stale data (alert disabled)."""
        from operators.monitoring_operator import DataFreshnessCheckOperator

        # Setup mock with stale data (30 hours old)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime.utcnow() - timedelta(hours=30), 30.0)
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness_stale",
            source_name="stale_source",
            max_staleness_hours=24,
            alert_on_stale=False,  # Disable alert to avoid exception
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertFalse(result["is_fresh"])
        self.assertEqual(result["source_name"], "stale_source")
        self.assertGreater(result["staleness_hours"], 24)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_data_freshness_operator_no_data(self, mock_hook):
        """Test DataFreshnessCheckOperator when no data exists."""
        from operators.monitoring_operator import DataFreshnessCheckOperator

        # Setup mock with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None, None)
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness_nodata",
            source_name="missing_source",
            max_staleness_hours=24,
            alert_on_stale=False,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertFalse(result["is_fresh"])
        self.assertIsNone(result["last_update"])
        self.assertIsNone(result["staleness_hours"])

    @patch("operators.monitoring_operator.PostgresHook")
    def test_data_freshness_operator_with_layer(self, mock_hook):
        """Test DataFreshnessCheckOperator stores correct layer."""
        from operators.monitoring_operator import DataFreshnessCheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime.utcnow() - timedelta(hours=2), 2.0)
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness_staging",
            source_name="staging_table",
            max_staleness_hours=12,
            layer="staging",
            alert_on_stale=False,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertTrue(result["is_fresh"])
        self.assertEqual(result["layer"], "staging")
        self.assertEqual(result["threshold_hours"], 12)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_sla_check_operator(self, mock_hook):
        """Test SLACheckOperator calculation."""
        from operators.monitoring_operator import SLACheckOperator

        # Setup mock with sample runs
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (datetime(2024, 1, 15), datetime(2024, 1, 15, 10), datetime(2024, 1, 15, 10, 45), 45, "success"),
            (datetime(2024, 1, 14), datetime(2024, 1, 14, 10), datetime(2024, 1, 14, 11, 30), 90, "success"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = SLACheckOperator(
            task_id="test_sla",
            target_dag_id="test_dag",
            expected_duration_minutes=60,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["dag_id"], "test_dag")
        self.assertEqual(result["expected_duration_minutes"], 60)
        self.assertIn("compliance_percent", result)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_sla_check_operator_all_met(self, mock_hook):
        """Test SLACheckOperator when all SLAs are met."""
        from operators.monitoring_operator import SLACheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (datetime(2024, 1, 15), datetime(2024, 1, 15, 10), datetime(2024, 1, 15, 10, 30), 30, "success"),
            (datetime(2024, 1, 14), datetime(2024, 1, 14, 10), datetime(2024, 1, 14, 10, 45), 45, "success"),
            (datetime(2024, 1, 13), datetime(2024, 1, 13, 10), datetime(2024, 1, 13, 10, 55), 55, "success"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = SLACheckOperator(
            task_id="test_sla_all_met",
            target_dag_id="test_dag",
            expected_duration_minutes=60,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["compliance_percent"], 100.0)
        self.assertEqual(result["sla_met"], 3)
        self.assertEqual(result["sla_missed"], 0)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_sla_check_operator_all_missed(self, mock_hook):
        """Test SLACheckOperator when all SLAs are missed."""
        from operators.monitoring_operator import SLACheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (datetime(2024, 1, 15), datetime(2024, 1, 15, 10), datetime(2024, 1, 15, 12, 0), 120, "success"),
            (datetime(2024, 1, 14), datetime(2024, 1, 14, 10), datetime(2024, 1, 14, 11, 30), 90, "success"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = SLACheckOperator(
            task_id="test_sla_all_missed",
            target_dag_id="test_dag",
            expected_duration_minutes=60,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["compliance_percent"], 0.0)
        self.assertEqual(result["sla_met"], 0)
        self.assertEqual(result["sla_missed"], 2)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_sla_check_operator_no_runs(self, mock_hook):
        """Test SLACheckOperator when no runs exist."""
        from operators.monitoring_operator import SLACheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = SLACheckOperator(
            task_id="test_sla_no_runs",
            target_dag_id="test_dag",
            expected_duration_minutes=60,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["runs_checked"], 0)
        self.assertEqual(result["compliance_percent"], 100.0)  # No runs = 100% compliance

    @patch("operators.monitoring_operator.PostgresHook")
    def test_sla_check_operator_with_task_id(self, mock_hook):
        """Test SLACheckOperator with specific task_id."""
        from operators.monitoring_operator import SLACheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (datetime(2024, 1, 15), datetime(2024, 1, 15, 10), datetime(2024, 1, 15, 10, 40), 40, "success"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = SLACheckOperator(
            task_id="test_sla_with_task",
            target_dag_id="test_dag",
            target_task_id="specific_task",
            expected_duration_minutes=60,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["task_id"], "specific_task")
        self.assertEqual(result["compliance_percent"], 100.0)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_volume_anomaly_operator_normal(self, mock_hook):
        """Test VolumeAnomalyCheckOperator with normal volume."""
        from operators.monitoring_operator import VolumeAnomalyCheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # First call: current count, Second call: historical stats
        mock_cursor.fetchone.side_effect = [
            (1050,),  # Current count
            (1000, 100, 800, 1200, 10),  # avg, stddev, min, max, samples
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume_normal",
            table_name="test_table",
            schema_name="public",
            deviation_threshold_percent=50.0,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertFalse(result["is_anomaly"])
        self.assertEqual(result["current_count"], 1050)
        self.assertLess(result["deviation_percent"], 50.0)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_volume_anomaly_operator_high_deviation(self, mock_hook):
        """Test VolumeAnomalyCheckOperator with high deviation."""
        from operators.monitoring_operator import VolumeAnomalyCheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.fetchone.side_effect = [
            (200,),  # Current count - much lower
            (1000, 100, 800, 1200, 10),  # Historical stats
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume_anomaly",
            table_name="test_table",
            schema_name="public",
            deviation_threshold_percent=50.0,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertTrue(result["is_anomaly"])
        self.assertEqual(result["current_count"], 200)
        self.assertGreater(result["deviation_percent"], 50.0)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_volume_anomaly_operator_zero_rows(self, mock_hook):
        """Test VolumeAnomalyCheckOperator detects zero rows as anomaly."""
        from operators.monitoring_operator import VolumeAnomalyCheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.fetchone.side_effect = [
            (0,),  # Zero current count
            (1000, 100, 800, 1200, 10),  # Historical had data
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume_zero",
            table_name="test_table",
            schema_name="public",
            deviation_threshold_percent=50.0,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertTrue(result["is_anomaly"])
        self.assertEqual(result["current_count"], 0)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_volume_anomaly_operator_no_history(self, mock_hook):
        """Test VolumeAnomalyCheckOperator with no historical data."""
        from operators.monitoring_operator import VolumeAnomalyCheckOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.fetchone.side_effect = [
            (500,),  # Current count
            (None, None, None, None, 0),  # No historical data
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume_no_history",
            table_name="new_table",
            schema_name="public",
            deviation_threshold_percent=50.0,
        )

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertFalse(result["is_anomaly"])
        self.assertEqual(result["current_count"], 500)
        self.assertIsNone(result["deviation_percent"])

    @patch("operators.monitoring_operator.PostgresHook")
    def test_health_score_operator(self, mock_hook):
        """Test HealthScoreOperator retrieves health metrics."""
        from operators.monitoring_operator import HealthScoreOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True, 5, 95.5, 2, 25, 85)
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = HealthScoreOperator(task_id="test_health_score")

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["health_score"], 85)
        self.assertTrue(result["scheduler_healthy"])
        self.assertEqual(result["failed_tasks_24h"], 5)
        self.assertEqual(result["sla_compliance_percent"], 95.5)

    @patch("operators.monitoring_operator.PostgresHook")
    def test_health_score_operator_no_metrics(self, mock_hook):
        """Test HealthScoreOperator when no metrics exist."""
        from operators.monitoring_operator import HealthScoreOperator

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.return_value.get_conn.return_value = mock_conn

        operator = HealthScoreOperator(task_id="test_health_no_metrics")

        context = {"ti": MagicMock()}
        result = operator.execute(context)

        self.assertEqual(result["health_score"], 100)
        self.assertIn("message", result)


class TestHealthScoreEdgeCases(unittest.TestCase):
    """Tests for health score edge cases and boundary conditions."""

    def calculate_health_score(
        self,
        scheduler_healthy,
        failed_tasks_24h,
        sla_compliance,
        stale_sources,
        queued_tasks,
    ):
        """Calculate health score based on metrics."""
        score = 100

        if not scheduler_healthy:
            score -= 20

        if failed_tasks_24h > 10:
            score -= 25
        elif failed_tasks_24h > 5:
            score -= 15
        elif failed_tasks_24h > 0:
            score -= 5

        if sla_compliance < 80:
            score -= 25
        elif sla_compliance < 90:
            score -= 15
        elif sla_compliance < 95:
            score -= 5

        if stale_sources > 5:
            score -= 20
        elif stale_sources > 2:
            score -= 10
        elif stale_sources > 0:
            score -= 5

        if queued_tasks > 100:
            score -= 10
        elif queued_tasks > 50:
            score -= 5

        return max(score, 0)

    def test_scheduler_down_only(self):
        """Test score with only scheduler down."""
        score = self.calculate_health_score(
            scheduler_healthy=False,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=0,
        )
        self.assertEqual(score, 80)

    def test_boundary_failed_tasks_5(self):
        """Test boundary condition at 5 failed tasks."""
        score_at_5 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=5,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=0,
        )
        score_at_6 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=6,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=0,
        )
        self.assertEqual(score_at_5, 95)  # -5 for 1-5 failures
        self.assertEqual(score_at_6, 85)  # -15 for 6-10 failures

    def test_boundary_failed_tasks_10(self):
        """Test boundary condition at 10 failed tasks."""
        score_at_10 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=10,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=0,
        )
        score_at_11 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=11,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=0,
        )
        self.assertEqual(score_at_10, 85)  # -15 for 6-10 failures
        self.assertEqual(score_at_11, 75)  # -25 for >10 failures

    def test_boundary_sla_compliance(self):
        """Test SLA compliance boundary conditions."""
        score_at_95 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=95,
            stale_sources=0,
            queued_tasks=0,
        )
        score_at_94 = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=94,
            stale_sources=0,
            queued_tasks=0,
        )
        self.assertEqual(score_at_95, 100)
        self.assertEqual(score_at_94, 95)  # -5 for 90-94%

    def test_boundary_stale_sources(self):
        """Test stale sources boundary conditions."""
        score_2_stale = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=2,
            queued_tasks=0,
        )
        score_3_stale = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=3,
            queued_tasks=0,
        )
        self.assertEqual(score_2_stale, 95)  # -5 for 1-2 stale
        self.assertEqual(score_3_stale, 90)  # -10 for 3-5 stale

    def test_boundary_queued_tasks(self):
        """Test queued tasks boundary conditions."""
        score_50_queued = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=50,
        )
        score_51_queued = self.calculate_health_score(
            scheduler_healthy=True,
            failed_tasks_24h=0,
            sla_compliance=100,
            stale_sources=0,
            queued_tasks=51,
        )
        self.assertEqual(score_50_queued, 100)
        self.assertEqual(score_51_queued, 95)  # -5 for 51-100 queued

    def test_score_never_negative(self):
        """Test that score never goes below 0."""
        score = self.calculate_health_score(
            scheduler_healthy=False,  # -20
            failed_tasks_24h=100,     # -25
            sla_compliance=50,        # -25
            stale_sources=100,        # -20
            queued_tasks=1000,        # -10 = -100 total
        )
        self.assertEqual(score, 0)


class TestLayerFreshnessThresholds(unittest.TestCase):
    """Tests for layer-specific freshness thresholds."""

    def test_raw_layer_threshold(self):
        """Test raw layer uses 24-hour threshold."""
        LAYER_THRESHOLDS = {"raw": 24, "staging": 12, "marts": 6}

        staleness_hours = 20
        threshold = LAYER_THRESHOLDS["raw"]
        is_fresh = staleness_hours < threshold

        self.assertTrue(is_fresh)

    def test_staging_layer_threshold(self):
        """Test staging layer uses 12-hour threshold."""
        LAYER_THRESHOLDS = {"raw": 24, "staging": 12, "marts": 6}

        staleness_hours = 10
        threshold = LAYER_THRESHOLDS["staging"]
        is_fresh = staleness_hours < threshold

        self.assertTrue(is_fresh)

        # Test just over threshold
        staleness_hours = 13
        is_fresh = staleness_hours < threshold
        self.assertFalse(is_fresh)

    def test_marts_layer_threshold(self):
        """Test marts layer uses 6-hour threshold."""
        LAYER_THRESHOLDS = {"raw": 24, "staging": 12, "marts": 6}

        staleness_hours = 5
        threshold = LAYER_THRESHOLDS["marts"]
        is_fresh = staleness_hours < threshold

        self.assertTrue(is_fresh)

        # Test just over threshold
        staleness_hours = 7
        is_fresh = staleness_hours < threshold
        self.assertFalse(is_fresh)

    def test_same_staleness_different_layers(self):
        """Test same staleness hours produces different freshness results per layer."""
        LAYER_THRESHOLDS = {"raw": 24, "staging": 12, "marts": 6}

        staleness_hours = 10  # 10 hours old

        raw_fresh = staleness_hours < LAYER_THRESHOLDS["raw"]
        staging_fresh = staleness_hours < LAYER_THRESHOLDS["staging"]
        marts_fresh = staleness_hours < LAYER_THRESHOLDS["marts"]

        self.assertTrue(raw_fresh)      # 10 < 24
        self.assertTrue(staging_fresh)  # 10 < 12
        self.assertFalse(marts_fresh)   # 10 >= 6


class TestAlertIntegration(unittest.TestCase):
    """Tests for alert integration with monitoring."""

    def test_format_freshness_alert(self):
        """Test formatting freshness check result for alerting."""
        from utils.alerting import AlertSeverity

        result = {
            "source_name": "transactions",
            "layer": "staging",
            "staleness_hours": 30.5,
            "is_fresh": False,
            "threshold_hours": 24,
        }

        # Simulate alert formatting
        message = (
            f"Data freshness alert: `{result['source_name']}` is stale\n"
            f"Layer: {result['layer']}\n"
            f"Staleness: {result['staleness_hours']}h (threshold: {result['threshold_hours']}h)"
        )

        self.assertIn("transactions", message)
        self.assertIn("30.5", message)
        self.assertIn("staging", message)

    def test_severity_escalation(self):
        """Test alert severity escalation based on conditions."""
        from utils.alerting import AlertSeverity

        def determine_severity(failure_rate, layer):
            if layer == "marts" or failure_rate > 0.5:
                return AlertSeverity.CRITICAL
            elif failure_rate > 0.2:
                return AlertSeverity.HIGH
            else:
                return AlertSeverity.MEDIUM

        self.assertEqual(determine_severity(0.6, "staging"), AlertSeverity.CRITICAL)
        self.assertEqual(determine_severity(0.3, "staging"), AlertSeverity.HIGH)
        self.assertEqual(determine_severity(0.1, "staging"), AlertSeverity.MEDIUM)
        self.assertEqual(determine_severity(0.1, "marts"), AlertSeverity.CRITICAL)


class TestAlertHistoryLogging(unittest.TestCase):
    """Tests for alert history logging functionality."""

    def test_alert_type_detection_task_failure(self):
        """Test alert type detection for task failure."""
        title = "Task Failure: test_dag.test_task"

        if "Task Failure" in title:
            alert_type = "task_failure"
        else:
            alert_type = "general"

        self.assertEqual(alert_type, "task_failure")

    def test_alert_type_detection_data_quality(self):
        """Test alert type detection for data quality."""
        title = "Data Quality Alert: transactions"

        if "Data Quality" in title:
            alert_type = "data_quality"
        else:
            alert_type = "general"

        self.assertEqual(alert_type, "data_quality")

    def test_alert_type_detection_sla(self):
        """Test alert type detection for SLA miss."""
        title = "SLA Miss: test_dag.test_task"

        if "SLA" in title:
            alert_type = "sla_miss"
        else:
            alert_type = "general"

        self.assertEqual(alert_type, "sla_miss")

    def test_alert_type_detection_platform_health(self):
        """Test alert type detection for platform health."""
        title = "Platform Health: Scheduler Health"

        if "Platform Health" in title:
            alert_type = "platform_health"
        else:
            alert_type = "general"

        self.assertEqual(alert_type, "platform_health")

    def test_channels_notified_list(self):
        """Test channels notified list construction."""
        email_sent = True
        slack_sent = True

        channels_notified = []
        if email_sent:
            channels_notified.append("email")
        if slack_sent:
            channels_notified.append("slack")

        self.assertEqual(channels_notified, ["email", "slack"])

    def test_channels_notified_partial(self):
        """Test channels notified when only one succeeds."""
        email_sent = False
        slack_sent = True

        channels_notified = []
        if email_sent:
            channels_notified.append("email")
        if slack_sent:
            channels_notified.append("slack")

        self.assertEqual(channels_notified, ["slack"])


if __name__ == "__main__":
    unittest.main()
