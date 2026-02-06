"""Unit tests for monitoring operators."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException

from pipelines.operators.monitoring_operator import (
    DataFreshnessCheckOperator,
    SLACheckOperator,
    VolumeAnomalyCheckOperator,
    HealthScoreOperator,
)


class TestDataFreshnessCheckOperator:
    """Test cases for DataFreshnessCheckOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="transactions",
            max_staleness_hours=12,
            postgres_conn_id="test_conn",
            alert_on_stale=False,
            layer="staging",
        )

        assert operator.source_name == "transactions"
        assert operator.max_staleness_hours == 12
        assert operator.postgres_conn_id == "test_conn"
        assert operator.alert_on_stale is False
        assert operator.layer == "staging"

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="transactions",
        )

        assert operator.max_staleness_hours == 24
        assert operator.postgres_conn_id == "postgres_default"
        assert operator.alert_on_stale is True
        assert operator.layer == "raw"

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "source_name" in DataFreshnessCheckOperator.template_fields
        assert "query_template" in DataFreshnessCheckOperator.template_fields

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_data_is_fresh(self, mock_hook_class, mock_context):
        """Test execution when data is fresh."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime.now(), 2.5)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="transactions",
            max_staleness_hours=24,
            alert_on_stale=False,
        )

        result = operator.execute(mock_context)

        assert result["is_fresh"] is True
        assert result["source_name"] == "transactions"
        assert result["staleness_hours"] == 2.5

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_data_is_stale_with_alert(self, mock_hook_class, mock_context):
        """Test execution when data is stale and alerts are enabled."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime.now() - timedelta(hours=48), 48.0)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="transactions",
            max_staleness_hours=24,
            alert_on_stale=True,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        assert "stale" in str(exc_info.value).lower()

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_no_data_returns_not_fresh(self, mock_hook_class, mock_context):
        """Test execution when no data exists."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None, None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = DataFreshnessCheckOperator(
            task_id="test_freshness",
            source_name="new_source",
            alert_on_stale=False,
        )

        result = operator.execute(mock_context)

        assert result["is_fresh"] is False
        assert result["last_update"] is None


class TestSLACheckOperator:
    """Test cases for SLACheckOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = SLACheckOperator(
            task_id="test_sla",
            target_dag_id="ingestion_dag",
            target_task_id="load_data",
            expected_duration_minutes=30,
            check_window_hours=48,
        )

        assert operator.target_dag_id == "ingestion_dag"
        assert operator.target_task_id == "load_data"
        assert operator.expected_duration_minutes == 30
        assert operator.check_window_hours == 48

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = SLACheckOperator(
            task_id="test_sla",
            target_dag_id="my_dag",
        )

        assert operator.target_task_id is None
        assert operator.expected_duration_minutes == 60
        assert operator.check_window_hours == 24

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_sla_met(self, mock_hook_class, mock_context):
        """Test execution when SLA is met."""
        now = datetime.now()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (now, now, now + timedelta(minutes=25), 25.0, "success"),
            (now - timedelta(days=1), now - timedelta(days=1), now - timedelta(days=1) + timedelta(minutes=28), 28.0, "success"),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = SLACheckOperator(
            task_id="test_sla",
            target_dag_id="test_dag",
            expected_duration_minutes=30,
        )

        result = operator.execute(mock_context)

        assert result["compliance_percent"] == 100.0
        assert result["sla_met"] == 2
        assert result["sla_missed"] == 0

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_sla_partially_missed(self, mock_hook_class, mock_context):
        """Test execution when SLA is partially missed."""
        now = datetime.now()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (now, now, now + timedelta(minutes=25), 25.0, "success"),  # Met
            (now - timedelta(days=1), now - timedelta(days=1), now - timedelta(days=1) + timedelta(minutes=45), 45.0, "success"),  # Missed
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = SLACheckOperator(
            task_id="test_sla",
            target_dag_id="test_dag",
            expected_duration_minutes=30,
        )

        result = operator.execute(mock_context)

        assert result["compliance_percent"] == 50.0
        assert result["sla_met"] == 1
        assert result["sla_missed"] == 1


class TestVolumeAnomalyCheckOperator:
    """Test cases for VolumeAnomalyCheckOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume",
            table_name="transactions",
            schema_name="staging",
            deviation_threshold_percent=30.0,
            historical_window_days=14,
        )

        assert operator.table_name == "transactions"
        assert operator.schema_name == "staging"
        assert operator.deviation_threshold_percent == 30.0
        assert operator.historical_window_days == 14

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume",
            table_name="orders",
        )

        assert operator.schema_name == "public"
        assert operator.deviation_threshold_percent == 50.0
        assert operator.historical_window_days == 7

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_volume_normal(self, mock_hook_class, mock_context):
        """Test execution when volume is within normal range."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (1000,),  # Current count
            (1050.0, 100.0, 900, 1200, 7),  # Historical stats
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume",
            table_name="transactions",
            deviation_threshold_percent=50.0,
        )

        result = operator.execute(mock_context)

        assert result["is_anomaly"] is False
        assert result["current_count"] == 1000

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_volume_anomaly_detected(self, mock_hook_class, mock_context):
        """Test execution when volume anomaly is detected."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (100,),  # Current count (much lower)
            (1000.0, 50.0, 900, 1100, 7),  # Historical stats
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume",
            table_name="transactions",
            deviation_threshold_percent=50.0,
        )

        result = operator.execute(mock_context)

        assert result["is_anomaly"] is True
        assert result["deviation_percent"] == 90.0  # 90% deviation

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_zero_rows_is_anomaly(self, mock_hook_class, mock_context):
        """Test execution when current rows is zero."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (0,),  # Current count is zero
            (1000.0, 50.0, 900, 1100, 7),  # Had historical data
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = VolumeAnomalyCheckOperator(
            task_id="test_volume",
            table_name="transactions",
        )

        result = operator.execute(mock_context)

        assert result["is_anomaly"] is True


class TestHealthScoreOperator:
    """Test cases for HealthScoreOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = HealthScoreOperator(
            task_id="test_health",
            postgres_conn_id="custom_conn",
        )

        assert operator.postgres_conn_id == "custom_conn"

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = HealthScoreOperator(task_id="test_health")

        assert operator.postgres_conn_id == "postgres_default"

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_returns_health_score(self, mock_hook_class, mock_context):
        """Test execution returns health score data."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True, 5, 98.5, 0, 10, 95)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = HealthScoreOperator(task_id="test_health")

        result = operator.execute(mock_context)

        assert result["health_score"] == 95
        assert result["scheduler_healthy"] is True
        assert result["failed_tasks_24h"] == 5
        assert result["sla_compliance_percent"] == 98.5

    @patch("pipelines.operators.monitoring_operator.PostgresHook")
    def test_execute_handles_no_metrics(self, mock_hook_class, mock_context):
        """Test execution when no metrics are available."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        operator = HealthScoreOperator(task_id="test_health")

        result = operator.execute(mock_context)

        assert result["health_score"] == 100
        assert "message" in result
