"""Unit tests for DataQualityCheckOperator."""

import pytest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException

from pipelines.operators.data_quality_operator import DataQualityCheckOperator


class TestDataQualityCheckOperator:
    """Test cases for DataQualityCheckOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    @pytest.fixture
    def sample_checks(self):
        """Create sample data quality checks."""
        return [
            {
                "sql": "SELECT COUNT(*) FROM transactions",
                "description": "Check transactions count",
                "min_threshold": 100,
            },
            {
                "sql": "SELECT AVG(amount) FROM transactions",
                "description": "Check average amount",
                "min_threshold": 0,
                "max_threshold": 10000,
            },
            {
                "sql": "SELECT COUNT(*) FROM transactions WHERE amount < 0",
                "description": "Check negative amounts",
                "max_threshold": 0,
            },
        ]

    def test_operator_initialization(self, sample_checks):
        """Test operator initializes with correct parameters."""
        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=sample_checks,
            postgres_conn_id="test_conn",
            min_threshold=1,
            max_threshold=1000,
        )

        assert operator.sql_checks == sample_checks
        assert operator.postgres_conn_id == "test_conn"
        assert operator.min_threshold == 1
        assert operator.max_threshold == 1000

    def test_operator_default_values(self, sample_checks):
        """Test operator uses correct default values."""
        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=sample_checks,
        )

        assert operator.postgres_conn_id == "postgres_default"
        assert operator.min_threshold is None
        assert operator.max_threshold is None

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "sql_checks" in DataQualityCheckOperator.template_fields

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_all_checks_pass(self, mock_hook_class, mock_context, sample_checks):
        """Test execution when all quality checks pass."""
        mock_hook = MagicMock()
        mock_hook.get_first.side_effect = [
            (500,),  # Count > 100 (passes)
            (150.5,),  # Avg between 0 and 10000 (passes)
            (0,),  # No negative amounts (passes max_threshold=0)
        ]
        mock_hook_class.return_value = mock_hook

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=sample_checks,
        )

        # Should not raise
        operator.execute(mock_context)

        assert mock_hook.get_first.call_count == 3

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_check_fails_below_min(self, mock_hook_class, mock_context):
        """Test execution fails when value is below minimum threshold."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (50,)  # Below min of 100
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM transactions",
                "description": "Row count check",
                "min_threshold": 100,
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        assert "below minimum threshold" in str(exc_info.value)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_check_fails_above_max(self, mock_hook_class, mock_context):
        """Test execution fails when value is above maximum threshold."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (100,)  # Above max of 0
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM errors",
                "description": "Error count check",
                "max_threshold": 0,
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        assert "above maximum threshold" in str(exc_info.value)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_check_fails_null_result(self, mock_hook_class, mock_context):
        """Test execution fails when query returns null."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = None
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT MAX(date) FROM empty_table",
                "description": "Latest date check",
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        assert "no results" in str(exc_info.value)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_check_fails_zero_without_threshold(self, mock_hook_class, mock_context):
        """Test execution fails when value is 0 without thresholds set."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (0,)
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM transactions",
                "description": "Row count check",
                # No thresholds - zero is considered failure
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        assert "value is 0" in str(exc_info.value)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_uses_default_thresholds(self, mock_hook_class, mock_context):
        """Test operator uses default thresholds when check-level ones missing."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (50,)  # Below default min of 100
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM transactions",
                "description": "Row count check",
                # No thresholds - uses operator defaults
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
            min_threshold=100,  # Default min
        )

        with pytest.raises(AirflowException):
            operator.execute(mock_context)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_multiple_failures_reported(self, mock_hook_class, mock_context):
        """Test all failures are reported when multiple checks fail."""
        mock_hook = MagicMock()
        mock_hook.get_first.side_effect = [
            (50,),  # Fails min
            (20000,),  # Fails max
        ]
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM table1",
                "description": "Check 1",
                "min_threshold": 100,
            },
            {
                "sql": "SELECT AVG(val) FROM table2",
                "description": "Check 2",
                "max_threshold": 1000,
            },
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        with pytest.raises(AirflowException) as exc_info:
            operator.execute(mock_context)

        error_msg = str(exc_info.value)
        assert "2/2" in error_msg  # Both checks failed
        assert "Check 1" in error_msg
        assert "Check 2" in error_msg

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_execute_uses_default_description(self, mock_hook_class, mock_context):
        """Test operator uses default description when not provided."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (100,)
        mock_hook_class.return_value = mock_hook

        checks = [
            {
                "sql": "SELECT COUNT(*) FROM transactions",
                # No description
                "min_threshold": 1,
            }
        ]

        operator = DataQualityCheckOperator(
            task_id="test_quality",
            sql_checks=checks,
        )

        # Should not raise, just verifying it handles missing description
        operator.execute(mock_context)
