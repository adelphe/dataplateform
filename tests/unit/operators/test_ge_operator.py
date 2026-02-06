"""Unit tests for Great Expectations operators."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from airflow.exceptions import AirflowException

from pipelines.operators.ge_operator import (
    GreatExpectationsOperator,
    GreatExpectationsValidationOperator,
)


class TestGreatExpectationsOperator:
    """Test cases for GreatExpectationsOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        ti = MagicMock()
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": ti,
            "ti": ti,
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = GreatExpectationsOperator(
            task_id="test_ge",
            checkpoint_name="raw_checkpoint",
            ge_root_dir="/path/to/ge",
            fail_task_on_validation_failure=False,
            return_json_dict=True,
        )

        assert operator.checkpoint_name == "raw_checkpoint"
        assert operator.ge_root_dir == "/path/to/ge"
        assert operator.fail_task_on_validation_failure is False
        assert operator.return_json_dict is True

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = GreatExpectationsOperator(
            task_id="test_ge",
            checkpoint_name="test_checkpoint",
        )

        assert operator.ge_root_dir is None
        assert operator.fail_task_on_validation_failure is True
        assert operator.return_json_dict is False
        assert operator.runtime_configuration == {}
        assert operator.batch_request is None
        assert operator.checkpoint_kwargs == {}

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "checkpoint_name" in GreatExpectationsOperator.template_fields
        assert "batch_request" in GreatExpectationsOperator.template_fields
        assert "expectation_suite_name" in GreatExpectationsOperator.template_fields

    @patch("pipelines.operators.ge_operator.gx", create=True)
    @patch("pipelines.operators.ge_operator.FileDataContext", create=True)
    def test_execute_successful_validation(self, mock_file_context, mock_gx, mock_context):
        """Test successful checkpoint execution."""
        # Mock the checkpoint result
        mock_run_result = {
            "validation_result": {
                "success": True,
                "meta": {"expectation_suite_name": "test_suite"},
                "statistics": {
                    "evaluated_expectations": 10,
                    "successful_expectations": 10,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "results": [],
            }
        }

        mock_checkpoint_result = MagicMock()
        mock_checkpoint_result.success = True
        mock_checkpoint_result.run_id.run_name = "test_run_123"
        mock_checkpoint_result.run_results = {"key1": mock_run_result}

        mock_data_context = MagicMock()
        mock_data_context.run_checkpoint.return_value = mock_checkpoint_result
        mock_file_context.return_value = mock_data_context

        with patch.object(
            GreatExpectationsOperator,
            "_get_ge_root_dir",
            return_value="/fake/ge/path",
        ):
            operator = GreatExpectationsOperator(
                task_id="test_ge",
                checkpoint_name="test_checkpoint",
            )

            result = operator.execute(mock_context)

        assert result is True
        mock_data_context.run_checkpoint.assert_called_once()

    @patch("pipelines.operators.ge_operator.gx", create=True)
    @patch("pipelines.operators.ge_operator.FileDataContext", create=True)
    def test_execute_failed_validation_raises_exception(
        self, mock_file_context, mock_gx, mock_context
    ):
        """Test failed validation raises AirflowException."""
        mock_run_result = {
            "validation_result": {
                "success": False,
                "meta": {"expectation_suite_name": "test_suite"},
                "statistics": {
                    "evaluated_expectations": 10,
                    "successful_expectations": 7,
                    "unsuccessful_expectations": 3,
                    "success_percent": 70.0,
                },
                "results": [
                    {
                        "success": False,
                        "expectation_config": {
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "kwargs": {"column": "id"},
                        },
                    }
                ],
            }
        }

        mock_checkpoint_result = MagicMock()
        mock_checkpoint_result.success = False
        mock_checkpoint_result.run_id.run_name = "test_run_456"
        mock_checkpoint_result.run_results = {"key1": mock_run_result}

        mock_data_context = MagicMock()
        mock_data_context.run_checkpoint.return_value = mock_checkpoint_result
        mock_file_context.return_value = mock_data_context

        with patch.object(
            GreatExpectationsOperator,
            "_get_ge_root_dir",
            return_value="/fake/ge/path",
        ):
            operator = GreatExpectationsOperator(
                task_id="test_ge",
                checkpoint_name="test_checkpoint",
                fail_task_on_validation_failure=True,
            )

            with pytest.raises(AirflowException) as exc_info:
                operator.execute(mock_context)

        assert "validation failed" in str(exc_info.value).lower()

    @patch("pipelines.operators.ge_operator.gx", create=True)
    @patch("pipelines.operators.ge_operator.FileDataContext", create=True)
    def test_execute_failed_validation_no_raise(
        self, mock_file_context, mock_gx, mock_context
    ):
        """Test failed validation doesn't raise when fail_task_on_validation_failure=False."""
        mock_run_result = {
            "validation_result": {
                "success": False,
                "meta": {"expectation_suite_name": "test_suite"},
                "statistics": {
                    "evaluated_expectations": 10,
                    "successful_expectations": 7,
                    "unsuccessful_expectations": 3,
                    "success_percent": 70.0,
                },
                "results": [],
            }
        }

        mock_checkpoint_result = MagicMock()
        mock_checkpoint_result.success = False
        mock_checkpoint_result.run_id.run_name = "test_run_789"
        mock_checkpoint_result.run_results = {"key1": mock_run_result}

        mock_data_context = MagicMock()
        mock_data_context.run_checkpoint.return_value = mock_checkpoint_result
        mock_file_context.return_value = mock_data_context

        with patch.object(
            GreatExpectationsOperator,
            "_get_ge_root_dir",
            return_value="/fake/ge/path",
        ):
            operator = GreatExpectationsOperator(
                task_id="test_ge",
                checkpoint_name="test_checkpoint",
                fail_task_on_validation_failure=False,
            )

            result = operator.execute(mock_context)

        assert result is False

    @patch("pipelines.operators.ge_operator.gx", create=True)
    @patch("pipelines.operators.ge_operator.FileDataContext", create=True)
    def test_execute_returns_json_dict(self, mock_file_context, mock_gx, mock_context):
        """Test return_json_dict returns dictionary."""
        mock_run_result = {
            "validation_result": {
                "success": True,
                "meta": {"expectation_suite_name": "test_suite"},
                "statistics": {
                    "evaluated_expectations": 5,
                    "successful_expectations": 5,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "results": [],
            }
        }

        mock_checkpoint_result = MagicMock()
        mock_checkpoint_result.success = True
        mock_checkpoint_result.run_id.run_name = "test_run"
        mock_checkpoint_result.run_results = {"key1": mock_run_result}

        mock_data_context = MagicMock()
        mock_data_context.run_checkpoint.return_value = mock_checkpoint_result
        mock_file_context.return_value = mock_data_context

        with patch.object(
            GreatExpectationsOperator,
            "_get_ge_root_dir",
            return_value="/fake/ge/path",
        ):
            operator = GreatExpectationsOperator(
                task_id="test_ge",
                checkpoint_name="test_checkpoint",
                return_json_dict=True,
            )

            result = operator.execute(mock_context)

        assert isinstance(result, dict)
        assert result["success"] is True
        assert result["checkpoint_name"] == "test_checkpoint"
        assert "validation_results" in result

    def test_ge_not_installed_raises_exception(self, mock_context):
        """Test proper error when Great Expectations is not installed."""
        operator = GreatExpectationsOperator(
            task_id="test_ge",
            checkpoint_name="test_checkpoint",
            ge_root_dir="/fake/path",
        )

        with patch.dict("sys.modules", {"great_expectations": None}):
            with pytest.raises((AirflowException, ImportError)):
                operator.execute(mock_context)


class TestGreatExpectationsValidationOperator:
    """Test cases for GreatExpectationsValidationOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        ti = MagicMock()
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": ti,
            "ti": ti,
            "ds": "2024-01-01",
        }

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = GreatExpectationsValidationOperator(
            task_id="test_ge_val",
            datasource_name="my_datasource",
            data_asset_name="transactions",
            expectation_suite_name="transactions_suite",
            ge_root_dir="/path/to/ge",
            fail_task_on_validation_failure=False,
            batch_identifiers={"date": "2024-01-01"},
        )

        assert operator.datasource_name == "my_datasource"
        assert operator.data_asset_name == "transactions"
        assert operator.expectation_suite_name == "transactions_suite"
        assert operator.ge_root_dir == "/path/to/ge"
        assert operator.fail_task_on_validation_failure is False
        assert operator.batch_identifiers == {"date": "2024-01-01"}

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = GreatExpectationsValidationOperator(
            task_id="test_ge_val",
            datasource_name="ds",
            data_asset_name="asset",
            expectation_suite_name="suite",
        )

        assert operator.ge_root_dir is None
        assert operator.fail_task_on_validation_failure is True
        assert operator.batch_identifiers == {}

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "data_asset_name" in GreatExpectationsValidationOperator.template_fields
        assert "expectation_suite_name" in GreatExpectationsValidationOperator.template_fields
        assert "batch_identifiers" in GreatExpectationsValidationOperator.template_fields
