"""Unit tests for custom Airflow operators."""

import os
from unittest.mock import MagicMock, patch, mock_open

import pytest
from botocore.exceptions import ClientError


class TestMinIOUploadOperator:
    """Tests for the MinIOUploadOperator."""

    def _make_operator(self, **kwargs):
        from pipelines.operators.minio_operator import MinIOUploadOperator

        defaults = {
            "task_id": "test_upload",
            "bucket_name": "test-bucket",
            "object_name": "test/file.csv",
            "file_path": "/tmp/test.csv",
        }
        defaults.update(kwargs)
        return MinIOUploadOperator(**defaults)

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3")
    @patch("os.path.isfile", return_value=True)
    def test_execute_uploads_file(self, mock_isfile, mock_boto3, mock_get_conn, mock_airflow_context, minio_env):
        """Verify file upload to MinIO succeeds."""
        # Mock the Airflow connection
        mock_conn = MagicMock()
        mock_conn.login = "test-key"
        mock_conn.password = "test-secret"
        mock_conn.extra_dejson = {"endpoint_url": "http://localhost:9000"}
        mock_get_conn.return_value = mock_conn

        client = MagicMock()
        mock_boto3.client.return_value = client
        client.head_bucket.return_value = {}

        op = self._make_operator()
        result = op.execute(mock_airflow_context)

        assert result == "test/file.csv"
        client.upload_file.assert_called_once()

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3")
    @patch("os.path.isfile", return_value=True)
    def test_creates_bucket_if_missing(self, mock_isfile, mock_boto3, mock_get_conn, mock_airflow_context, minio_env):
        """Verify bucket creation when it doesn't exist."""
        # Mock the Airflow connection
        mock_conn = MagicMock()
        mock_conn.login = "test-key"
        mock_conn.password = "test-secret"
        mock_conn.extra_dejson = {"endpoint_url": "http://localhost:9000"}
        mock_get_conn.return_value = mock_conn

        client = MagicMock()
        mock_boto3.client.return_value = client
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")

        op = self._make_operator()
        op.execute(mock_airflow_context)

        client.create_bucket.assert_called_once_with(Bucket="test-bucket")

    @patch("os.path.isfile", return_value=False)
    def test_raises_on_missing_file(self, mock_isfile, mock_airflow_context, minio_env):
        """Verify FileNotFoundError when source file doesn't exist."""
        op = self._make_operator()

        with pytest.raises(FileNotFoundError):
            op.execute(mock_airflow_context)

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3")
    @patch("os.path.isfile", return_value=True)
    def test_skip_upload_when_replace_false_and_exists(
        self, mock_isfile, mock_boto3, mock_get_conn, mock_airflow_context, minio_env
    ):
        """Verify upload is skipped when replace=False and object exists."""
        # Mock the Airflow connection
        mock_conn = MagicMock()
        mock_conn.login = "test-key"
        mock_conn.password = "test-secret"
        mock_conn.extra_dejson = {"endpoint_url": "http://localhost:9000"}
        mock_get_conn.return_value = mock_conn

        client = MagicMock()
        mock_boto3.client.return_value = client
        client.head_bucket.return_value = {}
        client.head_object.return_value = {}

        op = self._make_operator(replace=False)
        result = op.execute(mock_airflow_context)

        assert result == "test/file.csv"
        client.upload_file.assert_not_called()


class TestDataQualityCheckOperator:
    """Tests for the DataQualityCheckOperator."""

    def _make_operator(self, **kwargs):
        from pipelines.operators.data_quality_operator import DataQualityCheckOperator

        defaults = {
            "task_id": "test_quality",
            "sql_checks": [
                {
                    "sql": "SELECT COUNT(*) FROM test_table;",
                    "description": "Row count check",
                    "min_threshold": 1,
                }
            ],
        }
        defaults.update(kwargs)
        return DataQualityCheckOperator(**defaults)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_passes_when_above_threshold(self, mock_hook_cls, mock_airflow_context):
        """Verify check passes when value meets threshold."""
        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.return_value = (100,)

        op = self._make_operator()
        op.execute(mock_airflow_context)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_fails_when_below_threshold(self, mock_hook_cls, mock_airflow_context):
        """Verify AirflowException when value is below threshold."""
        from airflow.exceptions import AirflowException

        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.return_value = (0,)

        op = self._make_operator()
        with pytest.raises(AirflowException, match="below minimum threshold"):
            op.execute(mock_airflow_context)

    @patch("pipelines.operators.data_quality_operator.PostgresHook")
    def test_fails_when_above_max_threshold(self, mock_hook_cls, mock_airflow_context):
        """Verify AirflowException when value exceeds max threshold."""
        from airflow.exceptions import AirflowException

        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.return_value = (50,)

        op = self._make_operator(
            sql_checks=[
                {
                    "sql": "SELECT COUNT(*) FROM duplicates;",
                    "description": "Duplicate check",
                    "max_threshold": 0,
                }
            ]
        )
        with pytest.raises(AirflowException, match="above maximum threshold"):
            op.execute(mock_airflow_context)


class TestPostgresTableSensorOperator:
    """Tests for the PostgresTableSensorOperator."""

    def _make_operator(self, **kwargs):
        from pipelines.operators.postgres_operator import PostgresTableSensorOperator

        defaults = {
            "task_id": "test_sensor",
            "table_name": "test_table",
        }
        defaults.update(kwargs)
        return PostgresTableSensorOperator(**defaults)

    @patch("pipelines.operators.postgres_operator.PostgresHook")
    def test_poke_returns_true_when_table_exists(self, mock_hook_cls, mock_airflow_context):
        """Verify sensor returns True when table exists."""
        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.return_value = (True,)

        op = self._make_operator()
        assert op.poke(mock_airflow_context) is True

    @patch("pipelines.operators.postgres_operator.PostgresHook")
    def test_poke_returns_false_when_table_missing(self, mock_hook_cls, mock_airflow_context):
        """Verify sensor returns False when table doesn't exist."""
        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.return_value = (False,)

        op = self._make_operator()
        assert op.poke(mock_airflow_context) is False

    @patch("pipelines.operators.postgres_operator.PostgresHook")
    def test_poke_checks_row_count(self, mock_hook_cls, mock_airflow_context):
        """Verify sensor checks minimum row count when specified."""
        hook = MagicMock()
        mock_hook_cls.return_value = hook
        hook.get_first.side_effect = [(True,), (5,)]

        op = self._make_operator(min_row_count=10)
        assert op.poke(mock_airflow_context) is False


class TestMinIOFileSensorOperator:
    """Tests for the MinIOFileSensorOperator."""

    def _make_operator(self, **kwargs):
        from pipelines.operators.file_sensor_operator import MinIOFileSensorOperator

        defaults = {
            "task_id": "test_file_sensor",
            "bucket_name": "test-bucket",
            "prefix": "data/",
        }
        defaults.update(kwargs)
        return MinIOFileSensorOperator(**defaults)

    @patch("pipelines.operators.file_sensor_operator.boto3")
    def test_poke_returns_true_when_file_exists(self, mock_boto3, mock_airflow_context, minio_env):
        """Verify sensor returns True when file exists."""
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": "data/file.csv"}]
        }

        op = self._make_operator()
        assert op.poke(mock_airflow_context) is True

    @patch("pipelines.operators.file_sensor_operator.boto3")
    def test_poke_returns_false_when_no_files(self, mock_boto3, mock_airflow_context, minio_env):
        """Verify sensor returns False when no files match."""
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.list_objects_v2.return_value = {}

        op = self._make_operator()
        assert op.poke(mock_airflow_context) is False

    @patch("pipelines.operators.file_sensor_operator.boto3")
    def test_wildcard_match(self, mock_boto3, mock_airflow_context, minio_env):
        """Verify wildcard pattern matching works."""
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data/export_2024.csv"},
                {"Key": "data/export_2024.json"},
            ]
        }

        op = self._make_operator(prefix="data/*.csv", wildcard_match=True)
        assert op.poke(mock_airflow_context) is True
