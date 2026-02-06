"""Unit tests for MinIOFileSensorOperator."""

import pytest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from pipelines.operators.file_sensor_operator import MinIOFileSensorOperator


class TestMinIOFileSensorOperator:
    """Test cases for MinIOFileSensorOperator."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow task context."""
        return {
            "dag_run": MagicMock(run_id="test_run"),
            "task_instance": MagicMock(),
            "ds": "2024-01-01",
        }

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Airflow connection."""
        conn = MagicMock()
        conn.login = "minioadmin"
        conn.password = "minioadmin"
        conn.extra_dejson = {"endpoint_url": "http://localhost:9000"}
        return conn

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="data/input/",
            minio_conn_id="minio_conn",
            wildcard_match=True,
        )

        assert operator.bucket_name == "test-bucket"
        assert operator.prefix == "data/input/"
        assert operator.minio_conn_id == "minio_conn"
        assert operator.wildcard_match is True

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="files/",
        )

        assert operator.minio_conn_id == "minio_default"
        assert operator.wildcard_match is False

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "bucket_name" in MinIOFileSensorOperator.template_fields
        assert "prefix" in MinIOFileSensorOperator.template_fields

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_finds_file_with_prefix(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke returns True when file with prefix exists."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "data/input/file1.csv"}]
        }
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="data/input/",
        )

        result = operator.poke(mock_context)

        assert result is True

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_no_file_found(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke returns False when no file exists."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {}
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="nonexistent/",
        )

        result = operator.poke(mock_context)

        assert result is False

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_wildcard_match_found(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke with wildcard pattern finds matching file."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data/input_2024-01-01.csv"},
                {"Key": "data/input_2024-01-02.csv"},
            ]
        }
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="data/input_*.csv",
            wildcard_match=True,
        )

        result = operator.poke(mock_context)

        assert result is True

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_wildcard_no_match(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke with wildcard pattern returns False when no match."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data/output_2024-01-01.csv"},  # Different prefix
            ]
        }
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="data/input_*.csv",
            wildcard_match=True,
        )

        result = operator.poke(mock_context)

        assert result is False

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_handles_client_error(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke returns False on client error."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2"
        )
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="nonexistent-bucket",
            prefix="files/",
        )

        result = operator.poke(mock_context)

        assert result is False

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_falls_back_to_env_vars(
        self, mock_boto_client, mock_get_connection, mock_context
    ):
        """Test operator falls back to environment variables when connection fails."""
        mock_get_connection.side_effect = Exception("Connection not found")
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "data/file.csv"}]
        }
        mock_boto_client.return_value = mock_s3

        with patch.dict(
            "os.environ",
            {
                "AWS_ENDPOINT_URL": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret",
            },
        ):
            operator = MinIOFileSensorOperator(
                task_id="test_sensor",
                bucket_name="test-bucket",
                prefix="data/",
            )

            result = operator.poke(mock_context)

        assert result is True

    @patch("pipelines.operators.file_sensor_operator.BaseHook.get_connection")
    @patch("pipelines.operators.file_sensor_operator.boto3.client")
    def test_poke_empty_contents_in_response(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection
    ):
        """Test poke handles empty Contents in response."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_boto_client.return_value = mock_s3

        operator = MinIOFileSensorOperator(
            task_id="test_sensor",
            bucket_name="test-bucket",
            prefix="empty/",
        )

        result = operator.poke(mock_context)

        assert result is False


class TestMinIOFileSensorOperatorIntegration:
    """Integration-style tests for MinIOFileSensorOperator."""

    def test_inherits_from_base_sensor(self):
        """Test operator inherits from BaseSensorOperator."""
        from airflow.sensors.base import BaseSensorOperator

        assert issubclass(MinIOFileSensorOperator, BaseSensorOperator)

    def test_wildcard_patterns(self):
        """Test various wildcard patterns are supported."""
        patterns = [
            "data/*.csv",
            "data/2024-*/file.csv",
            "data/input_*.parquet",
            "prefix/*/subdir/*.json",
        ]

        for pattern in patterns:
            operator = MinIOFileSensorOperator(
                task_id=f"test_{pattern.replace('/', '_').replace('*', 'star')}",
                bucket_name="test-bucket",
                prefix=pattern,
                wildcard_match=True,
            )
            assert operator.wildcard_match is True
            assert operator.prefix == pattern
