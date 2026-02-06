"""Unit tests for MinIO operators."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipelines.operators.minio_operator import MinIOUploadOperator


class TestMinIOUploadOperator:
    """Test cases for MinIOUploadOperator."""

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

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing uploads."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("id,name,value\n1,test,100\n2,test2,200\n")
            temp_path = f.name
        yield temp_path
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    def test_operator_initialization(self):
        """Test operator initializes with correct parameters."""
        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="test/file.csv",
            file_path="/path/to/file.csv",
            minio_conn_id="minio_conn",
            replace=False,
        )

        assert operator.bucket_name == "test-bucket"
        assert operator.object_name == "test/file.csv"
        assert operator.file_path == "/path/to/file.csv"
        assert operator.minio_conn_id == "minio_conn"
        assert operator.replace is False

    def test_operator_default_values(self):
        """Test operator uses correct default values."""
        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="test.csv",
            file_path="/path/to/file.csv",
        )

        assert operator.minio_conn_id == "minio_default"
        assert operator.replace is True

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        assert "bucket_name" in MinIOUploadOperator.template_fields
        assert "object_name" in MinIOUploadOperator.template_fields
        assert "file_path" in MinIOUploadOperator.template_fields

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3.client")
    def test_execute_successful_upload(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection, temp_file
    ):
        """Test successful file upload."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="uploaded/file.csv",
            file_path=temp_file,
        )

        result = operator.execute(mock_context)

        assert result == "uploaded/file.csv"
        mock_s3.upload_file.assert_called_once_with(
            Filename=temp_file,
            Bucket="test-bucket",
            Key="uploaded/file.csv",
        )

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3.client")
    def test_execute_creates_bucket_if_not_exists(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection, temp_file
    ):
        """Test bucket is created when it doesn't exist."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadBucket"
        )
        mock_boto_client.return_value = mock_s3

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="new-bucket",
            object_name="file.csv",
            file_path=temp_file,
        )

        operator.execute(mock_context)

        mock_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_execute_file_not_found(self, mock_context):
        """Test operator raises error when file doesn't exist."""
        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="file.csv",
            file_path="/nonexistent/file.csv",
        )

        with pytest.raises(FileNotFoundError, match="File not found"):
            operator.execute(mock_context)

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3.client")
    def test_execute_skip_upload_when_replace_false(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection, temp_file
    ):
        """Test upload is skipped when replace=False and object exists."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="existing.csv",
            file_path=temp_file,
            replace=False,
        )

        result = operator.execute(mock_context)

        assert result == "existing.csv"
        mock_s3.upload_file.assert_not_called()

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3.client")
    def test_execute_retry_on_failure(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection, temp_file
    ):
        """Test upload retries on transient failures."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = [
            ClientError({"Error": {"Code": "500"}}, "PutObject"),
            ClientError({"Error": {"Code": "500"}}, "PutObject"),
            None,  # Third attempt succeeds
        ]
        mock_boto_client.return_value = mock_s3

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="file.csv",
            file_path=temp_file,
        )

        result = operator.execute(mock_context)

        assert result == "file.csv"
        assert mock_s3.upload_file.call_count == 3

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    @patch("pipelines.operators.minio_operator.boto3.client")
    def test_execute_fails_after_max_retries(
        self, mock_boto_client, mock_get_connection, mock_context, mock_connection, temp_file
    ):
        """Test upload fails after maximum retry attempts."""
        mock_get_connection.return_value = mock_connection
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = ClientError(
            {"Error": {"Code": "500"}}, "PutObject"
        )
        mock_boto_client.return_value = mock_s3

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="file.csv",
            file_path=temp_file,
        )

        with pytest.raises(ClientError):
            operator.execute(mock_context)

        assert mock_s3.upload_file.call_count == 3

    @patch("pipelines.operators.minio_operator.BaseHook.get_connection")
    def test_get_client_missing_credentials(self, mock_get_connection):
        """Test error when connection credentials are missing."""
        mock_conn = MagicMock()
        mock_conn.login = None
        mock_conn.password = None
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        operator = MinIOUploadOperator(
            task_id="test_upload",
            bucket_name="test-bucket",
            object_name="file.csv",
            file_path="/path/to/file.csv",
        )

        with pytest.raises(ValueError, match="missing login"):
            operator._get_client()
