"""Tests for the file ingestor module."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from ingestion.metadata_logger import IngestionError


class TestIngestCsvFile:
    """Tests for ingest_csv_file."""

    @patch("ingestion.file_ingestor.upload_to_layer")
    @patch("ingestion.file_ingestor.log_ingestion_success")
    @patch("ingestion.file_ingestor.log_ingestion_start")
    @patch("ingestion.file_ingestor.log_schema_change")
    def test_ingests_csv_successfully(
        self, mock_log_schema, mock_start, mock_success, mock_upload, sample_csv_file
    ):
        mock_start.return_value = 1
        mock_upload.return_value = "s3://raw/files/test.parquet"

        from ingestion.file_ingestor import ingest_csv_file

        result = ingest_csv_file(sample_csv_file, "files/test.csv", "2024-01-01")

        assert result["uri"] == "s3://raw/files/test.parquet"
        assert result["row_count"] == 2
        assert "checksum" in result
        assert "schema" in result
        mock_start.assert_called_once()
        mock_success.assert_called_once()
        mock_log_schema.assert_called_once()

    @patch("ingestion.file_ingestor.upload_to_layer")
    @patch("ingestion.file_ingestor.log_ingestion_failure")
    @patch("ingestion.file_ingestor.log_ingestion_start")
    @patch("ingestion.file_ingestor.log_schema_change")
    def test_logs_failure_on_error(
        self, mock_log_schema, mock_start, mock_failure, mock_upload
    ):
        mock_start.return_value = 1
        mock_upload.side_effect = Exception("upload failed")

        from ingestion.file_ingestor import ingest_csv_file

        with pytest.raises(IngestionError, match="Failed to ingest CSV"):
            ingest_csv_file("/nonexistent.csv", "files/test.csv", "2024-01-01")

        mock_failure.assert_called_once()


class TestIngestJsonFile:
    """Tests for ingest_json_file."""

    @patch("ingestion.file_ingestor.upload_to_layer")
    @patch("ingestion.file_ingestor.log_ingestion_success")
    @patch("ingestion.file_ingestor.log_ingestion_start")
    @patch("ingestion.file_ingestor.log_schema_change")
    def test_ingests_json_successfully(
        self, mock_log_schema, mock_start, mock_success, mock_upload, sample_json_file
    ):
        mock_start.return_value = 1
        mock_upload.return_value = "s3://raw/files/test.parquet"

        from ingestion.file_ingestor import ingest_json_file

        result = ingest_json_file(sample_json_file, "files/test.json", "2024-01-01")

        assert result["uri"] == "s3://raw/files/test.parquet"
        assert result["row_count"] == 2
        assert "checksum" in result
        mock_start.assert_called_once()
        mock_success.assert_called_once()


class TestBatchIngestFiles:
    """Tests for batch_ingest_files."""

    @patch("ingestion.file_ingestor.upload_to_layer")
    @patch("ingestion.file_ingestor.log_ingestion_success")
    @patch("ingestion.file_ingestor.log_ingestion_start")
    @patch("ingestion.file_ingestor.log_schema_change")
    def test_processes_multiple_csv_files(
        self, mock_log_schema, mock_start, mock_success, mock_upload
    ):
        mock_start.return_value = 1
        mock_upload.return_value = "s3://raw/files/test.parquet"

        tmpdir = tempfile.mkdtemp()
        for name in ["a.csv", "b.csv"]:
            with open(os.path.join(tmpdir, name), "w") as f:
                f.write("x,y\n1,2\n3,4\n")

        from ingestion.file_ingestor import batch_ingest_files

        results = batch_ingest_files(tmpdir, "*.csv", "2024-01-01")
        assert len(results) == 2
        assert all("uri" in r for r in results)

    def test_returns_empty_for_no_matches(self):
        from ingestion.file_ingestor import batch_ingest_files

        tmpdir = tempfile.mkdtemp()
        results = batch_ingest_files(tmpdir, "*.csv", "2024-01-01")
        assert results == []

    @patch("ingestion.file_ingestor.upload_to_layer")
    @patch("ingestion.file_ingestor.log_ingestion_success")
    @patch("ingestion.file_ingestor.log_ingestion_start")
    @patch("ingestion.file_ingestor.log_schema_change")
    def test_skips_unsupported_extensions(
        self, mock_log_schema, mock_start, mock_success, mock_upload
    ):
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "data.xml"), "w") as f:
            f.write("<root/>")

        from ingestion.file_ingestor import batch_ingest_files

        results = batch_ingest_files(tmpdir, "*.xml", "2024-01-01")
        assert results == []
