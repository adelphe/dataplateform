"""Tests for the metadata logging module."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.metadata_logger import (
    compute_file_checksum,
    get_last_successful_run,
    log_ingestion_failure,
    log_ingestion_start,
    log_ingestion_success,
    ensure_metadata_tables,
)


class TestComputeFileChecksum:
    """Tests for compute_file_checksum."""

    def test_md5_checksum(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            path = f.name
        try:
            checksum = compute_file_checksum(path, "md5")
            assert len(checksum) == 32
            # Known MD5 of "hello world"
            assert checksum == "5eb63bbbe01eeed093cb22bb8f5acdc3"
        finally:
            os.unlink(path)

    def test_sha256_checksum(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            path = f.name
        try:
            checksum = compute_file_checksum(path, "sha256")
            assert len(checksum) == 64
        finally:
            os.unlink(path)

    def test_consistent_for_same_content(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            path = f.name
        try:
            c1 = compute_file_checksum(path)
            c2 = compute_file_checksum(path)
            assert c1 == c2
        finally:
            os.unlink(path)


class TestLogIngestion:
    """Tests for ingestion logging functions."""

    @patch("ingestion.metadata_logger.execute_query")
    def test_log_ingestion_start(self, mock_execute):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (42,)
        mock_execute.return_value = mock_result

        run_id = log_ingestion_start("test_source", "csv", "2024-01-01")
        assert run_id == 42
        mock_execute.assert_called_once()

    @patch("ingestion.metadata_logger.execute_query")
    def test_log_ingestion_success(self, mock_execute):
        log_ingestion_success(42, 100, 2048, "abc123")
        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert call_args[1]["params"]["run_id"] == 42
        assert call_args[1]["params"]["row_count"] == 100

    @patch("ingestion.metadata_logger.execute_query")
    def test_log_ingestion_failure(self, mock_execute):
        log_ingestion_failure(42, "Something broke")
        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert call_args[1]["params"]["error_message"] == "Something broke"

    @patch("ingestion.metadata_logger.execute_query")
    def test_ensure_metadata_tables(self, mock_execute):
        ensure_metadata_tables()
        mock_execute.assert_called_once()


class TestGetLastSuccessfulRun:
    """Tests for get_last_successful_run."""

    @patch("ingestion.metadata_logger.fetch_dataframe")
    def test_returns_dict_when_found(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame([{
            "id": 10,
            "source_name": "test",
            "source_type": "csv",
            "execution_date": "2024-01-01",
            "row_count": 50,
            "file_size_bytes": 1024,
            "checksum": "abc",
            "created_at": "2024-01-01T00:00:00Z",
        }])

        result = get_last_successful_run("test")
        assert result is not None
        assert result["id"] == 10
        assert result["row_count"] == 50

    @patch("ingestion.metadata_logger.fetch_dataframe")
    def test_returns_none_when_not_found(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()

        result = get_last_successful_run("nonexistent")
        assert result is None
