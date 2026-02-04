"""Tests for the database ingestor module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.metadata_logger import IngestionError


class TestExtractTableFull:
    """Tests for extract_table_full."""

    @patch("ingestion.database_ingestor.upload_to_layer")
    @patch("ingestion.database_ingestor.log_ingestion_success")
    @patch("ingestion.database_ingestor.log_ingestion_start")
    @patch("ingestion.database_ingestor.log_schema_change")
    @patch("ingestion.database_ingestor.detect_postgres_schema")
    @patch("ingestion.database_ingestor.fetch_dataframe")
    def test_extracts_full_table(
        self,
        mock_fetch,
        mock_detect,
        mock_log_schema,
        mock_start,
        mock_success,
        mock_upload,
    ):
        mock_fetch.return_value = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        mock_detect.return_value = {"format": "postgres", "columns": []}
        mock_start.return_value = 1
        mock_upload.return_value = "s3://raw/postgres/test.parquet"

        from ingestion.database_ingestor import extract_table_full

        result = extract_table_full("my_table", "public", "postgres/test.parquet", "2024-01-01")

        assert result["uri"] == "s3://raw/postgres/test.parquet"
        assert result["row_count"] == 2
        assert "schema" in result
        mock_fetch.assert_called_once()
        mock_success.assert_called_once()

    @patch("ingestion.database_ingestor.log_ingestion_failure")
    @patch("ingestion.database_ingestor.log_ingestion_start")
    @patch("ingestion.database_ingestor.detect_postgres_schema")
    @patch("ingestion.database_ingestor.fetch_dataframe")
    def test_logs_failure_on_error(
        self, mock_fetch, mock_detect, mock_start, mock_failure
    ):
        mock_detect.return_value = {"format": "postgres", "columns": []}
        mock_start.return_value = 1
        mock_fetch.side_effect = Exception("connection lost")

        from ingestion.database_ingestor import extract_table_full

        with pytest.raises(IngestionError, match="Failed full extraction"):
            extract_table_full("bad_table", "public", "key", "2024-01-01")

        mock_failure.assert_called_once()


class TestExtractTableIncremental:
    """Tests for extract_table_incremental."""

    @patch("ingestion.database_ingestor.update_watermark")
    @patch("ingestion.database_ingestor.get_watermark")
    @patch("ingestion.database_ingestor.upload_to_layer")
    @patch("ingestion.database_ingestor.log_ingestion_success")
    @patch("ingestion.database_ingestor.log_ingestion_start")
    @patch("ingestion.database_ingestor.log_schema_change")
    @patch("ingestion.database_ingestor.detect_postgres_schema")
    @patch("ingestion.database_ingestor.fetch_dataframe")
    def test_extracts_incrementally(
        self,
        mock_fetch,
        mock_detect,
        mock_log_schema,
        mock_start,
        mock_success,
        mock_upload,
        mock_get_wm,
        mock_update_wm,
    ):
        mock_fetch.return_value = pd.DataFrame({
            "id": [3, 4],
            "updated_at": ["2024-01-15", "2024-01-16"],
        })
        mock_detect.return_value = {"format": "postgres", "columns": []}
        mock_start.return_value = 1
        mock_get_wm.return_value = "2024-01-14"
        mock_upload.return_value = "s3://raw/postgres/incr.parquet"

        from ingestion.database_ingestor import extract_table_incremental

        result = extract_table_incremental(
            "my_table", "public", "updated_at", "postgres/incr.parquet", "2024-01-01"
        )

        assert result["row_count"] == 2
        assert result["watermark"] == "2024-01-16"
        mock_update_wm.assert_called_once()

    @patch("ingestion.database_ingestor.get_watermark")
    @patch("ingestion.database_ingestor.upload_to_layer")
    @patch("ingestion.database_ingestor.log_ingestion_success")
    @patch("ingestion.database_ingestor.log_ingestion_start")
    @patch("ingestion.database_ingestor.log_schema_change")
    @patch("ingestion.database_ingestor.detect_postgres_schema")
    @patch("ingestion.database_ingestor.fetch_dataframe")
    def test_returns_zero_rows_when_nothing_new(
        self,
        mock_fetch,
        mock_detect,
        mock_log_schema,
        mock_start,
        mock_success,
        mock_upload,
        mock_get_wm,
    ):
        mock_fetch.return_value = pd.DataFrame()
        mock_detect.return_value = {"format": "postgres", "columns": []}
        mock_start.return_value = 1
        mock_get_wm.return_value = "2024-01-15"

        from ingestion.database_ingestor import extract_table_incremental

        result = extract_table_incremental(
            "my_table", "public", "updated_at", "key", "2024-01-01"
        )

        assert result["row_count"] == 0
        assert result["uri"] is None


class TestExtractWithCustomQuery:
    """Tests for extract_with_custom_query."""

    @patch("ingestion.database_ingestor.upload_to_layer")
    @patch("ingestion.database_ingestor.log_ingestion_success")
    @patch("ingestion.database_ingestor.log_ingestion_start")
    @patch("ingestion.database_ingestor.fetch_dataframe")
    def test_extracts_custom_query(
        self, mock_fetch, mock_start, mock_success, mock_upload
    ):
        mock_fetch.return_value = pd.DataFrame({"x": [1, 2, 3]})
        mock_start.return_value = 1
        mock_upload.return_value = "s3://raw/custom/result.parquet"

        from ingestion.database_ingestor import extract_with_custom_query

        result = extract_with_custom_query(
            "SELECT x FROM t", "custom/result.parquet", "2024-01-01"
        )

        assert result["row_count"] == 3
        mock_success.assert_called_once()
