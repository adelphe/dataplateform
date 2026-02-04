"""Tests for the watermark management module."""

from unittest.mock import patch

import pandas as pd
import pytest

from ingestion.watermark_manager import (
    WatermarkError,
    build_incremental_query,
    ensure_watermark_table,
    get_watermark,
    update_watermark,
)


class TestGetWatermark:
    """Tests for get_watermark."""

    @patch("ingestion.watermark_manager.fetch_dataframe")
    def test_returns_value_when_found(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame([{
            "watermark_value": "2024-01-15T00:00:00Z",
            "watermark_type": "timestamp",
        }])

        value = get_watermark("my_source", "updated_at")
        assert value == "2024-01-15T00:00:00Z"

    @patch("ingestion.watermark_manager.fetch_dataframe")
    def test_returns_none_when_not_found(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()

        value = get_watermark("new_source", "id")
        assert value is None

    @patch("ingestion.watermark_manager.fetch_dataframe")
    def test_raises_on_query_failure(self, mock_fetch):
        mock_fetch.side_effect = Exception("connection refused")

        with pytest.raises(WatermarkError, match="Failed to query watermark"):
            get_watermark("source", "col")


class TestUpdateWatermark:
    """Tests for update_watermark."""

    @patch("ingestion.watermark_manager.execute_query")
    def test_updates_successfully(self, mock_execute):
        update_watermark("source", "col", "2024-01-15", "timestamp")
        mock_execute.assert_called_once()
        params = mock_execute.call_args[1]["params"]
        assert params["source_name"] == "source"
        assert params["value"] == "2024-01-15"

    @patch("ingestion.watermark_manager.execute_query")
    def test_raises_on_failure(self, mock_execute):
        mock_execute.side_effect = Exception("write failed")

        with pytest.raises(WatermarkError, match="Failed to update watermark"):
            update_watermark("source", "col", "val", "timestamp")


class TestBuildIncrementalQuery:
    """Tests for build_incremental_query."""

    def test_no_watermark_returns_base(self):
        base = "SELECT * FROM my_table"
        result = build_incremental_query(base, "updated_at", None)
        assert result == base

    def test_adds_where_clause(self):
        base = "SELECT * FROM my_table"
        result = build_incremental_query(base, "updated_at", "2024-01-15")
        assert "WHERE updated_at > '2024-01-15'" in result

    def test_adds_and_clause_when_where_exists(self):
        base = "SELECT * FROM my_table WHERE active = true"
        result = build_incremental_query(base, "id", "100")
        assert "AND id > '100'" in result
        assert result.count("WHERE") == 1

    def test_preserves_base_query(self):
        base = "SELECT a, b FROM schema.table"
        result = build_incremental_query(base, "ts", "2024-01-01")
        assert result.startswith(base)


class TestEnsureWatermarkTable:
    """Tests for ensure_watermark_table."""

    @patch("ingestion.watermark_manager.execute_query")
    def test_calls_create_table(self, mock_execute):
        ensure_watermark_table()
        mock_execute.assert_called_once()
