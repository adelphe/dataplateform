"""Unit tests for the storage utility library."""

import os
from unittest.mock import MagicMock, patch, call

import pytest

from utils.storage import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    GOLD_BUCKET,
    StorageLayer,
    get_layer_path,
    upload_to_layer,
    download_from_layer,
    list_layer_objects,
    promote_data,
    _bucket_for_layer,
)


class TestStorageLayer:
    """Tests for the StorageLayer enum."""

    def test_bronze_value(self):
        assert StorageLayer.BRONZE.value == "raw"

    def test_silver_value(self):
        assert StorageLayer.SILVER.value == "staging"

    def test_gold_value(self):
        assert StorageLayer.GOLD.value == "curated"


class TestBucketForLayer:
    """Tests for _bucket_for_layer helper."""

    def test_bronze_returns_raw(self):
        assert _bucket_for_layer(StorageLayer.BRONZE) == BRONZE_BUCKET

    def test_silver_returns_staging(self):
        assert _bucket_for_layer(StorageLayer.SILVER) == SILVER_BUCKET

    def test_gold_returns_curated(self):
        assert _bucket_for_layer(StorageLayer.GOLD) == GOLD_BUCKET


class TestGetLayerPath:
    """Tests for get_layer_path."""

    def test_bronze_path(self):
        result = get_layer_path(StorageLayer.BRONZE, "postgres/users.csv")
        assert result == "s3://raw/postgres/users.csv"

    def test_silver_path(self):
        result = get_layer_path(StorageLayer.SILVER, "cleaned/users.parquet")
        assert result == "s3://staging/cleaned/users.parquet"

    def test_gold_path(self):
        result = get_layer_path(StorageLayer.GOLD, "aggregated/summary.parquet")
        assert result == "s3://curated/aggregated/summary.parquet"

    def test_nested_path(self):
        result = get_layer_path(StorageLayer.BRONZE, "api/weather/2024/01/15/data.json")
        assert result == "s3://raw/api/weather/2024/01/15/data.json"


class TestUploadToLayer:
    """Tests for upload_to_layer."""

    @patch("utils.storage.put_object_with_metadata")
    def test_upload_to_bronze(self, mock_put):
        mock_put.return_value = "postgres/users.csv"

        uri = upload_to_layer(
            "/tmp/users.csv", StorageLayer.BRONZE, "postgres/users.csv"
        )

        assert uri == "s3://raw/postgres/users.csv"
        mock_put.assert_called_once()
        args = mock_put.call_args
        assert args[0][0] == "/tmp/users.csv"
        assert args[0][1] == "raw"
        assert args[0][2] == "postgres/users.csv"

    @patch("utils.storage.put_object_with_metadata")
    def test_upload_attaches_metadata(self, mock_put):
        mock_put.return_value = "data.csv"

        upload_to_layer(
            "/tmp/data.csv",
            StorageLayer.SILVER,
            "cleaned/data.csv",
            metadata={"source_table": "orders"},
        )

        metadata = mock_put.call_args[0][3]
        assert "upload_timestamp" in metadata
        assert metadata["layer"] == "silver"
        assert metadata["source"] == "data.csv"
        assert metadata["source_table"] == "orders"

    @patch("utils.storage.put_object_with_metadata")
    def test_upload_to_gold(self, mock_put):
        mock_put.return_value = "report.parquet"

        uri = upload_to_layer(
            "/tmp/report.parquet", StorageLayer.GOLD, "reports/report.parquet"
        )

        assert uri == "s3://curated/reports/report.parquet"
        assert mock_put.call_args[0][1] == "curated"


class TestDownloadFromLayer:
    """Tests for download_from_layer."""

    @patch("utils.storage.download_file")
    def test_download_from_bronze(self, mock_dl):
        mock_dl.return_value = "/tmp/data.csv"

        result = download_from_layer(
            StorageLayer.BRONZE, "postgres/data.csv", "/tmp/data.csv"
        )

        assert result == "/tmp/data.csv"
        mock_dl.assert_called_once_with("raw", "postgres/data.csv", "/tmp/data.csv")

    @patch("utils.storage.download_file")
    def test_download_from_silver(self, mock_dl):
        mock_dl.return_value = "/tmp/clean.parquet"

        download_from_layer(
            StorageLayer.SILVER, "cleaned/data.parquet", "/tmp/clean.parquet"
        )

        mock_dl.assert_called_once_with("staging", "cleaned/data.parquet", "/tmp/clean.parquet")


class TestListLayerObjects:
    """Tests for list_layer_objects."""

    @patch("utils.storage.list_objects")
    def test_list_bronze_objects(self, mock_list):
        mock_list.return_value = [{"Key": "postgres/users.csv", "Size": 1024}]

        result = list_layer_objects(StorageLayer.BRONZE, prefix="postgres/")

        assert len(result) == 1
        mock_list.assert_called_once_with("raw", prefix="postgres/", max_keys=1000)

    @patch("utils.storage.list_objects")
    def test_list_with_custom_max_keys(self, mock_list):
        mock_list.return_value = []

        list_layer_objects(StorageLayer.GOLD, max_keys=50)

        mock_list.assert_called_once_with("curated", prefix=None, max_keys=50)


class TestPromoteData:
    """Tests for promote_data."""

    @patch("utils.storage.copy_object")
    def test_promote_bronze_to_silver(self, mock_copy):
        mock_copy.return_value = "postgres/users.csv"

        uri = promote_data(
            "postgres/users.csv", StorageLayer.BRONZE, StorageLayer.SILVER
        )

        assert uri == "s3://staging/postgres/users.csv"
        mock_copy.assert_called_once_with(
            "raw", "postgres/users.csv", "staging", "postgres/users.csv"
        )

    @patch("utils.storage.copy_object")
    def test_promote_with_different_target_key(self, mock_copy):
        mock_copy.return_value = "reports/users.parquet"

        uri = promote_data(
            "cleaned/users.parquet",
            StorageLayer.SILVER,
            StorageLayer.GOLD,
            target_key="reports/users.parquet",
        )

        assert uri == "s3://curated/reports/users.parquet"
        mock_copy.assert_called_once_with(
            "staging", "cleaned/users.parquet", "curated", "reports/users.parquet"
        )

    @patch("utils.storage.copy_object")
    def test_promote_silver_to_gold(self, mock_copy):
        mock_copy.return_value = "data.parquet"

        uri = promote_data(
            "data.parquet", StorageLayer.SILVER, StorageLayer.GOLD
        )

        assert uri == "s3://curated/data.parquet"
        mock_copy.assert_called_once_with(
            "staging", "data.parquet", "curated", "data.parquet"
        )
