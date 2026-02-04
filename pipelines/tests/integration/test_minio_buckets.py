"""Integration tests for MinIO bucket operations.

These tests require a running MinIO instance. They verify bucket creation,
data upload/download, listing, and data promotion across layers.

Run with:
    pytest pipelines/tests/integration/test_minio_buckets.py -v

Skip if MinIO is not available:
    pytest pipelines/tests/integration/ -v -k "not integration"
"""

import json
import os
import tempfile

import pytest

from utils.minio_client import (
    get_minio_client,
    bucket_exists,
    create_bucket,
    list_buckets,
    upload_file,
    download_file,
    list_objects,
    delete_object,
    copy_object,
)
from utils.storage import (
    StorageLayer,
    upload_to_layer,
    download_from_layer,
    list_layer_objects,
    promote_data,
)


def minio_available():
    """Check if MinIO is reachable."""
    try:
        client = get_minio_client()
        client.list_buckets()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not minio_available(), reason="MinIO is not available"
)

TEST_PREFIX = "_integration_test/"


@pytest.fixture(autouse=True)
def cleanup_test_objects():
    """Remove test objects after each test."""
    yield
    # Cleanup: remove all objects with the test prefix from all buckets
    client = get_minio_client()
    for bucket_name in ["raw", "staging", "curated"]:
        try:
            objects = list_objects(bucket_name, prefix=TEST_PREFIX)
            for obj in objects:
                delete_object(bucket_name, obj["Key"])
        except Exception:
            pass


class TestBucketExistence:
    """Verify that medallion buckets exist after initialization."""

    def test_raw_bucket_exists(self):
        assert bucket_exists("raw")

    def test_staging_bucket_exists(self):
        assert bucket_exists("staging")

    def test_curated_bucket_exists(self):
        assert bucket_exists("curated")

    def test_list_buckets_includes_all(self):
        buckets = list_buckets()
        bucket_names = {b["Name"] for b in buckets}
        assert "raw" in bucket_names
        assert "staging" in bucket_names
        assert "curated" in bucket_names


class TestUploadDownload:
    """Test uploading and downloading files to each layer."""

    def test_upload_and_download_to_raw(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
            upload_path = f.name

        try:
            object_key = f"{TEST_PREFIX}test_users.csv"
            upload_file(upload_path, "raw", object_key)

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
                download_path = f.name

            download_file("raw", object_key, download_path)

            with open(download_path) as f:
                content = f.read()
            assert "Alice" in content
            assert "Bob" in content
        finally:
            os.unlink(upload_path)
            os.unlink(download_path)

    def test_upload_to_layer_bronze(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"test": True}, f)
            upload_path = f.name

        try:
            object_key = f"{TEST_PREFIX}test_data.json"
            uri = upload_to_layer(upload_path, StorageLayer.BRONZE, object_key)
            assert uri == f"s3://raw/{object_key}"
        finally:
            os.unlink(upload_path)

    def test_download_from_layer_silver(self):
        # Upload first
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2\na,b\n")
            upload_path = f.name

        try:
            object_key = f"{TEST_PREFIX}test_clean.csv"
            upload_file(upload_path, "staging", object_key)

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
                download_path = f.name

            download_from_layer(StorageLayer.SILVER, object_key, download_path)

            with open(download_path) as f:
                content = f.read()
            assert "col1" in content
        finally:
            os.unlink(upload_path)
            os.unlink(download_path)


class TestListObjects:
    """Test listing objects with prefix filtering."""

    def test_list_objects_with_prefix(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test content")
            upload_path = f.name

        try:
            upload_file(upload_path, "raw", f"{TEST_PREFIX}a.txt")
            upload_file(upload_path, "raw", f"{TEST_PREFIX}b.txt")

            objects = list_objects("raw", prefix=TEST_PREFIX)
            keys = [o["Key"] for o in objects]
            assert f"{TEST_PREFIX}a.txt" in keys
            assert f"{TEST_PREFIX}b.txt" in keys
        finally:
            os.unlink(upload_path)

    def test_list_layer_objects(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("data")
            upload_path = f.name

        try:
            upload_file(upload_path, "staging", f"{TEST_PREFIX}item.txt")

            objects = list_layer_objects(StorageLayer.SILVER, prefix=TEST_PREFIX)
            assert len(objects) >= 1
        finally:
            os.unlink(upload_path)


class TestDataPromotion:
    """Test promoting data between layers."""

    def test_promote_bronze_to_silver(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,value\n1,100\n")
            upload_path = f.name

        try:
            source_key = f"{TEST_PREFIX}promote_source.csv"
            upload_file(upload_path, "raw", source_key)

            target_key = f"{TEST_PREFIX}promote_dest.csv"
            uri = promote_data(source_key, StorageLayer.BRONZE, StorageLayer.SILVER, target_key)

            assert uri == f"s3://staging/{target_key}"

            # Verify the file exists in silver
            assert list_objects("staging", prefix=target_key)

            # Cleanup target
            delete_object("staging", target_key)
        finally:
            os.unlink(upload_path)

    def test_promote_silver_to_gold(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("metric,value\nrevenue,5000\n")
            upload_path = f.name

        try:
            source_key = f"{TEST_PREFIX}metrics.csv"
            upload_file(upload_path, "staging", source_key)

            target_key = f"{TEST_PREFIX}report_metrics.csv"
            uri = promote_data(source_key, StorageLayer.SILVER, StorageLayer.GOLD, target_key)

            assert uri == f"s3://curated/{target_key}"
            assert list_objects("curated", prefix=target_key)

            # Cleanup target
            delete_object("curated", target_key)
        finally:
            os.unlink(upload_path)
