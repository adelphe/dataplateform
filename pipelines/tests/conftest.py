"""Pytest configuration and shared fixtures for pipeline tests."""

import csv
import io
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Ensure pipelines package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_airflow_context():
    """Create a mock Airflow task context."""
    ti = MagicMock()
    ti.xcom_pull.return_value = None
    ti.xcom_push.return_value = None

    return {
        "ds": "2024-01-01",
        "ds_nodash": "20240101",
        "execution_date": "2024-01-01T00:00:00+00:00",
        "dag": MagicMock(dag_id="test_dag"),
        "task_instance": ti,
        "ti": ti,
        "params": {},
    }


@pytest.fixture
def mock_postgres_hook():
    """Create a mock PostgresHook."""
    with patch("airflow.providers.postgres.hooks.postgres.PostgresHook") as mock_cls:
        hook = MagicMock()
        mock_cls.return_value = hook
        yield hook


@pytest.fixture
def mock_boto3_client():
    """Create a mock boto3 S3 client configured for MinIO."""
    with patch("boto3.client") as mock_client_fn:
        client = MagicMock()
        mock_client_fn.return_value = client
        yield client


@pytest.fixture
def minio_env():
    """Set MinIO environment variables for testing."""
    env_vars = {
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def postgres_env():
    """Set PostgreSQL environment variables for testing."""
    env_vars = {
        "POSTGRES_USER": "test-user",
        "POSTGRES_PASSWORD": "test-password",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "test-db",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_minio_client():
    """Create a mock MinIO/S3 client with common operations stubbed."""
    import utils.minio_client as mc_mod

    client = MagicMock()
    client.list_buckets.return_value = {
        "Buckets": [
            {"Name": "raw", "CreationDate": "2024-01-01T00:00:00Z"},
            {"Name": "staging", "CreationDate": "2024-01-01T00:00:00Z"},
            {"Name": "curated", "CreationDate": "2024-01-01T00:00:00Z"},
        ]
    }
    client.head_bucket.return_value = {}
    client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}

    original = mc_mod._client
    mc_mod._client = client
    yield client
    mc_mod._client = original


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email"])
        writer.writerow([1, "Alice", "alice@example.com"])
        writer.writerow([2, "Bob", "bob@example.com"])
        path = f.name
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def sample_json_file():
    """Create a temporary JSON file with sample data."""
    data = [
        {"id": 1, "amount": 100.0, "timestamp": "2024-01-01T00:00:00Z"},
        {"id": 2, "amount": 250.5, "timestamp": "2024-01-02T00:00:00Z"},
    ]
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(data, f)
        path = f.name
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def sample_parquet_file():
    """Create a temporary Parquet file with sample data."""
    try:
        import pandas as pd

        df = pd.DataFrame(
            {
                "product_id": [1, 2, 3],
                "name": ["Widget", "Gadget", "Doohickey"],
                "price": [9.99, 24.99, 4.99],
            }
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        df.to_parquet(path, index=False)
        yield path
        if os.path.exists(path):
            os.unlink(path)
    except ImportError:
        pytest.skip("pandas/pyarrow not available")
