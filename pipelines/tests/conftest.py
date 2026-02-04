"""Pytest configuration and shared fixtures for pipeline tests."""

import os
import sys
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
