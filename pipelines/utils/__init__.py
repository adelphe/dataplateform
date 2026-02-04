"""Shared utility functions for the data platform."""

from pipelines.utils.minio_client import get_minio_client, upload_file, download_file, list_objects, delete_object
from pipelines.utils.postgres_client import get_postgres_connection, execute_query, fetch_dataframe, bulk_insert
from pipelines.utils.logging_config import get_logger, setup_logging

__all__ = [
    "get_minio_client",
    "upload_file",
    "download_file",
    "list_objects",
    "delete_object",
    "get_postgres_connection",
    "execute_query",
    "fetch_dataframe",
    "bulk_insert",
    "get_logger",
    "setup_logging",
]
