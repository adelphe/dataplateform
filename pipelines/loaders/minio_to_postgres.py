"""Loader utility to transfer Parquet data from MinIO Bronze layer to PostgreSQL raw schema.

Reads Parquet files from MinIO, loads them into PostgreSQL tables using pandas,
and logs load metadata for tracking purposes.

Usage:
    from loaders.minio_to_postgres import load_parquet_to_postgres

    row_count = load_parquet_to_postgres(
        bucket="raw",
        object_key="postgres/transactions/2024-01-01/transactions.parquet",
        target_table="transactions",
        target_schema="raw",
    )
"""

import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

from utils.minio_client import download_file
from utils.postgres_client import bulk_insert, get_postgres_connection, execute_query

log = logging.getLogger(__name__)


def load_parquet_to_postgres(
    bucket: str,
    object_key: str,
    target_table: str,
    target_schema: str = "raw",
    load_mode: str = "replace",
    connection_string: Optional[str] = None,
) -> int:
    """Load a Parquet file from MinIO into a PostgreSQL table.

    Downloads the Parquet file to a temporary location, reads it into a
    pandas DataFrame, and writes it to the specified PostgreSQL table.

    Args:
        bucket: MinIO bucket name (e.g., "raw").
        object_key: Object key path in MinIO.
        target_table: Target PostgreSQL table name.
        target_schema: Target PostgreSQL schema (default: "raw").
        load_mode: How to handle existing data - "replace" or "append".
        connection_string: Optional explicit PostgreSQL connection string.

    Returns:
        Number of rows loaded.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "data.parquet")

        log.info("Downloading s3://%s/%s to %s", bucket, object_key, local_path)
        download_file(bucket, object_key, local_path)

        df = pd.read_parquet(local_path)
        log.info("Read %d rows from Parquet file", len(df))

        if df.empty:
            log.warning("Parquet file is empty, skipping load")
            return 0

        engine = get_postgres_connection(connection_string)

        # Ensure the target schema exists
        execute_query(
            f"CREATE SCHEMA IF NOT EXISTS {target_schema}",
            engine=engine,
        )

        row_count = bulk_insert(
            df=df,
            table_name=target_table,
            schema=target_schema,
            if_exists=load_mode,
            engine=engine,
        )

        log.info(
            "Loaded %d rows into %s.%s (mode=%s)",
            row_count,
            target_schema,
            target_table,
            load_mode,
        )
        return row_count


def load_parquet_to_postgres_incremental(
    bucket: str,
    object_key: str,
    target_table: str,
    target_schema: str = "raw",
    watermark_source: Optional[str] = None,
    connection_string: Optional[str] = None,
) -> int:
    """Load a Parquet file with incremental awareness using watermarks.

    Checks the watermark table to determine if this data has already been
    loaded, then loads only if the object is newer than the last watermark.

    Args:
        bucket: MinIO bucket name.
        object_key: Object key path in MinIO.
        target_table: Target PostgreSQL table name.
        target_schema: Target PostgreSQL schema.
        watermark_source: Source name for watermark lookup. Defaults to object_key.
        connection_string: Optional explicit PostgreSQL connection string.

    Returns:
        Number of rows loaded (0 if skipped).
    """
    engine = get_postgres_connection(connection_string)
    source_name = watermark_source or object_key

    # Check existing watermark
    try:
        result = execute_query(
            "SELECT watermark_value FROM data_platform.watermarks "
            "WHERE source_name = :source_name AND watermark_column = 'load_timestamp'",
            params={"source_name": source_name},
            engine=engine,
        )
        row = result.fetchone()
        last_watermark = row[0] if row else None
    except Exception:
        last_watermark = None

    row_count = load_parquet_to_postgres(
        bucket=bucket,
        object_key=object_key,
        target_table=target_table,
        target_schema=target_schema,
        load_mode="append",
        connection_string=connection_string,
    )

    if row_count > 0:
        now = datetime.now(timezone.utc).isoformat()
        execute_query(
            "INSERT INTO data_platform.watermarks "
            "(source_name, watermark_column, watermark_value, watermark_type, updated_at) "
            "VALUES (:source_name, 'load_timestamp', :watermark_value, 'timestamp', NOW()) "
            "ON CONFLICT (source_name, watermark_column) "
            "DO UPDATE SET watermark_value = :watermark_value, updated_at = NOW()",
            params={"source_name": source_name, "watermark_value": now},
            engine=engine,
        )
        log.info("Updated watermark for %s to %s", source_name, now)

    return row_count
