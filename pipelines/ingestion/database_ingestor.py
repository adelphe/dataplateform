"""Database ingestion module for the data platform.

Extracts data from PostgreSQL tables (full or incremental) and uploads
the results as Parquet files to the MinIO Bronze layer. Supports
watermark-based incremental loading and custom SQL queries.
"""

import logging
import os
import tempfile
from typing import Any, Dict, Optional

import pandas as pd

from .metadata_logger import (
    compute_file_checksum,
    log_ingestion_failure,
    log_ingestion_start,
    log_ingestion_success,
    IngestionError,
)
from .schema_detector import detect_postgres_schema, log_schema_change
from .watermark_manager import (
    build_incremental_query,
    get_watermark,
    update_watermark,
)
from utils.postgres_client import fetch_dataframe
from utils.storage import StorageLayer, upload_to_layer

log = logging.getLogger(__name__)


def _upload_dataframe(
    df: pd.DataFrame,
    object_key: str,
    source_name: str,
    source_type: str,
    execution_date: str,
    run_id: int,
) -> Dict[str, Any]:
    """Convert a DataFrame to Parquet, upload to Bronze, and log metadata.

    Args:
        df: DataFrame to upload.
        object_key: Target object key in the Bronze bucket.
        source_name: Source identifier for metadata.
        source_type: Source type string for metadata.
        execution_date: Airflow execution date.
        run_id: Metadata logger run id.

    Returns:
        Result dict with upload details.
    """
    parquet_key = object_key
    if not parquet_key.endswith(".parquet"):
        parquet_key = os.path.splitext(parquet_key)[0] + ".parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    df.to_parquet(tmp_path, index=False)

    file_size = os.path.getsize(tmp_path)
    checksum = compute_file_checksum(tmp_path)
    row_count = len(df)

    uri = upload_to_layer(
        tmp_path,
        StorageLayer.BRONZE,
        parquet_key,
        metadata={
            "source": source_name,
            "source_type": source_type,
            "row_count": str(row_count),
            "execution_date": execution_date,
        },
    )
    os.unlink(tmp_path)

    log_ingestion_success(run_id, row_count, file_size, checksum)

    return {
        "uri": uri,
        "row_count": row_count,
        "file_size": file_size,
        "checksum": checksum,
    }


def extract_table_full(
    table_name: str,
    schema: str,
    object_key: str,
    execution_date: str,
) -> Dict[str, Any]:
    """Extract a full snapshot of a PostgreSQL table into the Bronze layer.

    Args:
        table_name: Source table name.
        schema: Source schema name.
        object_key: Target object key in the Bronze bucket.
        execution_date: Airflow execution date string.

    Returns:
        Dict with ``uri``, ``row_count``, ``file_size``, ``checksum``,
        and ``schema`` keys.

    Raises:
        IngestionError: If extraction or upload fails.
    """
    source_name = f"{schema}.{table_name}"
    run_id = log_ingestion_start(source_name, "postgres", execution_date)

    try:
        # Detect schema
        table_schema = detect_postgres_schema(table_name, schema)
        log_schema_change(source_name, table_schema, execution_date)

        # Extract full table
        sql = f'SELECT * FROM {schema}."{table_name}"'
        df = fetch_dataframe(sql)

        result = _upload_dataframe(df, object_key, source_name, "postgres", execution_date, run_id)
        result["schema"] = table_schema

        log.info("Full extraction complete: %s -> %s (%d rows)",
                 source_name, result["uri"], result["row_count"])
        return result

    except Exception as exc:
        log_ingestion_failure(run_id, str(exc))
        raise IngestionError(
            f"Failed full extraction of '{source_name}': {exc}"
        ) from exc


def extract_table_incremental(
    table_name: str,
    schema: str,
    watermark_column: str,
    object_key: str,
    execution_date: str,
    watermark_type: str = "timestamp",
) -> Dict[str, Any]:
    """Extract incremental data from a PostgreSQL table using watermarking.

    Retrieves only rows where ``watermark_column`` is greater than the
    last recorded watermark value. On the first run (no watermark), a
    full extract is performed.

    Args:
        table_name: Source table name.
        schema: Source schema name.
        watermark_column: Column to use as the high-water mark.
        object_key: Target object key in the Bronze bucket.
        execution_date: Airflow execution date string.
        watermark_type: Type of watermark (``timestamp`` or ``integer``).

    Returns:
        Dict with ``uri``, ``row_count``, ``file_size``, ``checksum``,
        ``schema``, and ``watermark`` keys.

    Raises:
        IngestionError: If extraction or upload fails.
    """
    source_name = f"{schema}.{table_name}"
    run_id = log_ingestion_start(source_name, "postgres", execution_date)

    try:
        # Detect schema
        table_schema = detect_postgres_schema(table_name, schema)
        log_schema_change(source_name, table_schema, execution_date)

        # Get watermark and build query
        last_watermark = get_watermark(source_name, watermark_column)
        base_query = f'SELECT * FROM {schema}."{table_name}"'
        sql = build_incremental_query(base_query, watermark_column, last_watermark)

        df = fetch_dataframe(sql)

        if df.empty:
            log.info("No new rows found for '%s' since watermark '%s'",
                     source_name, last_watermark)
            log_ingestion_success(run_id, 0, 0, "")
            return {
                "uri": None,
                "row_count": 0,
                "file_size": 0,
                "checksum": "",
                "schema": table_schema,
                "watermark": str(last_watermark),
            }

        result = _upload_dataframe(df, object_key, source_name, "postgres", execution_date, run_id)
        result["schema"] = table_schema

        # Determine new watermark value from extracted data
        new_watermark = str(df[watermark_column].max())
        update_watermark(source_name, watermark_column, new_watermark, watermark_type)
        result["watermark"] = new_watermark

        log.info("Incremental extraction complete: %s -> %s (%d rows, watermark=%s)",
                 source_name, result["uri"], result["row_count"], new_watermark)
        return result

    except Exception as exc:
        log_ingestion_failure(run_id, str(exc))
        raise IngestionError(
            f"Failed incremental extraction of '{source_name}': {exc}"
        ) from exc


def extract_with_custom_query(
    query: str,
    object_key: str,
    execution_date: str,
    source_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Extract data using a custom SQL query and upload to the Bronze layer.

    Args:
        query: SQL SELECT query to execute.
        object_key: Target object key in the Bronze bucket.
        execution_date: Airflow execution date string.
        source_name: Optional source identifier for metadata.
            Defaults to a truncated version of the query.

    Returns:
        Dict with ``uri``, ``row_count``, ``file_size``, and ``checksum`` keys.

    Raises:
        IngestionError: If extraction or upload fails.
    """
    if source_name is None:
        source_name = f"custom_query:{query[:80]}"

    run_id = log_ingestion_start(source_name, "postgres", execution_date)

    try:
        df = fetch_dataframe(query)
        result = _upload_dataframe(df, object_key, source_name, "postgres", execution_date, run_id)

        log.info("Custom query extraction complete: %s (%d rows)",
                 result["uri"], result["row_count"])
        return result

    except Exception as exc:
        log_ingestion_failure(run_id, str(exc))
        raise IngestionError(
            f"Failed custom query extraction: {exc}"
        ) from exc
