"""Watermark management for incremental data loading.

Manages high-water marks in a PostgreSQL table to support incremental
extraction. Supports both timestamp-based and integer-based watermarks.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from utils.postgres_client import execute_query, fetch_dataframe

log = logging.getLogger(__name__)


class WatermarkError(Exception):
    """Raised when watermark operations fail."""


_WATERMARK_TABLE = "data_platform.watermarks"

_CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS data_platform;

    CREATE TABLE IF NOT EXISTS data_platform.watermarks (
        source_name      VARCHAR(512) NOT NULL,
        watermark_column VARCHAR(256) NOT NULL,
        watermark_value  VARCHAR(512),
        watermark_type   VARCHAR(32)  NOT NULL DEFAULT 'timestamp',
        updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        PRIMARY KEY (source_name, watermark_column)
    );
"""


def ensure_watermark_table() -> None:
    """Create the watermarks table if it does not exist."""
    try:
        execute_query(_CREATE_TABLE_SQL)
        log.info("Ensured watermarks table exists")
    except Exception as exc:
        log.error("Failed to create watermarks table: %s", exc)
        raise


def get_watermark(
    source_name: str,
    watermark_column: str,
) -> Optional[Any]:
    """Retrieve the last watermark value for a source.

    Args:
        source_name: Identifier for the data source.
        watermark_column: Column used as the watermark.

    Returns:
        The stored watermark value (as a string), or ``None`` if no
        watermark exists (indicating a full/initial load is needed).
    """
    sql = f"""
        SELECT watermark_value, watermark_type
        FROM {_WATERMARK_TABLE}
        WHERE source_name = :source_name AND watermark_column = :watermark_column
    """
    try:
        df = fetch_dataframe(sql, params={
            "source_name": source_name,
            "watermark_column": watermark_column,
        })
    except Exception as exc:
        raise WatermarkError(
            f"Failed to query watermark for '{source_name}.{watermark_column}': {exc}"
        ) from exc

    if df.empty:
        log.info("No watermark found for '%s.%s' (initial load)",
                 source_name, watermark_column)
        return None

    value = df.iloc[0]["watermark_value"]
    wtype = df.iloc[0]["watermark_type"]
    log.info("Retrieved watermark for '%s.%s': value=%s type=%s",
             source_name, watermark_column, value, wtype)
    return value


def update_watermark(
    source_name: str,
    watermark_column: str,
    value: Any,
    watermark_type: str = "timestamp",
) -> None:
    """Update (or insert) the watermark for a source after successful ingestion.

    Args:
        source_name: Identifier for the data source.
        watermark_column: Column used as the watermark.
        value: New watermark value.
        watermark_type: Type of watermark (``timestamp`` or ``integer``).
    """
    sql = f"""
        INSERT INTO {_WATERMARK_TABLE}
            (source_name, watermark_column, watermark_value, watermark_type, updated_at)
        VALUES
            (:source_name, :watermark_column, :value, :watermark_type, :now)
        ON CONFLICT (source_name, watermark_column) DO UPDATE
        SET watermark_value = :value,
            watermark_type  = :watermark_type,
            updated_at      = :now
    """
    try:
        execute_query(sql, params={
            "source_name": source_name,
            "watermark_column": watermark_column,
            "value": str(value),
            "watermark_type": watermark_type,
            "now": datetime.now(timezone.utc).isoformat(),
        })
        log.info("Updated watermark for '%s.%s' to '%s'",
                 source_name, watermark_column, value)
    except Exception as exc:
        raise WatermarkError(
            f"Failed to update watermark for '{source_name}.{watermark_column}': {exc}"
        ) from exc


def build_incremental_query(
    base_query: str,
    watermark_column: str,
    watermark_value: Optional[Any],
) -> str:
    """Append an incremental WHERE clause to a base SQL query.

    If ``watermark_value`` is ``None`` (initial load), returns the base
    query unchanged.

    Args:
        base_query: The SQL SELECT query without a trailing semicolon.
        watermark_column: Column to filter on.
        watermark_value: Last known watermark value.

    Returns:
        Modified SQL query string with the incremental filter applied.
    """
    if watermark_value is None:
        log.info("No watermark value provided; returning full query")
        return base_query

    # Determine if the base query already has a WHERE clause
    upper = base_query.upper()
    if "WHERE" in upper:
        connector = "AND"
    else:
        connector = "WHERE"

    incremental_query = (
        f"{base_query} {connector} {watermark_column} > '{watermark_value}'"
    )
    log.info("Built incremental query with watermark '%s' > '%s'",
             watermark_column, watermark_value)
    return incremental_query
