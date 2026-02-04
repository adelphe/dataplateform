"""Ingestion metadata logging for the data platform.

Tracks ingestion runs in a PostgreSQL table with row counts, file sizes,
checksums, and status. Provides functions to log start, success, and
failure of ingestion tasks.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from utils.postgres_client import execute_query, fetch_dataframe

log = logging.getLogger(__name__)


class IngestionError(Exception):
    """Raised when an ingestion operation fails."""


_METADATA_TABLE = "data_platform.ingestion_metadata"

_CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS data_platform;

    CREATE TABLE IF NOT EXISTS data_platform.ingestion_metadata (
        id              SERIAL PRIMARY KEY,
        source_name     VARCHAR(512) NOT NULL,
        source_type     VARCHAR(64)  NOT NULL,
        execution_date  VARCHAR(64)  NOT NULL,
        row_count       INTEGER,
        file_size_bytes BIGINT,
        checksum        VARCHAR(128),
        status          VARCHAR(32)  NOT NULL DEFAULT 'running',
        error_message   TEXT,
        created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
"""


def ensure_metadata_tables() -> None:
    """Create the ingestion_metadata table if it does not exist."""
    try:
        execute_query(_CREATE_TABLE_SQL)
        log.info("Ensured ingestion_metadata table exists")
    except Exception as exc:
        log.error("Failed to create ingestion_metadata table: %s", exc)
        raise


def compute_file_checksum(file_path: str, algorithm: str = "md5") -> str:
    """Compute a hex-digest checksum for a file.

    Args:
        file_path: Path to the file.
        algorithm: Hash algorithm (``md5`` or ``sha256``).

    Returns:
        Hex-encoded checksum string.
    """
    h = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def log_ingestion_start(
    source_name: str,
    source_type: str,
    execution_date: str,
) -> int:
    """Log the start of an ingestion run.

    Args:
        source_name: Identifier for the data source.
        source_type: Type of source (``csv``, ``json``, ``postgres``).
        execution_date: Airflow execution date string.

    Returns:
        The ``id`` of the newly created metadata row.
    """
    sql = f"""
        INSERT INTO {_METADATA_TABLE}
            (source_name, source_type, execution_date, status, created_at, updated_at)
        VALUES
            (:source_name, :source_type, :execution_date, 'running', :now, :now)
        RETURNING id
    """
    now = datetime.now(timezone.utc).isoformat()
    result = execute_query(sql, params={
        "source_name": source_name,
        "source_type": source_type,
        "execution_date": execution_date,
        "now": now,
    })
    row = result.fetchone()
    run_id = row[0]
    log.info("Ingestion started: run_id=%d source='%s' type='%s'",
             run_id, source_name, source_type)
    return run_id


def log_ingestion_success(
    run_id: int,
    row_count: int,
    file_size: int,
    checksum: str,
) -> None:
    """Log the successful completion of an ingestion run.

    Args:
        run_id: The metadata row id from ``log_ingestion_start``.
        row_count: Number of rows ingested.
        file_size: Size of the ingested file in bytes.
        checksum: File checksum string.
    """
    sql = f"""
        UPDATE {_METADATA_TABLE}
        SET status = 'success',
            row_count = :row_count,
            file_size_bytes = :file_size,
            checksum = :checksum,
            updated_at = :now
        WHERE id = :run_id
    """
    execute_query(sql, params={
        "run_id": run_id,
        "row_count": row_count,
        "file_size": file_size,
        "checksum": checksum,
        "now": datetime.now(timezone.utc).isoformat(),
    })
    log.info("Ingestion succeeded: run_id=%d rows=%d size=%d",
             run_id, row_count, file_size)


def log_ingestion_failure(run_id: int, error_message: str) -> None:
    """Log the failure of an ingestion run.

    Args:
        run_id: The metadata row id from ``log_ingestion_start``.
        error_message: Description of the error.
    """
    sql = f"""
        UPDATE {_METADATA_TABLE}
        SET status = 'failed',
            error_message = :error_message,
            updated_at = :now
        WHERE id = :run_id
    """
    execute_query(sql, params={
        "run_id": run_id,
        "error_message": error_message,
        "now": datetime.now(timezone.utc).isoformat(),
    })
    log.error("Ingestion failed: run_id=%d error='%s'", run_id, error_message)


def get_last_successful_run(source_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve the most recent successful ingestion run for a source.

    Args:
        source_name: Identifier for the data source.

    Returns:
        Dict with run metadata, or ``None`` if no successful run exists.
    """
    sql = f"""
        SELECT id, source_name, source_type, execution_date,
               row_count, file_size_bytes, checksum, created_at
        FROM {_METADATA_TABLE}
        WHERE source_name = :source_name AND status = 'success'
        ORDER BY created_at DESC
        LIMIT 1
    """
    df = fetch_dataframe(sql, params={"source_name": source_name})
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    log.info("Last successful run for '%s': id=%s", source_name, row.get("id"))
    return row
