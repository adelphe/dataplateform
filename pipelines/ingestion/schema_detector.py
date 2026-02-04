"""Schema detection utilities for the data platform.

Detects and logs schema information from CSV files, JSON files, and
PostgreSQL tables. Returns standardized schema metadata and tracks
schema changes over time in a PostgreSQL tracking table.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.postgres_client import execute_query, fetch_dataframe

log = logging.getLogger(__name__)


class SchemaDetectionError(Exception):
    """Raised when schema detection fails."""


# Mapping from pandas/numpy dtype names to portable type strings
_DTYPE_MAP = {
    "int64": "integer",
    "int32": "integer",
    "float64": "float",
    "float32": "float",
    "object": "string",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
    "datetime64[ns, UTC]": "timestamp",
    "category": "string",
}


def _portable_type(dtype) -> str:
    """Convert a pandas dtype to a portable type string."""
    name = str(dtype)
    return _DTYPE_MAP.get(name, "string")


def detect_csv_schema(file_path: str) -> Dict[str, Any]:
    """Detect schema from a CSV file using pandas DataFrame inspection.

    Args:
        file_path: Path to the CSV file.

    Returns:
        Dict with keys ``columns`` (list of column dicts) and ``row_count``.

    Raises:
        SchemaDetectionError: If the file cannot be read or parsed.
    """
    try:
        df = pd.read_csv(file_path, nrows=1000)
    except Exception as exc:
        raise SchemaDetectionError(f"Failed to read CSV file '{file_path}': {exc}") from exc

    columns: List[Dict[str, Any]] = []
    for col in df.columns:
        columns.append({
            "name": str(col),
            "type": _portable_type(df[col].dtype),
            "nullable": bool(df[col].isnull().any()),
        })

    schema = {
        "format": "csv",
        "columns": columns,
        "row_count": len(df),
    }
    log.info("Detected CSV schema for '%s': %d columns, %d sample rows",
             file_path, len(columns), len(df))
    return schema


def detect_json_schema(file_path: str, sample_size: int = 100) -> Dict[str, Any]:
    """Detect schema from a JSON file by inferring types from sample records.

    Expects the file to contain either a JSON array of objects or
    newline-delimited JSON objects.

    Args:
        file_path: Path to the JSON file.
        sample_size: Maximum number of records to sample for type inference.

    Returns:
        Dict with keys ``columns`` (list of column dicts) and ``row_count``.

    Raises:
        SchemaDetectionError: If the file cannot be read or parsed.
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        # Try newline-delimited JSON
        try:
            data = []
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data.append(json.loads(line))
        except Exception as exc:
            raise SchemaDetectionError(
                f"Failed to parse JSON file '{file_path}': {exc}"
            ) from exc
    except Exception as exc:
        raise SchemaDetectionError(f"Failed to read JSON file '{file_path}': {exc}") from exc

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list) or len(data) == 0:
        raise SchemaDetectionError(f"JSON file '{file_path}' contains no records")

    sample = data[:sample_size]
    df = pd.DataFrame(sample)

    columns: List[Dict[str, Any]] = []
    for col in df.columns:
        columns.append({
            "name": str(col),
            "type": _portable_type(df[col].dtype),
            "nullable": bool(df[col].isnull().any()),
        })

    schema = {
        "format": "json",
        "columns": columns,
        "row_count": len(data),
    }
    log.info("Detected JSON schema for '%s': %d columns, %d total records",
             file_path, len(columns), len(data))
    return schema


def detect_postgres_schema(
    table_name: str,
    schema: str = "public",
    conn_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Query PostgreSQL information_schema to extract table schema.

    Args:
        table_name: Name of the table.
        schema: Database schema name.
        conn_id: Unused placeholder for Airflow connection compatibility.
            The function uses ``postgres_client`` which reads from env vars.

    Returns:
        Dict with keys ``columns`` (list of column dicts), ``table_name``,
        and ``schema``.

    Raises:
        SchemaDetectionError: If the table does not exist or query fails.
    """
    sql = """
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_name = :table_name AND table_schema = :schema
        ORDER BY ordinal_position
    """
    try:
        df = fetch_dataframe(sql, params={"table_name": table_name, "schema": schema})
    except Exception as exc:
        raise SchemaDetectionError(
            f"Failed to query schema for '{schema}.{table_name}': {exc}"
        ) from exc

    if df.empty:
        raise SchemaDetectionError(
            f"Table '{schema}.{table_name}' not found or has no columns"
        )

    columns: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        columns.append({
            "name": row["column_name"],
            "type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
        })

    result = {
        "format": "postgres",
        "table_name": table_name,
        "schema": schema,
        "columns": columns,
    }
    log.info("Detected PostgreSQL schema for '%s.%s': %d columns",
             schema, table_name, len(columns))
    return result


def log_schema_change(
    source: str,
    schema_metadata: Dict[str, Any],
    execution_date: str,
) -> None:
    """Log schema metadata to the data_platform.schema_history table.

    Args:
        source: Identifier for the data source (e.g. file path or table name).
        schema_metadata: Schema dict as returned by detect_* functions.
        execution_date: Airflow execution date string.
    """
    sql = """
        INSERT INTO data_platform.schema_history
            (source_name, schema_metadata, execution_date, created_at)
        VALUES
            (:source_name, :schema_metadata, :execution_date, :created_at)
    """
    try:
        execute_query(sql, params={
            "source_name": source,
            "schema_metadata": json.dumps(schema_metadata),
            "execution_date": execution_date,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        log.info("Logged schema change for source '%s'", source)
    except Exception:
        log.warning("Failed to log schema change for '%s' (table may not exist yet)", source)
