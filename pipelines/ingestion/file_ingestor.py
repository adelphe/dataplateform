"""File-based ingestion module for the data platform.

Handles CSV and JSON file ingestion from the local filesystem into the
MinIO Bronze layer. Files are validated, converted to Parquet, and
accompanied by schema detection and metadata logging.
"""

import glob as globmod
import logging
import os
import tempfile
from typing import Any, Dict, List

import pandas as pd

from .metadata_logger import (
    compute_file_checksum,
    log_ingestion_failure,
    log_ingestion_start,
    log_ingestion_success,
    IngestionError,
)
from .schema_detector import detect_csv_schema, detect_json_schema, log_schema_change
from utils.storage import StorageLayer, upload_to_layer

log = logging.getLogger(__name__)


def ingest_csv_file(
    file_path: str,
    object_key: str,
    execution_date: str,
) -> Dict[str, Any]:
    """Ingest a single CSV file into the Bronze layer.

    The file is read, validated, converted to Parquet, and uploaded to
    MinIO. Schema detection and metadata logging are performed
    automatically.

    Args:
        file_path: Local path to the CSV file.
        object_key: Target object key in the Bronze bucket.
        execution_date: Airflow execution date string.

    Returns:
        Dict with ``uri``, ``row_count``, ``file_size``, ``checksum``,
        and ``schema`` keys.

    Raises:
        IngestionError: If the file cannot be read or uploaded.
    """
    source_name = os.path.basename(file_path)
    run_id = log_ingestion_start(source_name, "csv", execution_date)

    try:
        # Validate and detect schema
        schema = detect_csv_schema(file_path)
        log_schema_change(source_name, schema, execution_date)

        # Read full file
        df = pd.read_csv(file_path)
        row_count = len(df)

        # Convert to Parquet for efficient Bronze storage
        parquet_key = object_key
        if not parquet_key.endswith(".parquet"):
            parquet_key = os.path.splitext(parquet_key)[0] + ".parquet"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        df.to_parquet(tmp_path, index=False)

        # Compute checksum of the parquet file
        file_size = os.path.getsize(tmp_path)
        checksum = compute_file_checksum(tmp_path)

        # Upload to Bronze layer
        uri = upload_to_layer(
            tmp_path,
            StorageLayer.BRONZE,
            parquet_key,
            metadata={
                "source_file": source_name,
                "source_type": "csv",
                "row_count": str(row_count),
                "execution_date": execution_date,
            },
        )
        os.unlink(tmp_path)

        log_ingestion_success(run_id, row_count, file_size, checksum)

        result = {
            "uri": uri,
            "row_count": row_count,
            "file_size": file_size,
            "checksum": checksum,
            "schema": schema,
        }
        log.info("CSV ingestion complete: %s -> %s (%d rows)", file_path, uri, row_count)
        return result

    except Exception as exc:
        log_ingestion_failure(run_id, str(exc))
        raise IngestionError(f"Failed to ingest CSV file '{file_path}': {exc}") from exc


def ingest_json_file(
    file_path: str,
    object_key: str,
    execution_date: str,
) -> Dict[str, Any]:
    """Ingest a single JSON file into the Bronze layer.

    The file is read, validated, converted to Parquet, and uploaded to
    MinIO. Schema detection and metadata logging are performed
    automatically.

    Args:
        file_path: Local path to the JSON file.
        object_key: Target object key in the Bronze bucket.
        execution_date: Airflow execution date string.

    Returns:
        Dict with ``uri``, ``row_count``, ``file_size``, ``checksum``,
        and ``schema`` keys.

    Raises:
        IngestionError: If the file cannot be read or uploaded.
    """
    source_name = os.path.basename(file_path)
    run_id = log_ingestion_start(source_name, "json", execution_date)

    try:
        # Detect schema
        schema = detect_json_schema(file_path)
        log_schema_change(source_name, schema, execution_date)

        # Read full file into DataFrame
        df = pd.read_json(file_path)
        row_count = len(df)

        # Convert to Parquet
        parquet_key = object_key
        if not parquet_key.endswith(".parquet"):
            parquet_key = os.path.splitext(parquet_key)[0] + ".parquet"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        df.to_parquet(tmp_path, index=False)

        file_size = os.path.getsize(tmp_path)
        checksum = compute_file_checksum(tmp_path)

        uri = upload_to_layer(
            tmp_path,
            StorageLayer.BRONZE,
            parquet_key,
            metadata={
                "source_file": source_name,
                "source_type": "json",
                "row_count": str(row_count),
                "execution_date": execution_date,
            },
        )
        os.unlink(tmp_path)

        log_ingestion_success(run_id, row_count, file_size, checksum)

        result = {
            "uri": uri,
            "row_count": row_count,
            "file_size": file_size,
            "checksum": checksum,
            "schema": schema,
        }
        log.info("JSON ingestion complete: %s -> %s (%d rows)", file_path, uri, row_count)
        return result

    except Exception as exc:
        log_ingestion_failure(run_id, str(exc))
        raise IngestionError(f"Failed to ingest JSON file '{file_path}': {exc}") from exc


def batch_ingest_files(
    directory: str,
    pattern: str,
    execution_date: str,
) -> List[Dict[str, Any]]:
    """Batch ingest files matching a glob pattern from a directory.

    Automatically detects whether each file is CSV or JSON based on its
    extension and routes to the appropriate ingestion function.

    Args:
        directory: Base directory to scan.
        pattern: Glob pattern for file matching (e.g. ``*.csv``).
        execution_date: Airflow execution date string.

    Returns:
        List of result dicts, one per ingested file.
    """
    search_path = os.path.join(directory, pattern)
    files = sorted(globmod.glob(search_path))

    if not files:
        log.warning("No files matched pattern '%s' in '%s'", pattern, directory)
        return []

    log.info("Found %d files matching '%s'", len(files), search_path)
    results: List[Dict[str, Any]] = []

    for file_path in files:
        filename = os.path.basename(file_path)
        ext = os.path.splitext(filename)[1].lower()
        object_key = f"files/{filename}"

        try:
            if ext == ".csv":
                result = ingest_csv_file(file_path, object_key, execution_date)
            elif ext == ".json":
                result = ingest_json_file(file_path, object_key, execution_date)
            else:
                log.warning("Skipping unsupported file type: %s", filename)
                continue
            results.append(result)
        except IngestionError as exc:
            log.error("Failed to ingest '%s': %s", filename, exc)
            results.append({"file": filename, "error": str(exc)})

    log.info("Batch ingestion complete: %d/%d files processed",
             len([r for r in results if "uri" in r]), len(files))
    return results
