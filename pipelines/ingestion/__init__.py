"""Batch ingestion modules for the data platform.

Provides reusable components for file-based and database ingestion into
the Bronze (raw) layer of the medallion architecture.

Modules:
    schema_detector: Detect and log schema information from files and tables.
    metadata_logger: Track ingestion runs with row counts, checksums, and status.
    watermark_manager: Manage incremental load state using high-water marks.
    file_ingestor: Ingest CSV/JSON files from filesystem to MinIO Bronze layer.
    database_ingestor: Extract data from PostgreSQL with incremental logic.
"""

from .schema_detector import (
    detect_csv_schema,
    detect_json_schema,
    detect_postgres_schema,
    log_schema_change,
    SchemaDetectionError,
)
from .metadata_logger import (
    ensure_metadata_tables,
    log_ingestion_start,
    log_ingestion_success,
    log_ingestion_failure,
    get_last_successful_run,
    compute_file_checksum,
    IngestionError,
)
from .watermark_manager import (
    ensure_watermark_table,
    get_watermark,
    update_watermark,
    build_incremental_query,
    WatermarkError,
)
from .file_ingestor import (
    ingest_csv_file,
    ingest_json_file,
    batch_ingest_files,
)
from .database_ingestor import (
    extract_table_full,
    extract_table_incremental,
    extract_with_custom_query,
)

__all__ = [
    # Schema detection
    "detect_csv_schema",
    "detect_json_schema",
    "detect_postgres_schema",
    "log_schema_change",
    "SchemaDetectionError",
    # Metadata logging
    "ensure_metadata_tables",
    "log_ingestion_start",
    "log_ingestion_success",
    "log_ingestion_failure",
    "get_last_successful_run",
    "compute_file_checksum",
    "IngestionError",
    # Watermark management
    "ensure_watermark_table",
    "get_watermark",
    "update_watermark",
    "build_incremental_query",
    "WatermarkError",
    # File ingestion
    "ingest_csv_file",
    "ingest_json_file",
    "batch_ingest_files",
    # Database ingestion
    "extract_table_full",
    "extract_table_incremental",
    "extract_with_custom_query",
]
