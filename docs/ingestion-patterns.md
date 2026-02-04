# Ingestion Patterns

This guide covers the batch ingestion patterns available in the data platform, including full and incremental loading, file-based workflows, database extraction strategies, metadata tracking, and schema evolution handling.

## Overview

All ingestion pipelines follow a common flow:

1. **Setup** - Ensure tracking tables exist (metadata, watermarks, schema history)
2. **Extract** - Read data from source (files or database)
3. **Detect Schema** - Introspect structure and log schema metadata
4. **Convert** - Transform to Parquet format for efficient storage
5. **Upload** - Write to the MinIO Bronze (raw) layer
6. **Log Metadata** - Record row counts, checksums, and status
7. **Validate** - Run data quality checks on ingested data

## Full Load vs Incremental Load

### Full Load

Extracts the entire source dataset on every run. Use this pattern when:

- The source table is small (< 1M rows)
- You need a complete snapshot for point-in-time analysis
- The source does not have a reliable change-tracking column

```python
from ingestion.database_ingestor import extract_table_full

result = extract_table_full(
    table_name="products",
    schema="public",
    object_key=f"postgres/products/{ds}/products.parquet",
    execution_date=ds,
)
```

### Incremental Load

Extracts only new or changed rows since the last run using a high-water mark. Use this pattern when:

- The source table is large
- A monotonically increasing column exists (timestamp or auto-increment ID)
- You want to minimize extraction time and resource usage

```python
from ingestion.database_ingestor import extract_table_incremental

result = extract_table_incremental(
    table_name="orders",
    schema="public",
    watermark_column="updated_at",
    object_key=f"postgres/orders/{ds}/orders.parquet",
    execution_date=ds,
    watermark_type="timestamp",  # or "integer"
)
```

On the first run (no watermark stored), a full extract is performed automatically. Subsequent runs only pull rows where `watermark_column > last_watermark_value`.

### Watermark Types

| Type | Column Examples | Use Case |
|---|---|---|
| `timestamp` | `updated_at`, `created_at`, `execution_date` | Tables with timestamp-based change tracking |
| `integer` | `id`, `sequence_num`, `version` | Tables with auto-incrementing primary keys |

## File-Based Ingestion

### Single File Ingestion

```python
from ingestion.file_ingestor import ingest_csv_file, ingest_json_file

# CSV
result = ingest_csv_file(
    file_path="/data/customers.csv",
    object_key="files/customers.parquet",
    execution_date="2024-01-01",
)

# JSON
result = ingest_json_file(
    file_path="/data/events.json",
    object_key="files/events.parquet",
    execution_date="2024-01-01",
)
```

### Batch File Ingestion

Process all files matching a glob pattern in a directory:

```python
from ingestion.file_ingestor import batch_ingest_files

results = batch_ingest_files(
    directory="/opt/airflow/data/input",
    pattern="*.csv",
    execution_date="2024-01-01",
)

# Results include both successes and errors
for r in results:
    if "uri" in r:
        print(f"Success: {r['uri']} ({r['row_count']} rows)")
    else:
        print(f"Error: {r['file']} - {r['error']}")
```

### Supported File Formats

| Format | Extension | Conversion |
|---|---|---|
| CSV | `.csv` | Read with pandas, convert to Parquet |
| JSON | `.json` | JSON array or newline-delimited, convert to Parquet |

All files are converted to Parquet before uploading to the Bronze layer for consistent, efficient storage.

## Database Extraction Strategies

### Custom Query Extraction

For complex joins or filtered extractions:

```python
from ingestion.database_ingestor import extract_with_custom_query

result = extract_with_custom_query(
    query="""
        SELECT o.id, o.amount, c.name AS customer_name
        FROM public.orders o
        JOIN public.customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
    """,
    object_key=f"postgres/completed_orders/{ds}/data.parquet",
    execution_date=ds,
    source_name="completed_orders_join",
)
```

### Extraction from System Tables

Useful for metadata collection and monitoring:

```python
result = extract_table_full(
    table_name="tables",
    schema="information_schema",
    object_key=f"postgres/system/tables/{ds}/tables.parquet",
    execution_date=ds,
)
```

## Metadata Tracking and Monitoring

### Ingestion Metadata Table

Every ingestion run is tracked in `data_platform.ingestion_metadata`:

| Column | Description |
|---|---|
| `id` | Auto-generated run ID |
| `source_name` | Source identifier (filename or table name) |
| `source_type` | `csv`, `json`, or `postgres` |
| `execution_date` | Airflow execution date |
| `row_count` | Number of rows ingested |
| `file_size_bytes` | Size of the Parquet file uploaded |
| `checksum` | MD5 hash of the uploaded file |
| `status` | `running`, `success`, or `failed` |
| `error_message` | Error details (for failed runs) |

### Querying Ingestion History

```sql
-- Recent successful ingestions
SELECT source_name, execution_date, row_count, file_size_bytes
FROM data_platform.ingestion_metadata
WHERE status = 'success'
ORDER BY created_at DESC
LIMIT 20;

-- Failed ingestions requiring attention
SELECT source_name, execution_date, error_message, created_at
FROM data_platform.ingestion_metadata
WHERE status = 'failed'
ORDER BY created_at DESC;

-- Daily ingestion summary
SELECT execution_date,
       COUNT(*) AS total_runs,
       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failures,
       SUM(row_count) AS total_rows
FROM data_platform.ingestion_metadata
GROUP BY execution_date
ORDER BY execution_date DESC;
```

### Watermark State

Current watermark values are stored in `data_platform.watermarks`:

```sql
SELECT source_name, watermark_column, watermark_value, watermark_type, updated_at
FROM data_platform.watermarks
ORDER BY updated_at DESC;
```

## Schema Evolution Handling

### Schema Detection

Schema is automatically detected during ingestion and logged to `data_platform.schema_history`:

```python
from ingestion.schema_detector import detect_csv_schema, detect_postgres_schema

# From a file
schema = detect_csv_schema("/data/customers.csv")
# Returns: {"format": "csv", "columns": [...], "row_count": N}

# From a PostgreSQL table
schema = detect_postgres_schema("orders", "public")
# Returns: {"format": "postgres", "table_name": "orders", "schema": "public", "columns": [...]}
```

### Tracking Schema Changes

```sql
-- View schema history for a source
SELECT source_name, schema_metadata, execution_date, created_at
FROM data_platform.schema_history
WHERE source_name = 'customers_20240101.csv'
ORDER BY created_at DESC;

-- Detect schema drift (column count changes)
SELECT source_name, execution_date,
       jsonb_array_length(schema_metadata->'columns') AS column_count
FROM data_platform.schema_history
ORDER BY source_name, created_at;
```

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|---|---|---|
| `SchemaDetectionError: Failed to read CSV` | File path incorrect or file corrupted | Verify the file exists and is valid CSV |
| `IngestionError: Failed to ingest` | MinIO connection failure or disk space | Check MinIO connectivity and bucket availability |
| `WatermarkError: Failed to query watermark` | PostgreSQL connection issue | Verify `data_platform.watermarks` table exists |
| No new rows in incremental extract | Watermark already past latest data | Check watermark value vs source data range |
| Empty results from `batch_ingest_files` | No files match the glob pattern | Verify directory path and file naming |

### Resetting a Watermark

If you need to re-extract data from an earlier point:

```sql
UPDATE data_platform.watermarks
SET watermark_value = '2024-01-01T00:00:00Z'
WHERE source_name = 'public.orders'
  AND watermark_column = 'updated_at';
```

Or delete the watermark entry entirely to trigger a full re-extraction:

```sql
DELETE FROM data_platform.watermarks
WHERE source_name = 'public.orders';
```

### Verifying Bronze Layer Contents

```python
from utils.storage import StorageLayer, list_layer_objects

objects = list_layer_objects(StorageLayer.BRONZE, prefix="postgres/orders/2024-01-01/")
for obj in objects:
    print(f"{obj['Key']} ({obj['Size']} bytes)")
```
