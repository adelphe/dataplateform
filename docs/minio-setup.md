# MinIO Setup and Storage Architecture

## Overview

MinIO is an S3-compatible object storage server used as the data lake layer in this platform. It provides a local, self-hosted alternative to AWS S3 for development and can be swapped for S3 in production with no code changes.

## Medallion Architecture

Data flows through three layers, each represented by a dedicated bucket:

### Bronze Layer (`raw` bucket)

- **Purpose**: Store raw ingested data exactly as received from source systems.
- **Characteristics**: Immutable, append-only, original format preserved.
- **Directory structure**:
  - `raw/postgres/` - Data extracted from PostgreSQL databases
  - `raw/files/` - Uploaded flat files (CSV, Excel, etc.)
  - `raw/api/` - Data fetched from external APIs
  - `raw/streaming/` - Data from streaming sources

### Silver Layer (`staging` bucket)

- **Purpose**: Store cleaned, validated, and deduplicated data.
- **Characteristics**: Schema-enforced, data types standardized, nulls handled.
- **Directory structure**:
  - `staging/cleaned/` - Data with nulls removed and types corrected
  - `staging/validated/` - Data passing quality checks
  - `staging/deduplicated/` - Deduplicated datasets

### Gold Layer (`curated` bucket)

- **Purpose**: Store business-ready, aggregated data optimized for analytics and reporting.
- **Characteristics**: Denormalized, pre-aggregated, optimized for query performance.
- **Directory structure**:
  - `curated/aggregated/` - Pre-computed aggregations
  - `curated/reports/` - Report-ready datasets
  - `curated/ml/` - Feature stores and ML-ready data

## Accessing MinIO

### Web Console

Open [http://localhost:9001](http://localhost:9001) in your browser.

| Field    | Value     |
|----------|-----------|
| Username | `minio`   |
| Password | `minio123`|

### S3 API

The S3-compatible API is available at `http://localhost:9000`. Use any S3 client (boto3, aws-cli, mc) configured with:

```
Endpoint: http://localhost:9000
Access Key: minio
Secret Key: minio123
Region: us-east-1
```

## Using the Storage Utility Library

### Upload data to a layer

```python
from utils.storage import StorageLayer, upload_to_layer

uri = upload_to_layer(
    file_path="/tmp/users.csv",
    layer=StorageLayer.BRONZE,
    object_key="postgres/users/2024-01-15/users.csv",
    metadata={"source": "postgres", "table": "users"},
)
print(uri)  # s3://raw/postgres/users/2024-01-15/users.csv
```

### Download data from a layer

```python
from utils.storage import StorageLayer, download_from_layer

local_path = download_from_layer(
    layer=StorageLayer.SILVER,
    object_key="cleaned/users/users_clean.parquet",
    file_path="/tmp/users_clean.parquet",
)
```

### List objects in a layer

```python
from utils.storage import StorageLayer, list_layer_objects

objects = list_layer_objects(StorageLayer.BRONZE, prefix="postgres/")
for obj in objects:
    print(f"{obj['Key']} ({obj['Size']} bytes)")
```

### Promote data between layers

```python
from utils.storage import StorageLayer, promote_data

# After cleaning, promote from bronze to silver
uri = promote_data(
    object_key="postgres/users/users.csv",
    source_layer=StorageLayer.BRONZE,
    target_layer=StorageLayer.SILVER,
    target_key="cleaned/users/users_clean.csv",
)
```

## Bucket Initialization

Buckets are automatically created when the platform starts via the `minio-init` Docker service. The initialization script (`infrastructure/minio/init-buckets.sh`):

1. Waits for MinIO to be available
2. Creates `raw`, `staging`, and `curated` buckets (idempotent)
3. Establishes directory prefixes within each bucket
4. Enables versioning where supported

To manually re-run initialization:

```bash
docker compose restart minio-init
```

## Make Commands

```bash
make minio-buckets          # List all buckets and contents
make minio-upload-sample    # Upload sample test data
make minio-verify           # Verify bucket structure
make minio-console          # Show MinIO console URL
```

## Troubleshooting

### Cannot connect to MinIO

Verify the service is running:

```bash
docker compose ps minio
docker compose logs minio
```

### Buckets not created

Check the init service logs:

```bash
docker compose logs minio-init
```

### Permission denied errors

Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables match `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` in your `.env` file.

### Bucket versioning warnings

MinIO versioning requires the erasure-coded deployment mode. In single-node development mode, versioning warnings can be safely ignored.
