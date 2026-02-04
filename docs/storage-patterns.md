# Storage Patterns and Best Practices

## File Naming Conventions

Use consistent, predictable paths to simplify data discovery and pipeline logic.

### Recommended pattern

```
{bucket}/{source}/{entity}/{date}/data.{format}
```

Examples:

```
raw/postgres/users/2024-01-15/users.csv
raw/api/weather/2024-01-15/forecast.json
staging/cleaned/users/2024-01-15/users.parquet
curated/aggregated/daily_sales/2024-01-15/summary.parquet
```

### Date partitioning

For time-series or regularly ingested data, partition by date:

```
raw/postgres/orders/year=2024/month=01/day=15/orders.parquet
```

This enables efficient prefix-based listing and deletion of old partitions.

## Partitioning Strategies

### By date (most common)

Best for time-series data, daily extracts, and event logs.

```
raw/events/year=2024/month=01/day=15/events.parquet
```

### By source

Best when data arrives from multiple systems.

```
raw/postgres/table_name/data.parquet
raw/api/service_name/data.json
```

### By entity

Best for entity-centric data like customer or product records.

```
staging/cleaned/customers/batch_001.parquet
staging/cleaned/products/batch_001.parquet
```

## File Format Guidelines

| Format  | Use When | Layer | Pros | Cons |
|---------|----------|-------|------|------|
| CSV     | Source provides CSV, human-readable needed | Bronze | Universal, simple | No schema, slow reads |
| JSON    | API responses, nested data | Bronze | Flexible schema | Verbose, slow for analytics |
| Parquet | Analytics, large datasets | Silver, Gold | Columnar, compressed, fast | Not human-readable |

### General rules

- **Bronze**: Accept whatever format the source provides (CSV, JSON, XML, etc.)
- **Silver**: Convert to Parquet for efficient downstream processing
- **Gold**: Always use Parquet with appropriate compression (snappy or zstd)

## Data Retention and Lifecycle

### Bronze layer

- Retain raw data for the full compliance period (typically 7 years for financial data)
- Never modify or delete raw data; it serves as the system of record
- Enable versioning to protect against accidental overwrites

### Silver layer

- Retain for the duration needed by downstream consumers
- Can be regenerated from bronze data if needed
- Consider 90-day retention for intermediate processing artifacts

### Gold layer

- Retain as long as consumers require the aggregated views
- Update on a schedule matching business SLAs
- Archive older aggregations to cold storage if needed

## Example DAG Patterns

### Pattern 1: Simple extract-load

```python
@task
def extract_to_bronze():
    # Extract from source and save locally
    df = pd.read_sql("SELECT * FROM users", engine)
    df.to_csv("/tmp/users.csv", index=False)

    # Upload to bronze
    upload_to_layer("/tmp/users.csv", StorageLayer.BRONZE, "postgres/users/users.csv")
```

### Pattern 2: Bronze to silver transformation

```python
@task
def clean_bronze_to_silver():
    # Download from bronze
    download_from_layer(StorageLayer.BRONZE, "postgres/users/users.csv", "/tmp/raw.csv")

    # Clean
    df = pd.read_csv("/tmp/raw.csv")
    df = df.dropna(subset=["email"])
    df = df.drop_duplicates(subset=["id"])
    df.to_parquet("/tmp/clean.parquet", index=False)

    # Upload to silver
    upload_to_layer("/tmp/clean.parquet", StorageLayer.SILVER, "cleaned/users/users.parquet")
```

### Pattern 3: Silver to gold aggregation

```python
@task
def aggregate_silver_to_gold():
    # Download from silver
    download_from_layer(StorageLayer.SILVER, "cleaned/orders/orders.parquet", "/tmp/orders.parquet")

    # Aggregate
    df = pd.read_parquet("/tmp/orders.parquet")
    summary = df.groupby("date").agg(
        total_orders=("id", "count"),
        total_revenue=("amount", "sum"),
    ).reset_index()
    summary.to_parquet("/tmp/daily_sales.parquet", index=False)

    # Upload to gold
    upload_to_layer("/tmp/daily_sales.parquet", StorageLayer.GOLD, "aggregated/daily_sales.parquet")
```

### Pattern 4: Data promotion shortcut

```python
@task
def promote_validated_data():
    # After validation passes, promote directly
    promote_data(
        "cleaned/users/users.parquet",
        StorageLayer.SILVER,
        StorageLayer.GOLD,
        target_key="reports/users/users.parquet",
    )
```
