# Data Flow Documentation

This document describes the complete data flow from ingestion to consumption in the data platform.

## End-to-End Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           DATA FLOW OVERVIEW                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│   DATA SOURCES   │
│                  │
│  • PostgreSQL    │
│  • APIs          │
│  • CSV/JSON      │
│  • External DBs  │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: INGESTION                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐│
│  │  Ingestion DAG (pipelines/dags/ingestion/)                                                          ││
│  │                                                                                                      ││
│  │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     ││
│  │  │ Schema Detection │───▶│  Data Extraction │───▶│ Metadata Logging │───▶│ Upload to Bronze │     ││
│  │  │                  │    │                  │    │                  │    │                  │     ││
│  │  │ • Type inference │    │ • Full extract   │    │ • Row counts     │    │ • Parquet format │     ││
│  │  │ • Column mapping │    │ • Incremental    │    │ • Checksums      │    │ • Partitioned    │     ││
│  │  │ • Schema history │    │ • Watermarks     │    │ • Run tracking   │    │ • Compressed     │     ││
│  │  └──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘     ││
│  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (MinIO: raw bucket)                                                                        │
│                                                                                                          │
│  raw/                                                                                                    │
│  ├── postgres/                                                                                          │
│  │   ├── customers/2024-01-15/customers.parquet                                                         │
│  │   ├── orders/2024-01-15/orders.parquet                                                               │
│  │   └── products/2024-01-15/products.parquet                                                           │
│  ├── api/                                                                                               │
│  │   └── weather/2024-01-15/forecast.json                                                               │
│  └── files/                                                                                             │
│      └── sales/2024-01-15/transactions.csv                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         │  ┌─────────────────────────────────────────────────────────┐
         ├─▶│  DATA QUALITY CHECK (raw_data_checkpoint)               │
         │  │  • Validates: schema, nulls, data types, row counts     │
         │  │  • Output: Pass/Fail + Data Docs                        │
         │  └─────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 2: LOADING                                                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐│
│  │  MinIOToPostgresOperator                                                                            ││
│  │                                                                                                      ││
│  │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐                              ││
│  │  │ Download Parquet │───▶│   Read to DF     │───▶│ Bulk Insert to   │                              ││
│  │  │  from Bronze     │    │                  │    │ PostgreSQL raw   │                              ││
│  │  └──────────────────┘    └──────────────────┘    └──────────────────┘                              ││
│  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PostgreSQL: raw schema                                                                                  │
│                                                                                                          │
│  • raw.customers                                                                                        │
│  • raw.orders                                                                                           │
│  • raw.products                                                                                         │
│  • raw.transactions                                                                                     │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: TRANSFORMATION (dbt)                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐│
│  │  dbt Transform DAG (pipelines/dags/transformation/dbt_transform.py)                                  ││
│  │                                                                                                      ││
│  │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     ││
│  │  │   dbt seed       │───▶│  dbt run staging │───▶│dbt run intermedi.│───▶│   dbt run marts  │     ││
│  │  │                  │    │                  │    │                  │    │                  │     ││
│  │  │ Load reference   │    │ Clean & validate │    │ Complex joins &  │    │ Final business   │     ││
│  │  │ data             │    │ Type conversion  │    │ enrichments      │    │ aggregations     │     ││
│  │  └──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘     ││
│  │                                   │                       │                       │               ││
│  │                                   ▼                       ▼                       ▼               ││
│  │                          ┌─────────────────────────────────────────────────────────────┐          ││
│  │                          │  dbt test (per layer validation)                            │          ││
│  │                          └─────────────────────────────────────────────────────────────┘          ││
│  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (PostgreSQL: staging schema)                                                               │
│                                                                                                          │
│  • staging.stg_customers     - Cleaned customer data with valid emails                                  │
│  • staging.stg_orders        - Orders with proper timestamps, validated IDs                             │
│  • staging.stg_products      - Products with standardized categories                                    │
│  • staging.stg_transactions  - Transactions with computed fields                                        │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         │  ┌─────────────────────────────────────────────────────────┐
         ├─▶│  DATA QUALITY CHECK (staging_checkpoint)                │
         │  │  • Validates: business rules, referential integrity     │
         │  │  • Output: Pass/Fail + Data Docs                        │
         │  └─────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  INTERMEDIATE LAYER (PostgreSQL: intermediate schema)                                                    │
│                                                                                                          │
│  • intermediate.int_customer_orders    - Customer order history                                         │
│  • intermediate.int_product_sales      - Product performance metrics                                    │
│  • intermediate.int_daily_aggregates   - Daily rollups                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (PostgreSQL: marts schema)                                                                   │
│                                                                                                          │
│  • marts.dim_customers           - Customer dimension with attributes                                   │
│  • marts.dim_products            - Product dimension                                                    │
│  • marts.fct_orders              - Order facts                                                          │
│  • marts.mart_customer_analytics - Customer metrics and segments                                        │
│  • marts.mart_product_analytics  - Product performance                                                  │
│  • marts.mart_daily_summary      - Daily business KPIs                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │
         │  ┌─────────────────────────────────────────────────────────┐
         ├─▶│  DATA QUALITY CHECK (marts_checkpoint)                  │
         │  │  • Validates: KPI thresholds, completeness, freshness   │
         │  │  • Output: Pass/Fail + Data Docs                        │
         │  └─────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: CONSUMPTION                                                                                    │
│                                                                                                          │
│  ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐                       │
│  │   Apache Superset     │  │    Direct SQL Access  │  │    Reverse ETL        │                       │
│  │                       │  │                       │  │                       │                       │
│  │  • Dashboards         │  │  • SQL Lab queries    │  │  • Export to CRM      │                       │
│  │  • Scheduled reports  │  │  • BI tool connects   │  │  • API endpoints      │                       │
│  │  • Embedded analytics │  │  • Data science       │  │  • External systems   │                       │
│  └───────────────────────┘  └───────────────────────┘  └───────────────────────┘                       │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Detailed Phase Descriptions

### Phase 1: Ingestion

The ingestion layer extracts data from various sources and loads it into the Bronze layer.

#### Ingestion Modules

| Module | Location | Purpose |
|--------|----------|---------|
| `file_ingestor` | `pipelines/ingestion/file_ingestor.py` | CSV/JSON file ingestion |
| `database_ingestor` | `pipelines/ingestion/database_ingestor.py` | PostgreSQL extraction |
| `schema_detector` | `pipelines/ingestion/schema_detector.py` | Automatic schema inference |
| `watermark_manager` | `pipelines/ingestion/watermark_manager.py` | Incremental load state |
| `metadata_logger` | `pipelines/ingestion/metadata_logger.py` | Ingestion tracking |

#### Ingestion Process Flow

```python
# 1. Schema Detection
schema = detect_csv_schema("/path/to/file.csv")
# Returns: {"columns": [{"name": "id", "type": "integer"}, ...]}

# 2. Data Extraction
# Full extract:
extract_table_full("customers", "public", "postgres/customers/data.parquet")

# Incremental extract:
extract_table_incremental(
    table_name="orders",
    watermark_column="updated_at",
    object_key="postgres/orders/data.parquet"
)

# 3. Metadata Logging
log_ingestion_success(
    source_name="postgres.orders",
    row_count=1500,
    file_checksum="abc123...",
    watermark_value="2024-01-15T10:30:00"
)

# 4. Upload to Bronze
upload_to_layer(local_path, StorageLayer.BRONZE, object_key)
```

#### Watermark Management

Watermarks enable incremental data extraction:

```
┌─────────────────────────────────────────────────────────────┐
│  data_platform.watermarks                                   │
├─────────────────────────────────────────────────────────────┤
│  source_name          │ watermark_column │ watermark_value  │
│  ─────────────────────┼──────────────────┼─────────────────│
│  postgres.orders      │ updated_at       │ 2024-01-15 10:30 │
│  postgres.customers   │ modified_date    │ 2024-01-14 23:00 │
│  api.transactions     │ id               │ 150000           │
└─────────────────────────────────────────────────────────────┘
```

### Phase 2: Loading (Bronze to PostgreSQL)

The `MinIOToPostgresOperator` loads Parquet files from MinIO into PostgreSQL:

```python
MinIOToPostgresOperator(
    task_id="load_customers",
    bucket_name="raw",
    object_key="postgres/customers/2024-01-15/customers.parquet",
    table_name="customers",
    schema="raw",
    load_mode="replace",  # or "append"
)
```

### Phase 3: Transformation (dbt)

dbt models transform data through the medallion layers:

#### Model Organization

```
transformations/dbt_project/models/
├── staging/                    # SILVER: Clean and validate
│   ├── _staging__models.yml
│   ├── stg_customers.sql
│   ├── stg_orders.sql
│   └── stg_products.sql
├── intermediate/               # INTERMEDIATE: Enrich and join
│   ├── _intermediate__models.yml
│   ├── int_customer_orders.sql
│   └── int_product_sales.sql
└── marts/                      # GOLD: Business aggregations
    ├── _marts__models.yml
    ├── mart_customer_analytics.sql
    ├── mart_product_analytics.sql
    └── mart_daily_summary.sql
```

#### Transformation Example

```sql
-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        id AS customer_id,
        TRIM(LOWER(email)) AS email,
        INITCAP(first_name) AS first_name,
        INITCAP(last_name) AS last_name,
        created_at::timestamp AS created_at,
        updated_at::timestamp AS updated_at
    FROM source
    WHERE email IS NOT NULL
      AND email LIKE '%@%.%'
)

SELECT * FROM cleaned
```

### Phase 4: Consumption

#### Superset Dashboards

Pre-configured dashboards connect to marts schema:

| Dashboard | Tables | Purpose |
|-----------|--------|---------|
| Customer Insights | `mart_customer_analytics` | Customer segmentation and behavior |
| Product Performance | `mart_product_analytics` | Product sales and trends |
| Daily Operations | `mart_daily_summary` | Daily KPIs and metrics |
| Platform Operations | `platform_health_metrics` | Platform monitoring |

#### SQL Access

Direct connection to PostgreSQL marts:
```
Host: localhost (or postgres in Docker)
Port: 5432
Database: airflow
Schema: marts
User: airflow
```

## Data Quality Checkpoints

Quality validation occurs at each layer transition:

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  CHECKPOINT                │  LAYER      │  VALIDATIONS                        │
├────────────────────────────┼─────────────┼─────────────────────────────────────┤
│  raw_data_checkpoint       │  Bronze     │  Schema compliance, null checks,    │
│                            │             │  data type validation               │
├────────────────────────────┼─────────────┼─────────────────────────────────────┤
│  staging_checkpoint        │  Silver     │  Business rules, referential        │
│                            │             │  integrity, deduplication           │
├────────────────────────────┼─────────────┼─────────────────────────────────────┤
│  marts_checkpoint          │  Gold       │  KPI thresholds, completeness,      │
│                            │             │  freshness requirements             │
├────────────────────────────┼─────────────┼─────────────────────────────────────┤
│  full_pipeline_checkpoint  │  All        │  End-to-end validation              │
└────────────────────────────────────────────────────────────────────────────────┘
```

## Data Lineage

Data lineage is tracked through:
1. **dbt Lineage**: Model dependencies and column-level lineage
2. **OpenMetadata**: Cross-system lineage visualization
3. **Ingestion Metadata**: Source-to-Bronze tracking

```
Source DB → Bronze (MinIO) → Raw Schema → Staging → Intermediate → Marts → Superset
    │           │                │            │           │          │         │
    └───────────┴────────────────┴────────────┴───────────┴──────────┴─────────┘
                                 OpenMetadata Lineage Graph
```

## Scheduling

| Pipeline | Schedule | Dependencies |
|----------|----------|--------------|
| Ingestion DAGs | Varies (hourly/daily) | Source availability |
| dbt Transform | After ingestion | Ingestion completion |
| Quality Checks | After transformation | Transform completion |
| Metadata Sync | Every 6 hours | None |
| Platform Health | Every 15 minutes | None |

## Error Handling

### Retry Strategy

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}
```

### Failure Alerts

On failure, the alerting system triggers:
1. Email notification (if enabled)
2. Slack message (if enabled)
3. Alert logged to `data_platform.alert_history`

### Data Quality Failures

When quality checks fail:
1. DAG marked as failed
2. Data Docs updated with failure details
3. Downstream tasks prevented from running
4. Alert sent to data team

## Related Documentation

- [Architecture](architecture.md) - System architecture overview
- [Ingestion Patterns](ingestion-patterns.md) - Detailed ingestion patterns
- [Storage Patterns](storage-patterns.md) - Storage best practices
- [DAG Development Guide](dag-development-guide.md) - Creating pipelines
