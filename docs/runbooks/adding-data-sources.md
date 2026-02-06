# Runbook: Adding New Data Sources

This runbook covers the process of adding new data sources to the platform.

## Prerequisites

- Access to Airflow UI (port 8080)
- Access to MinIO console (port 9001)
- Knowledge of the source system schema
- Appropriate credentials for the source system

## Adding a Database Source

### Step 1: Create Database Connection in Airflow

1. Open Airflow UI at `http://localhost:8080`
2. Navigate to **Admin** > **Connections**
3. Click **+** to add a new connection
4. Fill in the connection details:

```
Connection Id: source_db_<name>
Connection Type: Postgres (or appropriate type)
Host: <database-host>
Schema: <database-name>
Login: <username>
Password: <password>
Port: 5432
```

5. Click **Save**

### Step 2: Create Ingestion DAG

Create a new DAG file at `pipelines/dags/ingestion/ingest_<source_name>.py`:

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract_table(**context):
    from ingestion.database_ingestor import extract_table_full, extract_table_incremental

    ds = context["ds"]

    # For full extract:
    result = extract_table_full(
        table_name="your_table",
        schema="public",
        object_key=f"source_name/your_table/{ds}/data.parquet",
        conn_id="source_db_name",
    )

    # OR for incremental extract:
    # result = extract_table_incremental(
    #     table_name="your_table",
    #     schema="public",
    #     watermark_column="updated_at",
    #     object_key=f"source_name/your_table/{ds}/data.parquet",
    #     conn_id="source_db_name",
    #     execution_date=ds,
    # )

    return result


with DAG(
    dag_id="ingest_source_name",
    default_args=default_args,
    description="Ingest data from <source_name>",
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "source-name"],
) as dag:

    extract = PythonOperator(
        task_id="extract_table",
        python_callable=_extract_table,
    )
```

### Step 3: Verify Data in MinIO

1. Open MinIO Console at `http://localhost:9001`
2. Navigate to the `raw` bucket
3. Verify the data appears at the expected path

### Step 4: Add dbt Source Definition

Edit `transformations/dbt_project/models/staging/_sources.yml`:

```yaml
sources:
  - name: raw
    schema: raw
    tables:
      - name: your_table
        description: "Data from <source_name>"
        columns:
          - name: id
            description: "Primary key"
          - name: created_at
            description: "Record creation timestamp"
```

### Step 5: Create Staging Model

Create `transformations/dbt_project/models/staging/stg_<source>_<table>.sql`:

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'your_table') }}
),

cleaned AS (
    SELECT
        id,
        -- Add cleaning/transformation logic
        created_at::timestamp AS created_at
    FROM source
)

SELECT * FROM cleaned
```

---

## Adding a File Source (CSV/JSON)

### Step 1: Define File Location

Files can be placed in:
- Local path: `/opt/airflow/data/input/`
- MinIO bucket: Upload directly to `raw` bucket

### Step 2: Create File Ingestion DAG

```python
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_files(**context):
    from ingestion.file_ingestor import batch_ingest_files

    ds = context["ds"]
    results = batch_ingest_files(
        directory="/opt/airflow/data/input/source_name",
        pattern="*.csv",
        execution_date=ds,
    )

    uris = [r["uri"] for r in results if "uri" in r]
    context["ti"].xcom_push(key="ingested_files", value=uris)
    return results


with DAG(
    dag_id="ingest_file_source",
    default_args=default_args,
    description="Ingest files from <source_name>",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "files"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_files",
        python_callable=_ingest_files,
    )
```

### Step 3: Register Schema

The schema detector automatically infers types, but you can also log schema manually:

```python
from ingestion.schema_detector import detect_csv_schema, log_schema_change

schema = detect_csv_schema("/path/to/sample.csv")
log_schema_change("source_name.table_name", schema, "new")
```

---

## Adding an API Source

### Step 1: Create API Extraction Function

```python
import requests
import pandas as pd


def extract_api_data(endpoint: str, params: dict = None) -> pd.DataFrame:
    """Extract data from API endpoint."""
    response = requests.get(endpoint, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    return pd.DataFrame(data)
```

### Step 2: Create Ingestion DAG

```python
def _extract_api(**context):
    from utils.storage import upload_to_layer, StorageLayer

    ds = context["ds"]

    # Extract from API
    df = extract_api_data(
        endpoint="https://api.example.com/data",
        params={"date": ds}
    )

    # Save locally
    local_path = f"/tmp/api_data_{ds}.parquet"
    df.to_parquet(local_path, index=False)

    # Upload to Bronze
    upload_to_layer(
        local_path,
        StorageLayer.BRONZE,
        f"api/source_name/{ds}/data.parquet"
    )

    # Log metadata
    from ingestion.metadata_logger import log_ingestion_success
    log_ingestion_success(
        source_name="api.source_name",
        row_count=len(df),
    )
```

---

## Post-Setup Checklist

- [ ] DAG appears in Airflow UI without import errors
- [ ] Test run completes successfully
- [ ] Data visible in MinIO `raw` bucket
- [ ] Schema registered in `data_platform.schema_history`
- [ ] Metadata logged in `data_platform.ingestion_metadata`
- [ ] dbt source definition added
- [ ] Staging model created and tested
- [ ] Data quality expectations defined (optional)
- [ ] OpenMetadata sync updated (runs automatically)

## Troubleshooting

### DAG not appearing in Airflow

```bash
# Check for import errors
make airflow-cli CMD="dags list-import-errors"

# Force DAG refresh
make airflow-cli CMD="dags reserialize"
```

### Connection test failing

```bash
# Test connection via Airflow CLI
make airflow-cli CMD="connections test source_db_name"
```

### Data not appearing in MinIO

1. Check DAG logs in Airflow UI
2. Verify MinIO credentials in environment
3. Check bucket permissions
4. Verify network connectivity

### Schema detection issues

```python
# Manually inspect detected schema
from ingestion.schema_detector import detect_csv_schema
schema = detect_csv_schema("/path/to/file.csv")
print(schema)
```

## Related Documentation

- [Ingestion Patterns](../ingestion-patterns.md)
- [Storage Patterns](../storage-patterns.md)
- [DAG Development Guide](../dag-development-guide.md)
