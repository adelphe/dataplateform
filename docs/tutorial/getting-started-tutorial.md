# Data Platform Tutorial: Complete Step-by-Step Guide

Welcome to this comprehensive tutorial for the Data Platform. This guide will take you from zero to running your own data pipelines, step by step. Each section explains the concepts involved and provides hands-on examples you can run.

## Table of Contents

1. [Introduction](#1-introduction)
2. [Prerequisites](#2-prerequisites)
3. [Understanding the Architecture](#3-understanding-the-architecture)
4. [Setting Up the Platform](#4-setting-up-the-platform)
5. [Exploring the Services](#5-exploring-the-services)
6. [The Medallion Architecture](#6-the-medallion-architecture)
7. [Your First Pipeline: Hello World](#7-your-first-pipeline-hello-world)
8. [Data Ingestion: From Source to Bronze](#8-data-ingestion-from-source-to-bronze)
9. [Data Transformation with dbt](#9-data-transformation-with-dbt)
10. [Data Quality with Great Expectations](#10-data-quality-with-great-expectations)
11. [Creating Dashboards with Superset](#11-creating-dashboards-with-superset)
12. [Data Governance with OpenMetadata](#12-data-governance-with-openmetadata)
13. [Building Your Own Pipeline](#13-building-your-own-pipeline)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Introduction

### What is this Platform?

This is a **modern data platform** that helps you:
- **Collect** data from various sources (databases, files, APIs)
- **Store** data in organized layers (raw, cleaned, aggregated)
- **Transform** data into business-ready formats
- **Validate** data quality automatically
- **Visualize** data through dashboards
- **Govern** data with catalogs and lineage tracking

### Who is this for?

This tutorial is designed for:
- Data engineers getting started with data platforms
- Developers who want to understand data infrastructure
- Analysts who want to know how data reaches their dashboards
- Anyone curious about modern data engineering

### What you'll learn

By the end of this tutorial, you will:
- Understand how all components work together
- Run the entire platform locally
- Execute data pipelines
- Transform data using SQL
- Create your own dashboards
- Build a complete pipeline from scratch

---

## 2. Prerequisites

### Required Knowledge

- **Basic command line** - Running commands in a terminal
- **Basic SQL** - SELECT, INSERT, WHERE clauses
- **Basic Python** - Variables, functions, imports (helpful but not required)

### Required Software

Before starting, install the following:

#### Docker and Docker Compose

Docker lets us run all services in isolated containers without installing them directly.

**Install Docker Desktop:**
- **macOS/Windows**: Download from [docker.com](https://www.docker.com/products/docker-desktop)
- **Linux**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install docker.io docker-compose-plugin

# Start Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (logout/login after)
sudo usermod -aG docker $USER
```

**Verify installation:**
```bash
docker --version
# Expected: Docker version 24.x or higher

docker compose version
# Expected: Docker Compose version v2.x or higher
```

#### Make (optional but recommended)

Make simplifies running complex commands.

```bash
# Ubuntu/Debian
sudo apt install make

# macOS (comes with Xcode tools)
xcode-select --install

# Verify
make --version
```

#### Git

```bash
# Ubuntu/Debian
sudo apt install git

# Verify
git --version
```

### System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Disk Space | 10 GB | 20 GB |
| CPU Cores | 4 | 8 |

---

## 3. Understanding the Architecture

Before we start the platform, let's understand what we're building.

### The Big Picture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PLATFORM                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│  │  DATA    │    │  BRONZE  │    │  SILVER  │    │   GOLD   │      │
│  │ SOURCES  │───▶│  (Raw)   │───▶│ (Cleaned)│───▶│ (Ready)  │      │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘      │
│       │               │               │               │             │
│       │          ┌────┴────┐    ┌────┴────┐    ┌────┴────┐         │
│       │          │  MinIO  │    │   dbt   │    │ Superset│         │
│       │          │ Storage │    │Transform│    │Dashboard│         │
│       │          └─────────┘    └─────────┘    └─────────┘         │
│       │                                                             │
│  ┌────┴─────────────────────────────────────────────────────────┐  │
│  │                    APACHE AIRFLOW                             │  │
│  │              (Orchestrates everything)                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    SUPPORTING SERVICES                        │  │
│  │   PostgreSQL (Database)  │  OpenMetadata (Catalog)           │  │
│  │   Great Expectations     │  Elasticsearch (Search)           │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components Explained

#### 1. Apache Airflow (Orchestrator)
**What it does:** Schedules and runs your data pipelines
**Analogy:** Think of it as a smart alarm clock that triggers tasks at the right time and in the right order
**URL:** http://localhost:8080

#### 2. MinIO (Object Storage)
**What it does:** Stores files (like a cloud storage service)
**Analogy:** It's like Dropbox or Google Drive, but for your data platform
**URL:** http://localhost:9001

#### 3. PostgreSQL (Database)
**What it does:** Stores structured data in tables
**Analogy:** It's like Excel spreadsheets, but much more powerful
**Port:** localhost:5432

#### 4. dbt (Data Build Tool)
**What it does:** Transforms raw data into clean, usable formats using SQL
**Analogy:** Like a factory assembly line that shapes raw materials into finished products

#### 5. Apache Superset (Visualization)
**What it does:** Creates dashboards and charts from your data
**Analogy:** Like creating PowerPoint charts, but connected to live data
**URL:** http://localhost:8088

#### 6. Great Expectations (Data Quality)
**What it does:** Validates that your data meets quality standards
**Analogy:** Like a quality inspector checking products before shipping

#### 7. OpenMetadata (Data Catalog)
**What it does:** Tracks what data exists and where it came from
**Analogy:** Like a library catalog that helps you find books
**URL:** http://localhost:8585

### How Data Flows Through the System

```
1. INGEST
   Raw data (CSV, JSON, database tables)
   ↓
2. STORE (Bronze Layer)
   MinIO "raw" bucket - exact copy of source data
   ↓
3. CLEAN (Silver Layer)
   PostgreSQL staging schema - validated, typed data
   ↓
4. TRANSFORM (Gold Layer)
   PostgreSQL marts schema - business-ready aggregations
   ↓
5. VISUALIZE
   Superset dashboards - charts and reports
```

---

## 4. Setting Up the Platform

Now let's get our hands dirty and start the platform!

### Step 4.1: Clone the Repository

If you haven't already:

```bash
# Navigate to where you want the project
cd ~/projects

# Clone the repository (adjust URL as needed)
git clone <repository-url> dataplateform
cd dataplateform
```

### Step 4.2: Configure Environment Variables

The platform uses environment variables for configuration. Let's set them up:

```bash
# Copy the example environment file
cp .env.example .env

# View the default settings (optional)
cat .env
```

**Key settings in .env:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `POSTGRES_USER` | airflow | Database username |
| `POSTGRES_PASSWORD` | airflow | Database password |
| `MINIO_ROOT_USER` | minio | MinIO username |
| `MINIO_ROOT_PASSWORD` | minio123 | MinIO password |
| `AIRFLOW_ADMIN_USER` | admin | Airflow login |
| `AIRFLOW_ADMIN_PASSWORD` | admin | Airflow password |

> **Note:** For local development, the defaults are fine. For production, always change passwords!

### Step 4.3: Start the Platform

Using Make (recommended):

```bash
# Setup and start everything
make setup
make start
```

Or using Docker Compose directly:

```bash
# Start all services
docker compose up -d
```

**What happens during startup:**

1. Docker downloads required images (~5-10 minutes first time)
2. PostgreSQL initializes databases and schemas
3. MinIO creates storage buckets (raw, staging, curated)
4. Airflow starts scheduler and webserver
5. Superset initializes with sample dashboards
6. OpenMetadata starts with Elasticsearch

### Step 4.4: Verify Everything is Running

```bash
# Check service status
make status

# Or with Docker directly
docker compose ps
```

**Expected output:**

```
NAME                STATUS
postgres            running (healthy)
minio               running (healthy)
airflow-webserver   running (healthy)
airflow-scheduler   running (healthy)
superset            running (healthy)
openmetadata        running (healthy)
elasticsearch       running (healthy)
```

> **Tip:** Services may take 2-3 minutes to become healthy after starting.

### Step 4.5: Troubleshooting Startup Issues

**Issue: Services not starting**
```bash
# View logs
docker compose logs airflow-webserver
docker compose logs postgres

# Restart a specific service
docker compose restart airflow-webserver
```

**Issue: Port already in use**
```bash
# Find what's using port 8080
lsof -i :8080

# Or change the port in docker-compose.yml
```

**Issue: Out of memory**
```bash
# Increase Docker memory in Docker Desktop settings
# Or reduce services (stop OpenMetadata if not needed)
docker compose stop openmetadata elasticsearch
```

---

## 5. Exploring the Services

Let's explore each service through its web interface.

### 5.1: Airflow - The Orchestrator

**Access:** http://localhost:8080
**Login:** admin / admin

#### What you see:

1. **DAGs page** - List of all pipelines (DAGs = Directed Acyclic Graphs)
2. **Toggle** - Enable/disable each DAG
3. **Schedule** - When the DAG runs (cron expression)
4. **Last Run** - Status of most recent execution

#### Try it:

1. Find `example_hello_world` in the DAG list
2. Toggle it ON (if not already)
3. Click the play button (▶) to trigger a run
4. Click the DAG name to see the execution graph

#### Understanding the DAG View:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Task 1    │───▶│   Task 2    │───▶│   Task 3    │
│  (extract)  │    │ (transform) │    │   (load)    │
└─────────────┘    └─────────────┘    └─────────────┘

Colors:
🟢 Green = Success
🔴 Red = Failed
🟡 Yellow = Running
⚪ Light green = Queued
```

### 5.2: MinIO - Object Storage

**Access:** http://localhost:9001
**Login:** minio / minio123

#### What you see:

Three buckets organized by data layer:
- **raw** - Bronze layer (original, untouched data)
- **staging** - Silver layer (cleaned data)
- **curated** - Gold layer (business-ready data)

#### Try it:

1. Click on the `raw` bucket
2. Explore the folder structure (organized by source/date)
3. Download a file to see its contents

#### Bucket Structure:

```
raw/
├── postgres/
│   └── customers/
│       └── 2024-01-15/
│           └── customers.parquet
├── csv_uploads/
│   └── sales_data.csv
└── api_responses/
    └── weather_data.json
```

### 5.3: PostgreSQL - The Database

**Connection:**
- Host: localhost
- Port: 5432
- User: airflow
- Password: airflow
- Database: airflow

#### Connect with psql:

```bash
# Using make
make db-shell

# Or directly
docker compose exec postgres psql -U airflow -d airflow
```

#### Explore the schemas:

```sql
-- List all schemas
\dn

-- You'll see:
-- public (raw data)
-- staging (cleaned data)
-- marts (gold layer)
-- metadata (tracking)

-- List tables in staging
\dt staging.*

-- Query a sample table
SELECT * FROM staging.stg_customers LIMIT 5;

-- Exit
\q
```

### 5.4: Superset - Dashboards

**Access:** http://localhost:8088
**Login:** admin / admin (or analyst / analyst123)

#### What you see:

1. **Dashboards** - Pre-built visualizations
2. **Charts** - Individual visualizations
3. **SQL Lab** - Query data directly

#### Try it:

1. Go to **SQL Lab** in the top menu
2. Select the `PostgreSQL - Data Warehouse` connection
3. Run a query:

```sql
SELECT
    date_trunc('day', created_at) as day,
    COUNT(*) as orders
FROM staging.stg_orders
GROUP BY 1
ORDER BY 1;
```

4. Click "Create Chart" to visualize the results

### 5.5: OpenMetadata - Data Catalog

**Access:** http://localhost:8585
**Login:** admin / admin

#### What you see:

1. **Explore** - Browse all data assets
2. **Lineage** - See how data flows between tables
3. **Quality** - Data quality test results

#### Try it:

1. Search for "customers" in the search bar
2. Click on a table to see:
   - Column definitions
   - Data profiling (statistics)
   - Lineage (where data comes from)
   - Sample data

---

## 6. The Medallion Architecture

The medallion architecture is the heart of this platform. Let's understand it deeply.

### What is the Medallion Architecture?

It's a design pattern that organizes data into three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│  BRONZE (Raw)           SILVER (Refined)        GOLD (Ready)    │
│  ─────────────          ──────────────          ─────────────   │
│                                                                  │
│  • Exact copy of        • Cleaned data          • Aggregated    │
│    source data          • Validated types       • Business      │
│  • No processing        • Deduplicated            metrics       │
│  • Append-only          • Standardized          • Ready for     │
│  • Full history           formats                 reporting     │
│                                                                  │
│  Location:              Location:               Location:       │
│  MinIO "raw" bucket     PostgreSQL staging      PostgreSQL      │
│                         schema                  marts schema    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Why Use This Architecture?

1. **Traceability** - You can always go back to raw data
2. **Reprocessing** - If logic changes, re-run from bronze
3. **Debugging** - Find where data issues originated
4. **Separation** - Different teams work on different layers

### Example: Customer Data Journey

Let's follow customer data through all three layers:

#### Bronze Layer (Raw)

```json
// File: raw/source_system/customers/2024-01-15/data.json
{
  "customer_id": "CUST001",
  "full_name": "  John Smith  ",
  "email": "JOHN@EXAMPLE.COM",
  "created_date": "15/01/2024",
  "is_active": "yes"
}
```

**Note the issues:**
- Extra spaces in name
- Uppercase email
- Non-standard date format
- "yes" instead of boolean

#### Silver Layer (Cleaned)

```sql
-- Table: staging.stg_customers
SELECT * FROM staging.stg_customers WHERE customer_id = 'CUST001';

-- Result:
customer_id | full_name   | email              | created_date | is_active
------------|-------------|--------------------|--------------|-----------
CUST001     | John Smith  | john@example.com   | 2024-01-15   | true
```

**Improvements:**
- Trimmed whitespace
- Lowercase email
- Standard date format (ISO 8601)
- Boolean value

#### Gold Layer (Aggregated)

```sql
-- Table: marts.dim_customers
SELECT * FROM marts.dim_customers WHERE customer_key = 1;

-- Result:
customer_key | customer_id | full_name  | email            | signup_date | total_orders | lifetime_value | customer_tier
-------------|-------------|------------|------------------|-------------|--------------|----------------|---------------
1            | CUST001     | John Smith | john@example.com | 2024-01-15  | 15           | 1250.00        | Gold
```

**Enhancements:**
- Surrogate key added
- Aggregated metrics (total_orders, lifetime_value)
- Derived attributes (customer_tier)

### Hands-On: Trace Data Through the Layers

Let's actually see this in action:

```bash
# 1. Check Bronze layer (MinIO)
# Open http://localhost:9001, browse "raw" bucket

# 2. Check Silver layer (PostgreSQL)
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM staging.stg_customers;"

# 3. Check Gold layer (PostgreSQL)
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM marts.dim_customers;"
```

---

## 7. Your First Pipeline: Hello World

Let's run and understand a simple pipeline.

### What is a DAG?

**DAG** stands for **Directed Acyclic Graph**:
- **Directed** - Tasks flow in one direction
- **Acyclic** - No loops (Task A can't depend on Task C if Task C depends on Task A)
- **Graph** - Tasks connected by dependencies

### The Hello World DAG

Location: `pipelines/dags/examples/example_hello_world.py`

Let's examine it:

```python
"""
This DAG demonstrates basic Airflow concepts.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# DAG Configuration
# This defines HOW and WHEN the DAG runs
default_args = {
    'owner': 'data-platform',           # Who owns this DAG
    'depends_on_past': False,           # Don't wait for previous runs
    'email_on_failure': False,          # Don't send emails on failure
    'retries': 1,                       # Retry once if failed
    'retry_delay': timedelta(minutes=5), # Wait 5 min before retry
}

# Create the DAG
with DAG(
    dag_id='example_hello_world',        # Unique identifier
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=None,              # Manual trigger only
    start_date=datetime(2024, 1, 1),     # When DAG becomes active
    catchup=False,                       # Don't run for past dates
    tags=['example'],                    # For filtering in UI
) as dag:

    # Task 1: Print hello using Bash
    say_hello = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Airflow!"',
    )

    # Task 2: Get current time using Python
    def get_current_time(**context):
        """Python function that runs as a task."""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Current time is: {current_time}")

        # Push value to XCom (cross-task communication)
        context['task_instance'].xcom_push(
            key='execution_time',
            value=current_time
        )
        return current_time

    report_time = PythonOperator(
        task_id='report_time',
        python_callable=get_current_time,
        provide_context=True,
    )

    # Task 3: Print goodbye
    say_goodbye = BashOperator(
        task_id='say_goodbye',
        bash_command='echo "Goodbye! Execution completed."',
    )

    # Define task order (dependencies)
    # say_hello runs first, then report_time, then say_goodbye
    say_hello >> report_time >> say_goodbye
```

### Concepts Explained

#### 1. Operators
Operators define what a task does:

| Operator | Purpose | Example |
|----------|---------|---------|
| `BashOperator` | Run shell commands | `echo "hello"` |
| `PythonOperator` | Run Python functions | Call any function |
| `PostgresOperator` | Run SQL queries | INSERT, SELECT |
| Custom Operators | Platform-specific tasks | Upload to MinIO |

#### 2. Task Dependencies
The `>>` operator sets execution order:

```python
task_a >> task_b >> task_c  # A, then B, then C

# Or multiple dependencies:
task_a >> [task_b, task_c]  # A, then B and C in parallel
[task_b, task_c] >> task_d  # B and C, then D
```

#### 3. XCom (Cross-Communication)
Tasks can pass data to each other:

```python
# Push (in task A)
context['task_instance'].xcom_push(key='my_key', value='my_value')

# Pull (in task B)
value = context['task_instance'].xcom_pull(task_ids='task_a', key='my_key')
```

### Run the Hello World DAG

#### Step 1: Enable the DAG

```bash
# Using Airflow CLI
make airflow-cli CMD="dags unpause example_hello_world"
```

Or in the UI: Toggle the switch next to the DAG name.

#### Step 2: Trigger the DAG

```bash
# Using CLI
make airflow-cli CMD="dags trigger example_hello_world"
```

Or in the UI: Click the play button (▶).

#### Step 3: Watch the Execution

1. Go to http://localhost:8080
2. Click on `example_hello_world`
3. Click on the latest run
4. Watch tasks turn green as they complete

#### Step 4: View the Logs

1. Click on a task (e.g., `say_hello`)
2. Click "Log" tab
3. See the output:

```
[2024-01-15 10:30:00] INFO - Hello from Airflow!
```

### Exercise: Modify the DAG

Try adding a new task:

1. Open `pipelines/dags/examples/example_hello_world.py`
2. Add a new task:

```python
    # Add this after say_goodbye
    count_to_five = BashOperator(
        task_id='count_to_five',
        bash_command='for i in 1 2 3 4 5; do echo "Count: $i"; sleep 1; done',
    )

    # Update the dependency chain
    say_hello >> report_time >> say_goodbye >> count_to_five
```

3. Save the file
4. Wait ~30 seconds (Airflow auto-detects changes)
5. Trigger the DAG again and watch the new task

---

## 8. Data Ingestion: From Source to Bronze

Now let's understand how data enters the platform.

### What is Data Ingestion?

Data ingestion is the process of:
1. **Extracting** data from a source
2. **Transforming** it to a storage format (Parquet)
3. **Loading** it into the Bronze layer (MinIO)

### The Ingestion Pipeline

Location: `pipelines/dags/ingestion/sample_batch_ingestion.py`

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Source     │────▶│  Transform   │────▶│    MinIO     │
│  (Postgres)  │     │  (Parquet)   │     │  (Bronze)    │
└──────────────┘     └──────────────┘     └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Metadata   │
                    │   Logging    │
                    └──────────────┘
```

### Key Ingestion Concepts

#### 1. Schema Detection

Automatically infer data types from files:

```python
from pipelines.ingestion.schema_detector import detect_csv_schema

# Detect schema from CSV
schema = detect_csv_schema('/path/to/data.csv')

# Result:
# {
#     'customer_id': 'string',
#     'amount': 'float64',
#     'order_date': 'datetime64',
#     'is_active': 'bool'
# }
```

#### 2. Full vs Incremental Extraction

**Full extraction** - Get all data every time:
```python
from pipelines.ingestion.database_ingestor import extract_table_full

extract_table_full(
    table_name='customers',
    schema='public',
    output_path='postgres/customers/full.parquet'
)
```

**Incremental extraction** - Get only new/changed data:
```python
from pipelines.ingestion.database_ingestor import extract_table_incremental

extract_table_incremental(
    table_name='orders',
    schema='public',
    watermark_column='updated_at',  # Track changes
    output_path='postgres/orders/incremental.parquet'
)
```

#### 3. Watermark Management

Watermarks track "where we left off":

```python
from pipelines.ingestion.watermark_manager import WatermarkManager

wm = WatermarkManager()

# Get last processed timestamp
last_watermark = wm.get_watermark('orders')
# Returns: 2024-01-14 23:59:59

# After processing new data
wm.set_watermark('orders', '2024-01-15 12:00:00')
```

#### 4. Storage Layers

The `storage.py` utility handles uploads:

```python
from pipelines.utils.storage import upload_to_layer, StorageLayer

# Upload to Bronze
upload_to_layer(
    local_path='/tmp/data.parquet',
    layer=StorageLayer.BRONZE,  # Goes to "raw" bucket
    object_key='postgres/orders/2024-01-15/data.parquet'
)

# Upload to Silver
upload_to_layer(
    local_path='/tmp/cleaned.parquet',
    layer=StorageLayer.SILVER,  # Goes to "staging" bucket
    object_key='orders/cleaned.parquet'
)
```

### Hands-On: Run an Ingestion Pipeline

#### Step 1: Generate Sample Data

```bash
# Generate sample CSV files
python scripts/generate_sample_input.py

# This creates files in sample_data/
ls sample_data/
# customers.csv  orders.csv  products.csv
```

#### Step 2: Upload Sample Data

```bash
# Upload to MinIO bronze layer
make minio-upload-sample

# Or manually:
python scripts/upload_sample_data.py
```

#### Step 3: Verify in MinIO

1. Go to http://localhost:9001
2. Navigate to `raw` bucket
3. Find your uploaded files

#### Step 4: Run the Ingestion DAG

```bash
# Enable and trigger
make airflow-cli CMD="dags unpause sample_batch_ingestion"
make airflow-cli CMD="dags trigger sample_batch_ingestion"
```

#### Step 5: Check the Results

```bash
# Check metadata table
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM metadata.ingestion_log ORDER BY ingested_at DESC LIMIT 5;"
```

### Exercise: Ingest Your Own Data

1. Create a CSV file:

```bash
cat > sample_data/my_data.csv << 'EOF'
id,name,value,created_at
1,Item A,100.50,2024-01-15
2,Item B,200.75,2024-01-15
3,Item C,50.25,2024-01-16
EOF
```

2. Create a simple ingestion script:

```python
# scripts/my_ingestion.py
from pipelines.utils.minio_client import get_minio_client
from pipelines.ingestion.schema_detector import detect_csv_schema
import pandas as pd

# Detect schema
schema = detect_csv_schema('sample_data/my_data.csv')
print(f"Detected schema: {schema}")

# Read and convert to parquet
df = pd.read_csv('sample_data/my_data.csv')
df.to_parquet('/tmp/my_data.parquet')

# Upload to MinIO
client = get_minio_client()
client.fput_object(
    'raw',
    'my_uploads/my_data.parquet',
    '/tmp/my_data.parquet'
)
print("Upload complete!")
```

3. Run it:
```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/my_ingestion.py
```

---

## 9. Data Transformation with dbt

dbt (data build tool) transforms raw data into analytics-ready tables using SQL.

### What is dbt?

dbt lets you:
- Write transformations in SQL
- Organize code into models (reusable pieces)
- Test your data automatically
- Document everything
- Track data lineage

### dbt Project Structure

```
transformations/dbt_project/
├── dbt_project.yml      # Project configuration
├── profiles.yml         # Database connection
├── models/
│   ├── staging/         # Silver layer models
│   │   ├── stg_customers.sql
│   │   └── stg_orders.sql
│   ├── intermediate/    # Business logic
│   │   └── int_customer_orders.sql
│   └── marts/           # Gold layer (final)
│       ├── dim_customers.sql
│       └── fct_orders.sql
├── tests/               # Data tests
└── macros/              # Reusable SQL snippets
```

### Understanding dbt Models

A model is just a SQL SELECT statement that dbt turns into a table or view.

#### Staging Model (Silver Layer)

File: `models/staging/stg_customers.sql`

```sql
-- Configure this model
{{
  config(
    materialized='table',
    schema='staging'
  )
}}

-- Transform raw data
SELECT
    -- Clean the customer_id
    TRIM(customer_id) AS customer_id,

    -- Standardize name
    INITCAP(TRIM(full_name)) AS full_name,

    -- Lowercase email
    LOWER(TRIM(email)) AS email,

    -- Parse date
    CAST(created_date AS DATE) AS created_date,

    -- Convert to boolean
    CASE
        WHEN LOWER(is_active) IN ('yes', 'true', '1') THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Add metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM {{ source('raw', 'customers') }}

-- Filter out invalid records
WHERE customer_id IS NOT NULL
```

#### Intermediate Model (Business Logic)

File: `models/intermediate/int_customer_orders.sql`

```sql
{{
  config(
    materialized='table',
    schema='intermediate'
  )
}}

-- Join customers with their orders
SELECT
    c.customer_id,
    c.full_name,
    o.order_id,
    o.order_date,
    o.total_amount,
    o.status

FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o
    ON c.customer_id = o.customer_id
```

#### Mart Model (Gold Layer)

File: `models/marts/dim_customers.sql`

```sql
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- Final customer dimension with metrics
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        MAX(order_date) AS last_order_date
    FROM {{ ref('int_customer_orders') }}
    WHERE order_id IS NOT NULL
    GROUP BY customer_id
)

SELECT
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,

    c.customer_id,
    c.full_name,
    c.email,
    c.created_date AS signup_date,
    c.is_active,

    -- Metrics from orders
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.lifetime_value, 0) AS lifetime_value,
    o.last_order_date,

    -- Derived tier
    CASE
        WHEN COALESCE(o.lifetime_value, 0) >= 1000 THEN 'Gold'
        WHEN COALESCE(o.lifetime_value, 0) >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier

FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_orders o
    ON c.customer_id = o.customer_id
```

### Key dbt Concepts

#### 1. ref() Function

References another model:

```sql
-- This creates a dependency
SELECT * FROM {{ ref('stg_customers') }}

-- dbt knows to run stg_customers first
```

#### 2. source() Function

References raw data:

```sql
-- Defined in models/sources.yml
SELECT * FROM {{ source('raw', 'customers') }}
```

#### 3. Materializations

How the model is stored:

| Type | Description | Use When |
|------|-------------|----------|
| `view` | Creates a view | Small, frequently changing data |
| `table` | Creates a table | Large data, needs performance |
| `incremental` | Appends new rows | Very large, append-only data |
| `ephemeral` | Not stored (CTE) | Intermediate calculations |

#### 4. Tests

Validate your data:

```yaml
# models/staging/schema.yml
version: 2

models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null

      - name: email
        tests:
          - unique
          - not_null

      - name: customer_tier
        tests:
          - accepted_values:
              values: ['Bronze', 'Silver', 'Gold']
```

### Hands-On: Run dbt

#### Step 1: Run All Models

```bash
# Using the Airflow DAG
make airflow-cli CMD="dags trigger dbt_transform"

# Or run dbt directly
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt run"
```

#### Step 2: Run Tests

```bash
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt test"
```

#### Step 3: Generate Documentation

```bash
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt docs generate"
```

#### Step 4: View Results

```sql
-- Connect to PostgreSQL
docker compose exec postgres psql -U airflow -d airflow

-- Check staging table
SELECT * FROM staging.stg_customers LIMIT 5;

-- Check mart table
SELECT * FROM marts.dim_customers LIMIT 5;

-- See the transformation
SELECT customer_tier, COUNT(*)
FROM marts.dim_customers
GROUP BY customer_tier;
```

### Exercise: Create Your Own Model

1. Create a new model file:

```bash
cat > transformations/dbt_project/models/marts/mart_daily_sales.sql << 'EOF'
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- Daily sales summary
SELECT
    DATE(order_date) AS order_day,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_order_value

FROM {{ ref('stg_orders') }}
WHERE status = 'completed'
GROUP BY DATE(order_date)
ORDER BY order_day
EOF
```

2. Run your new model:

```bash
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt run --select mart_daily_sales"
```

3. Query the results:

```bash
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM marts.mart_daily_sales ORDER BY order_day DESC LIMIT 10;"
```

---

## 10. Data Quality with Great Expectations

Great Expectations (GE) validates that your data meets quality standards.

### What is Great Expectations?

GE lets you:
- Define "expectations" (rules) for your data
- Validate data against those expectations
- Get detailed reports on failures
- Block bad data from moving forward

### How It Works

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Data        │────▶│ Expectations │────▶│  Validation  │
│  (Table/File)│     │  (Rules)     │     │  Results     │
└──────────────┘     └──────────────┘     └──────────────┘
                                                 │
                                          ┌──────┴──────┐
                                          │             │
                                          ▼             ▼
                                      ✅ Pass      ❌ Fail
                                    (Continue)   (Alert/Stop)
```

### Types of Expectations

#### Column-Level Expectations

```python
# Column should never have nulls
expect_column_values_to_not_be_null(column='customer_id')

# Column should be unique
expect_column_values_to_be_unique(column='email')

# Values should be in a set
expect_column_values_to_be_in_set(
    column='status',
    value_set=['pending', 'completed', 'cancelled']
)

# Values should match pattern
expect_column_values_to_match_regex(
    column='email',
    regex=r'^[\w\.-]+@[\w\.-]+\.\w+$'
)
```

#### Table-Level Expectations

```python
# Table should have data
expect_table_row_count_to_be_between(min_value=1, max_value=1000000)

# Table should have expected columns
expect_table_columns_to_match_set(
    column_set=['id', 'name', 'email', 'created_at']
)
```

### Expectation Suite Structure

Location: `data-quality/great_expectations/expectations/`

```yaml
# expectations/raw/customers_suite.json
{
  "expectation_suite_name": "customers_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "customer_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {
        "column": "customer_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "email",
        "regex": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$",
        "mostly": 0.95
      }
    }
  ]
}
```

### Checkpoints

Checkpoints group expectations and run them together:

```yaml
# checkpoints/raw_data_checkpoint.yml
name: raw_data_checkpoint
config_version: 1.0
class_name: Checkpoint
run_name_template: "%Y%m%d-%H%M%S-raw-validation"

validations:
  - batch_request:
      datasource_name: postgres_datasource
      data_asset_name: raw_customers
    expectation_suite_name: customers_suite

  - batch_request:
      datasource_name: postgres_datasource
      data_asset_name: raw_orders
    expectation_suite_name: orders_suite

action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction

  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
```

### Hands-On: Run Data Quality Checks

#### Step 1: Run the Quality DAG

```bash
# Enable and trigger
make airflow-cli CMD="dags unpause data_quality_checks"
make airflow-cli CMD="dags trigger data_quality_checks"
```

#### Step 2: Check the Results

```bash
# View validation results in database
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM data_quality.validation_results ORDER BY run_time DESC LIMIT 5;"
```

#### Step 3: View Data Docs

Great Expectations generates HTML reports:

```bash
# Find the data docs
ls data-quality/great_expectations/uncommitted/data_docs/local_site/

# Open in browser (or copy the path)
```

### Exercise: Add a Custom Expectation

1. Open the DAG file: `pipelines/dags/quality/data_quality_checks.py`

2. Add a simple SQL-based check:

```python
# Add this task to the DAG
from pipelines.operators.data_quality_operator import DataQualityCheckOperator

check_no_negative_amounts = DataQualityCheckOperator(
    task_id='check_no_negative_amounts',
    sql="""
        SELECT COUNT(*) as failures
        FROM staging.stg_orders
        WHERE total_amount < 0
    """,
    pass_value=0,  # Expect 0 failures
    conn_id='postgres_default',
)
```

3. Run the updated DAG and verify the check passes

---

## 11. Creating Dashboards with Superset

Let's create visualizations from our gold layer data.

### Understanding Superset

Superset has three main components:

1. **Datasets** - Tables/views you can query
2. **Charts** - Individual visualizations
3. **Dashboards** - Collections of charts

### Step-by-Step: Create a Dashboard

#### Step 1: Connect to Data

1. Go to http://localhost:8088
2. Login as admin / admin
3. Navigate to **Data** → **Datasets**
4. Click **+ Dataset**
5. Select:
   - Database: `PostgreSQL - Data Warehouse`
   - Schema: `marts`
   - Table: `dim_customers`
6. Click **Add**

#### Step 2: Create Your First Chart

1. Go to **Charts** → **+ Chart**
2. Select dataset: `marts.dim_customers`
3. Choose chart type: **Pie Chart**
4. Configure:
   - Dimensions: `customer_tier`
   - Metric: `COUNT(*)`
5. Click **Update Chart**
6. Click **Save** → Name it "Customer Tiers"

#### Step 3: Create More Charts

**Bar Chart - Orders by Day**

1. Create new dataset for `marts.fct_orders` (or use existing)
2. Create chart:
   - Type: **Bar Chart**
   - X-Axis: `order_date` (by day)
   - Metric: `SUM(total_amount)`
   - Name: "Daily Revenue"

**Big Number - Total Revenue**

1. Create chart:
   - Type: **Big Number**
   - Metric: `SUM(total_amount)`
   - Name: "Total Revenue"

**Table - Top Customers**

1. Create chart:
   - Type: **Table**
   - Columns: `full_name`, `total_orders`, `lifetime_value`, `customer_tier`
   - Order by: `lifetime_value DESC`
   - Row limit: 10
   - Name: "Top 10 Customers"

#### Step 4: Build the Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it "Sales Overview"
3. Drag your charts onto the canvas:

```
┌─────────────────────────────────────────────────────────┐
│ ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│ │ Total Revenue│  │ Total Orders │  │  Customers   │   │
│ │   $125,000   │  │    1,234     │  │     567      │   │
│ └──────────────┘  └──────────────┘  └──────────────┘   │
│                                                         │
│ ┌─────────────────────────┐  ┌─────────────────────┐   │
│ │    Daily Revenue        │  │   Customer Tiers    │   │
│ │    [Bar Chart]          │  │   [Pie Chart]       │   │
│ └─────────────────────────┘  └─────────────────────┘   │
│                                                         │
│ ┌───────────────────────────────────────────────────┐  │
│ │              Top 10 Customers                      │  │
│ │              [Table]                               │  │
│ └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

4. Click **Save**

#### Step 5: Add Filters

1. Click **Edit Dashboard**
2. Click **+ Filter** in the left panel
3. Add a filter:
   - Column: `customer_tier`
   - Filter type: **Select**
   - Datasets to apply: All
4. Save the filter

Now users can filter all charts by customer tier!

### SQL Lab: Ad-Hoc Queries

For quick analysis without creating charts:

1. Go to **SQL Lab** → **SQL Editor**
2. Run queries directly:

```sql
-- Top revenue days
SELECT
    DATE(order_date) AS day,
    COUNT(*) AS orders,
    SUM(total_amount) AS revenue
FROM marts.fct_orders
GROUP BY DATE(order_date)
ORDER BY revenue DESC
LIMIT 10;
```

3. Click **Explore** to turn results into a chart

### Exercise: Create a Customer Trends Dashboard

Create a dashboard showing:

1. **New customers per month** (bar chart)
2. **Customer tier distribution over time** (stacked area)
3. **Average order value by tier** (grouped bar)
4. **Customer retention** (metric tracking repeat orders)

Hint: You may need to create a new SQL-based dataset with custom aggregations.

---

## 12. Data Governance with OpenMetadata

OpenMetadata helps you understand and manage your data assets.

### What is Data Governance?

Data governance includes:
- **Cataloging** - What data exists and where
- **Lineage** - Where data comes from and goes
- **Quality** - Is the data trustworthy
- **Ownership** - Who is responsible
- **Access** - Who can see what

### Exploring OpenMetadata

#### Step 1: Access the Catalog

1. Go to http://localhost:8585
2. Login: admin / admin

#### Step 2: Search for Data

1. Use the search bar to find "customers"
2. Click on a table to see:
   - **Schema** - Column names and types
   - **Profile** - Statistics (nulls, unique values)
   - **Sample Data** - Preview of actual data
   - **Lineage** - Upstream/downstream dependencies

#### Step 3: View Lineage

The lineage graph shows data flow:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  raw.       │────▶│  staging.   │────▶│  marts.     │
│  customers  │     │  stg_       │     │  dim_       │
│  (Bronze)   │     │  customers  │     │  customers  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Click on any node to see its details.

#### Step 4: Add Documentation

1. Click on a table
2. Click **Edit**
3. Add:
   - Description
   - Tags
   - Owner
   - Tier (importance level)

#### Step 5: Set Up Ownership

1. Go to **Settings** → **Users**
2. Create users for your team
3. Assign tables to owners
4. Set up teams for group ownership

### Syncing Metadata from Airflow

The governance DAG syncs metadata automatically:

```bash
# Run the metadata sync
make airflow-cli CMD="dags trigger metadata_sync"
```

This updates OpenMetadata with:
- New tables from PostgreSQL
- dbt model descriptions
- Lineage from dbt

### Exercise: Document Your Data

1. Find the `dim_customers` table in OpenMetadata
2. Add:
   - A description explaining what the table contains
   - Tags like "PII" (personally identifiable information) for the email column
   - Owner (assign yourself)
   - Tier: "Tier1" (business-critical)

3. Add glossary terms:
   - Create a glossary term "Customer Lifetime Value"
   - Link it to the `lifetime_value` column

---

## 13. Building Your Own Pipeline

Now let's put it all together and build a complete pipeline from scratch.

### The Scenario

You need to:
1. Ingest sales data from a CSV file
2. Clean and validate the data
3. Transform it into analytics-ready tables
4. Create a dashboard

### Step 1: Create Sample Data

```bash
cat > sample_data/sales_data.csv << 'EOF'
transaction_id,customer_id,product_name,quantity,unit_price,sale_date,region
TXN001,CUST001,Widget A,5,19.99,2024-01-15,North
TXN002,CUST002,Widget B,3,29.99,2024-01-15,South
TXN003,CUST001,Widget C,2,49.99,2024-01-16,North
TXN004,CUST003,Widget A,10,19.99,2024-01-16,East
TXN005,CUST002,Widget B,1,29.99,2024-01-17,South
TXN006,CUST004,Widget D,4,39.99,2024-01-17,West
TXN007,CUST001,Widget A,3,19.99,2024-01-18,North
TXN008,CUST005,Widget C,6,49.99,2024-01-18,East
EOF
```

### Step 2: Create the Ingestion DAG

Create file: `pipelines/dags/ingestion/sales_ingestion.py`

```python
"""
Sales Data Ingestion Pipeline
Ingests sales CSV data into the Bronze layer
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from pipelines.utils.storage import upload_to_layer, StorageLayer
from pipelines.ingestion.schema_detector import detect_csv_schema
from pipelines.ingestion.metadata_logger import log_ingestion_success

import pandas as pd
import os

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Path configuration
SOURCE_FILE = '/opt/airflow/sample_data/sales_data.csv'
BRONZE_PATH = 'sales/transactions'


def detect_and_validate_schema(**context):
    """Detect schema and validate source file exists."""
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    schema = detect_csv_schema(SOURCE_FILE)
    context['task_instance'].xcom_push(key='schema', value=schema)

    # Count rows
    df = pd.read_csv(SOURCE_FILE)
    row_count = len(df)
    context['task_instance'].xcom_push(key='row_count', value=row_count)

    print(f"Detected schema: {schema}")
    print(f"Row count: {row_count}")
    return schema


def convert_to_parquet(**context):
    """Convert CSV to Parquet format."""
    df = pd.read_csv(SOURCE_FILE)

    # Basic cleaning
    df['transaction_id'] = df['transaction_id'].str.strip()
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['total_amount'] = df['quantity'] * df['unit_price']

    # Save as parquet
    output_path = '/tmp/sales_transactions.parquet'
    df.to_parquet(output_path, index=False)

    context['task_instance'].xcom_push(key='local_path', value=output_path)
    print(f"Converted to parquet: {output_path}")
    return output_path


def upload_to_bronze(**context):
    """Upload parquet file to Bronze layer."""
    local_path = context['task_instance'].xcom_pull(
        task_ids='convert_to_parquet',
        key='local_path'
    )

    # Create dated path
    today = datetime.now().strftime('%Y-%m-%d')
    object_key = f"{BRONZE_PATH}/{today}/transactions.parquet"

    upload_to_layer(
        local_path=local_path,
        layer=StorageLayer.BRONZE,
        object_key=object_key
    )

    context['task_instance'].xcom_push(key='bronze_path', value=object_key)
    print(f"Uploaded to Bronze: {object_key}")
    return object_key


def log_metadata(**context):
    """Log ingestion metadata."""
    row_count = context['task_instance'].xcom_pull(
        task_ids='detect_schema',
        key='row_count'
    )
    bronze_path = context['task_instance'].xcom_pull(
        task_ids='upload_to_bronze',
        key='bronze_path'
    )

    log_ingestion_success(
        source_name='sales_data.csv',
        destination_path=bronze_path,
        row_count=row_count,
        file_format='parquet'
    )
    print(f"Logged ingestion: {row_count} rows to {bronze_path}")


with DAG(
    dag_id='sales_ingestion',
    default_args=default_args,
    description='Ingest sales CSV data into Bronze layer',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'sales', 'tutorial'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting sales ingestion pipeline"',
    )

    detect_schema = PythonOperator(
        task_id='detect_schema',
        python_callable=detect_and_validate_schema,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet,
    )

    upload_to_bronze = PythonOperator(
        task_id='upload_to_bronze',
        python_callable=upload_to_bronze,
    )

    log_metadata = PythonOperator(
        task_id='log_metadata',
        python_callable=log_metadata,
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Sales ingestion complete!"',
    )

    # Define task order
    start >> detect_schema >> convert_to_parquet >> upload_to_bronze >> log_metadata >> end
```

### Step 3: Create the dbt Model

Create file: `transformations/dbt_project/models/staging/stg_sales.sql`

```sql
{{
  config(
    materialized='table',
    schema='staging'
  )
}}

-- Staging model for sales transactions
SELECT
    transaction_id,
    customer_id,
    TRIM(product_name) AS product_name,
    quantity,
    unit_price,
    quantity * unit_price AS total_amount,
    CAST(sale_date AS DATE) AS sale_date,
    UPPER(TRIM(region)) AS region,
    CURRENT_TIMESTAMP AS _loaded_at

FROM {{ source('bronze', 'sales_transactions') }}

WHERE transaction_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0
```

Create file: `transformations/dbt_project/models/marts/mart_regional_sales.sql`

```sql
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- Regional sales summary
WITH daily_sales AS (
    SELECT
        region,
        sale_date,
        COUNT(*) AS transaction_count,
        SUM(quantity) AS units_sold,
        SUM(total_amount) AS revenue,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM {{ ref('stg_sales') }}
    GROUP BY region, sale_date
)

SELECT
    region,
    sale_date,
    transaction_count,
    units_sold,
    revenue,
    unique_customers,
    revenue / NULLIF(transaction_count, 0) AS avg_transaction_value,

    -- Running totals
    SUM(revenue) OVER (
        PARTITION BY region
        ORDER BY sale_date
    ) AS cumulative_revenue,

    -- Day-over-day change
    revenue - LAG(revenue) OVER (
        PARTITION BY region
        ORDER BY sale_date
    ) AS revenue_change

FROM daily_sales
ORDER BY region, sale_date
```

### Step 4: Add Data Quality Checks

Create file: `data-quality/great_expectations/expectations/sales_suite.json`

```json
{
  "expectation_suite_name": "sales_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "transaction_id"}
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {"column": "transaction_id"}
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "quantity",
        "min_value": 1,
        "max_value": 1000
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "unit_price",
        "min_value": 0.01,
        "max_value": 10000
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "region",
        "value_set": ["NORTH", "SOUTH", "EAST", "WEST"]
      }
    }
  ]
}
```

### Step 5: Run the Complete Pipeline

```bash
# 1. Run the ingestion DAG
make airflow-cli CMD="dags unpause sales_ingestion"
make airflow-cli CMD="dags trigger sales_ingestion"

# 2. Wait for ingestion to complete, then run dbt
make airflow-cli CMD="dags trigger dbt_transform"

# 3. Run data quality checks
make airflow-cli CMD="dags trigger data_quality_checks"
```

### Step 6: Verify the Results

```bash
# Check Bronze layer (MinIO)
# Go to http://localhost:9001 and browse raw/sales/transactions/

# Check staging table
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM staging.stg_sales LIMIT 5;"

# Check mart table
docker compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM marts.mart_regional_sales ORDER BY region, sale_date;"
```

### Step 7: Create a Dashboard

1. Go to Superset (http://localhost:8088)
2. Create dataset for `marts.mart_regional_sales`
3. Create charts:
   - Regional revenue comparison (bar chart)
   - Revenue over time by region (line chart)
   - Total units sold (big number)
4. Combine into a "Regional Sales" dashboard

Congratulations! You've built a complete data pipeline from scratch!

---

## 14. Troubleshooting

### Common Issues and Solutions

#### Services Won't Start

```bash
# Check logs for errors
docker compose logs <service-name>

# Common fixes:
# 1. Increase Docker memory (Docker Desktop settings)
# 2. Remove old volumes and restart
make clean
make start

# 3. Check port conflicts
lsof -i :8080  # Find what's using the port
```

#### Airflow DAG Not Appearing

```bash
# Check DAG parsing errors
make airflow-cli CMD="dags list-import-errors"

# Verify DAG file is valid Python
python -c "import ast; ast.parse(open('path/to/dag.py').read())"

# Force DAG refresh
make airflow-cli CMD="dags reserialize"
```

#### dbt Models Failing

```bash
# Run with debugging
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt run --debug"

# Check database connection
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/transformations/dbt_project && dbt debug"
```

#### MinIO Connection Issues

```bash
# Verify MinIO is running
docker compose ps minio

# Test connection
docker compose exec airflow-webserver python -c "
from pipelines.utils.minio_client import get_minio_client
client = get_minio_client()
print(list(client.list_buckets()))
"
```

#### PostgreSQL Connection Issues

```bash
# Test connection
docker compose exec postgres psql -U airflow -d airflow -c "SELECT 1;"

# Check Airflow connection
make airflow-cli CMD="connections get postgres_default"
```

### Getting Help

1. **Check logs** - Always start with `docker compose logs <service>`
2. **Check Airflow UI** - Task logs show detailed error messages
3. **Documentation** - See `docs/` folder for detailed guides
4. **Runbooks** - See `docs/runbooks/troubleshooting.md`

---

## Next Steps

Now that you've completed this tutorial, here are ways to continue learning:

### Explore More Features

- [ ] Set up email/Slack alerts for pipeline failures
- [ ] Add more data sources (APIs, databases)
- [ ] Create incremental loading pipelines
- [ ] Build complex dbt models with tests
- [ ] Configure role-based access in Superset

### Read the Documentation

- `docs/architecture.md` - Deep dive into system design
- `docs/dag-development-guide.md` - Advanced DAG patterns
- `docs/data-governance-guide.md` - Setting up governance
- `docs/monitoring-guide.md` - Observability and alerting

### Practice Projects

1. **Weather Data Pipeline** - Ingest API data daily
2. **E-commerce Analytics** - Build a complete retail data model
3. **Real-time Dashboard** - Add streaming data updates
4. **Data Quality Framework** - Comprehensive validation suite

### Join the Community

- Airflow: https://airflow.apache.org/community/
- dbt: https://www.getdbt.com/community/
- Great Expectations: https://greatexpectations.io/community/

---

## Summary

In this tutorial, you learned:

1. **Architecture** - How components connect and data flows
2. **Setup** - Running the platform with Docker Compose
3. **Medallion Architecture** - Bronze, Silver, Gold layers
4. **Airflow** - Creating and running DAGs
5. **Ingestion** - Getting data into the platform
6. **dbt** - Transforming data with SQL models
7. **Great Expectations** - Validating data quality
8. **Superset** - Creating dashboards
9. **OpenMetadata** - Cataloging and governance
10. **End-to-End Pipeline** - Building from scratch

You now have the foundation to build production-grade data pipelines!

---

*Happy data engineering!*
