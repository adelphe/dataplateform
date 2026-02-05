# Data Quality Framework

This directory contains the Great Expectations-based data quality validation framework for the data platform.

## Directory Structure

```
data-quality/
├── great_expectations/
│   ├── great_expectations.yml     # Main GE configuration
│   ├── expectations/
│   │   ├── raw/                   # Raw/Bronze layer expectation suites
│   │   │   ├── raw_transactions_suite.json
│   │   │   ├── raw_customers_suite.json
│   │   │   └── raw_products_suite.json
│   │   └── curated/               # Silver/Gold layer expectation suites
│   │       ├── staging_transactions_suite.json
│   │       ├── mart_customer_analytics_suite.json
│   │       ├── mart_product_analytics_suite.json
│   │       └── mart_daily_summary_suite.json
│   ├── checkpoints/               # Validation checkpoint configurations
│   │   ├── raw_data_checkpoint.yml
│   │   ├── staging_checkpoint.yml
│   │   ├── marts_checkpoint.yml
│   │   └── full_pipeline_checkpoint.yml
│   ├── plugins/                   # Custom expectations and utilities
│   │   ├── custom_expectations.py
│   │   ├── validation_store.py
│   │   └── reporting.py
│   └── uncommitted/               # Local-only files (gitignored)
│       ├── config_variables.yml
│       ├── validations/
│       └── data_docs/
├── init_validation_tables.sql     # PostgreSQL setup script
└── README.md
```

## Setup

### 1. Install Dependencies

```bash
pip install great_expectations sqlalchemy psycopg2-binary pyarrow
```

### 2. Initialize PostgreSQL Tables

Run the initialization script to create validation tracking tables:

```bash
psql -h localhost -U postgres -d dataplatform -f init_validation_tables.sql
```

### 3. Configure Environment Variables

Set the following environment variables or update `uncommitted/config_variables.yml`:

```bash
export POSTGRES_CONNECTION_STRING="postgresql+psycopg2://user:pass@host:5432/db"
export BRONZE_DATA_PATH="/data/bronze/"
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minio"
export MINIO_SECRET_KEY="minio123"
export DATA_DOCS_BUCKET="data-quality"
```

## Usage

### Running Validations via Airflow

The `data_quality_checks` DAG runs validations automatically:

```python
# Trigger with default parameters (full validation)
airflow dags trigger data_quality_checks

# Run only raw layer validation
airflow dags trigger data_quality_checks --conf '{"validation_scope": "raw_only"}'

# Run without failing on validation errors
airflow dags trigger data_quality_checks --conf '{"fail_on_validation_error": false}'
```

### Running Validations Manually

```python
import great_expectations as gx
from great_expectations.data_context import FileDataContext

# Initialize context
context = FileDataContext(context_root_dir="/path/to/great_expectations")

# Run a checkpoint
result = context.run_checkpoint(checkpoint_name="raw_data_checkpoint")

# Check results
print(f"Validation success: {result.success}")
```

### Using the GE Operator in DAGs

```python
from operators.ge_operator import GreatExpectationsOperator

validate_data = GreatExpectationsOperator(
    task_id="validate_raw_data",
    checkpoint_name="raw_data_checkpoint",
    ge_root_dir="/opt/airflow/data-quality/great_expectations",
    fail_task_on_validation_failure=True,
)
```

## Expectation Suites

### Raw Layer

| Suite | Description | Key Expectations |
|-------|-------------|------------------|
| `raw_transactions_suite` | Transaction data validation | Unique IDs, valid amounts, currency codes |
| `raw_customers_suite` | Customer data validation | Valid emails, unique IDs, required fields |
| `raw_products_suite` | Product data validation | Valid categories, positive prices |

### Curated Layer

| Suite | Description | Key Expectations |
|-------|-------------|------------------|
| `staging_transactions_suite` | Cleaned transaction data | Standardized currencies, typed timestamps |
| `mart_customer_analytics_suite` | Customer analytics | Unique customers, valid metrics |
| `mart_product_analytics_suite` | Product analytics | Non-negative values, valid aggregations |
| `mart_daily_summary_suite` | Daily summaries | Unique dates, reconciled totals |

## Checkpoints

| Checkpoint | Purpose | Suites Included |
|------------|---------|-----------------|
| `raw_data_checkpoint` | Bronze layer validation | All raw suites |
| `staging_checkpoint` | Silver layer validation | Staging suites |
| `marts_checkpoint` | Gold layer validation | All mart suites |
| `full_pipeline_checkpoint` | End-to-end validation | All suites |

## Custom Expectations

The `plugins/custom_expectations.py` module provides:

- `expect_column_values_to_be_valid_currency_code`: Validates ISO 4217 currency codes
- `expect_column_values_to_be_positive_amount`: Validates positive financial amounts
- `expect_column_timestamp_to_be_recent`: Validates data freshness

## Validation Results Storage

Validation results are stored in PostgreSQL for historical tracking:

- `data_platform.validation_results`: Individual validation run results
- `data_platform.data_quality_alerts`: Alerts for failed validations
- `data_platform.data_quality_metrics`: Aggregated quality metrics
- `data_platform.vw_data_quality_status`: Current quality status view

## Data Docs

Great Expectations generates interactive HTML documentation:

- **Local site**: `uncommitted/data_docs/local_site/index.html`
- **MinIO site**: Published to the `data-quality` bucket

## Monitoring & Alerting

The framework supports:

1. **Airflow Integration**: Failed validations fail the DAG task
2. **PostgreSQL Alerts**: Automatic alert creation on validation failure
3. **Slack Notifications**: Configure webhook in checkpoint actions
4. **Data Docs**: Visual validation reports

## Adding New Expectations

1. Create a new suite JSON file in the appropriate directory
2. Add the suite to the relevant checkpoint
3. Test with a manual checkpoint run
4. Commit and deploy

Example suite structure:

```json
{
  "expectation_suite_name": "my_new_suite",
  "meta": {
    "data_asset_name": "my_table",
    "data_layer": "bronze"
  },
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "id"}
    }
  ]
}
```

## Troubleshooting

### Common Issues

1. **Connection errors**: Check PostgreSQL connection string and credentials
2. **Missing tables**: Run `init_validation_tables.sql`
3. **Checkpoint not found**: Verify checkpoint name matches YAML filename
4. **Data Docs not updating**: Run `context.build_data_docs()`

### Logs

- Airflow task logs: `/opt/airflow/logs/`
- GE validation logs: Check Airflow task output
