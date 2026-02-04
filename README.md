# Data Platform

A modern data platform monorepo with orchestration, transformations, data quality, and infrastructure as code.

## Project Structure

```
.
├── infrastructure/          # IaC and environment configs
│   ├── airflow/             # Custom Airflow Docker image
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── init-connections.sh
│   │   └── airflow.cfg
│   └── minio/               # MinIO bucket initialization
│       └── init-buckets.sh
├── pipelines/               # Airflow DAGs, operators, and pipeline logic
│   ├── __init__.py
│   ├── dags/
│   │   └── examples/       # Example DAG patterns
│   ├── operators/           # Custom Airflow operators
│   ├── utils/               # Shared utility functions
│   ├── config/              # Pipeline configuration
│   ├── tests/               # Unit and integration tests
│   ├── plugins/
│   └── logs/
├── transformations/         # dbt models and macros
├── data-quality/            # Data validation and quality checks
├── scripts/                 # Utility scripts (sample data, verification)
├── docs/                    # Project documentation
├── docker-compose.yml       # Local development services
├── Makefile                 # Common commands
└── .env.example             # Environment variable template
```

## Local Services

| Service            | URL                          | Default Credentials |
|--------------------|------------------------------|---------------------|
| Airflow Webserver  | http://localhost:8080         | admin / admin       |
| Superset           | http://localhost:8088         | admin / admin       |
| MinIO Console      | http://localhost:9001         | minio / minio123    |
| MinIO API          | http://localhost:9000         | -                   |
| PostgreSQL         | localhost:5432                | airflow / airflow   |

## MinIO Storage Layers

The platform uses the medallion architecture for data organization:

| Bucket | Layer | Purpose |
|--------|-------|---------|
| raw | Bronze | Raw ingested data (immutable) |
| staging | Silver | Cleaned and validated data |
| curated | Gold | Business-ready aggregated data |

Buckets are automatically created on platform startup via the `minio-init` service. See `docs/minio-setup.md` for detailed documentation.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Make
- Python 3.10+ (for local development)

### Setup

```bash
# 1. Clone the repository
git clone <repo-url> && cd dataplateform

# 2. Run initial setup (creates .env from .env.example, sets up directories)
make setup

# 3. Start all services
make start

# 4. Check service status
make status
```

### Common Commands

```bash
make help                # Show all available commands
make start               # Start all services
make stop                # Stop all services
make restart             # Restart all services
make test                # Run all tests
make logs                # Tail logs from all services
make clean               # Stop services and remove volumes
```

### MinIO Commands

```bash
make minio-buckets              # List all buckets and contents
make minio-upload-sample        # Upload sample test data
make minio-verify               # Verify bucket structure
make minio-console              # Show MinIO console URL
```

### Airflow Commands

```bash
make airflow-cli CMD="dags list"                  # Run Airflow CLI commands
make airflow-logs                                  # Tail Airflow logs
make airflow-test-dag DAG=example_hello_world      # Test a specific DAG
make airflow-connections                           # List configured connections
make airflow-reset                                 # Reset Airflow DB (dev only)
```

## Airflow Orchestration

The platform uses Apache Airflow (LocalExecutor) for workflow orchestration with a custom Docker image that includes additional Python packages for data operations.

### Custom Operators

| Operator | Description |
|----------|-------------|
| `MinIOUploadOperator` | Upload files to MinIO (S3-compatible) buckets |
| `PostgresTableSensorOperator` | Wait for a PostgreSQL table to exist with optional row count |
| `DataQualityCheckOperator` | Run SQL-based data quality validations |
| `MinIOFileSensorOperator` | Wait for files to appear in MinIO buckets |

### Example DAGs

| DAG | Description | Schedule |
|-----|-------------|----------|
| `example_hello_world` | Basic Airflow concepts | Manual |
| `example_postgres_to_minio` | Extract from PostgreSQL to MinIO | Daily |
| `example_data_quality` | SQL data quality checks | Daily |
| `example_taskflow_api` | TaskFlow API with `@task` decorator | Manual |
| `example_monitoring` | Platform health monitoring | Every 15 min |

### Creating a New DAG

1. Create a Python file in `pipelines/dags/`
2. Import operators from `operators/` or use built-in providers
3. Define the DAG with `default_args`, `schedule`, and `tags`
4. The scheduler picks up new DAGs within 60 seconds

See `docs/dag-development-guide.md` for detailed instructions.

### Connections

Airflow connections (PostgreSQL, MinIO) are automatically configured during initialization via `infrastructure/airflow/init-connections.sh`. To add new connections, update the script and `pipelines/config/connections.yaml`.

### Troubleshooting

- **DAGs not appearing**: Check for import errors in `make airflow-cli CMD="dags list-import-errors"`
- **Connection errors**: Verify connections with `make airflow-connections`
- **Scheduler issues**: Check logs with `make airflow-logs`
- **Reset state**: Run `make airflow-reset` to reset the Airflow database (dev only)

## Steps

### 1. Define Requirements and Strategy
- Identify business objectives and use cases
- Define data sources (databases, APIs, files, streaming)
- Determine data consumers (analysts, data scientists, applications)
- Establish SLAs for data freshness, quality, and availability

### 2. Set Up Infrastructure
- Choose a cloud provider (AWS, GCP, Azure) or on-premise setup
- Provision compute, storage, and networking resources
- Configure Infrastructure as Code (Terraform, Pulumi)
- Set up environments (dev, staging, production)

### 3. Data Ingestion
- Build batch ingestion pipelines (e.g., Airbyte, Fivetran, custom scripts)
- Set up real-time/streaming ingestion (e.g., Kafka, Kinesis, Pub/Sub)
- Implement CDC (Change Data Capture) for database sources
- Handle schema detection and evolution

### 4. Data Storage
- Set up a data lake (S3, GCS, ADLS) for raw data
- Deploy a data warehouse (BigQuery, Snowflake, Redshift, Databricks)
- Organize storage layers: raw (bronze), cleaned (silver), curated (gold)
- Define partitioning and file format strategies (Parquet, Delta, Iceberg)

### 5. Data Transformation
- Choose a transformation framework (dbt, Spark, SQL)
- Build staging models to clean and standardize raw data
- Create intermediate and mart models for business logic
- Implement incremental processing where applicable

### 6. Data Orchestration
- Set up a workflow orchestrator (Airflow, Dagster, Prefect, Mage)
- Define DAGs for ingestion, transformation, and export pipelines
- Configure scheduling, retries, and alerting
- Manage dependencies between pipelines

### 7. Data Quality and Testing
- Implement data validation checks (great_expectations, dbt tests, Soda)
- Monitor for schema changes, null rates, row counts, and freshness
- Set up anomaly detection on key metrics
- Define data contracts between producers and consumers

### 8. Data Governance and Cataloging
- Deploy a data catalog (DataHub, OpenMetadata, Atlan)
- Document datasets, ownership, and lineage
- Implement access controls and role-based permissions
- Ensure compliance (GDPR, HIPAA) and PII handling

### 9. Data Serving and Consumption
- Expose curated data to BI tools (Metabase, Looker, Superset, Power BI)
- Build APIs or reverse ETL pipelines for operational use cases
- Set up semantic/metrics layers for consistent definitions
- Enable self-service analytics for business users

### 10. Monitoring and Observability
- Monitor pipeline health, latency, and failures
- Track data freshness and SLA compliance
- Set up dashboards for platform operational metrics
- Integrate alerting with Slack, PagerDuty, or email

### 11. CI/CD and Version Control
- Version control all pipeline code, transformations, and configs
- Set up CI/CD pipelines for testing and deploying changes
- Implement code review workflows
- Automate environment promotion (dev -> staging -> prod)

### 12. Security
- Encrypt data at rest and in transit
- Manage secrets with a vault (HashiCorp Vault, AWS Secrets Manager)
- Implement network isolation and firewall rules
- Set up audit logging and access monitoring
