# Data Platform Architecture

## Overview

This data platform is a modern, production-ready data infrastructure built on Apache Airflow, implementing the medallion architecture (Bronze-Silver-Gold layers). The platform provides end-to-end capabilities for data ingestion, transformation, quality validation, governance, and visualization.

## System Architecture Diagram

```
                                    ┌─────────────────────────────────────────────────────────────┐
                                    │                     DATA PLATFORM                            │
                                    └─────────────────────────────────────────────────────────────┘

┌──────────────────┐                ┌─────────────────────────────────────────────────────────────┐
│   DATA SOURCES   │                │                    ORCHESTRATION LAYER                      │
├──────────────────┤                │  ┌─────────────────────────────────────────────────────┐   │
│                  │                │  │              Apache Airflow (2.9.3)                  │   │
│  • PostgreSQL    │  ─────────────▶│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐       │   │
│  • APIs          │                │  │  │ Scheduler │  │ Webserver │  │  Workers  │       │   │
│  • CSV/JSON      │                │  │  └───────────┘  └───────────┘  └───────────┘       │   │
│  • Databases     │                │  │                                                     │   │
└──────────────────┘                │  │  DAGs: Ingestion │ Transformation │ Quality │ Sync  │   │
                                    │  └─────────────────────────────────────────────────────┘   │
                                    └───────────────────────────┬─────────────────────────────────┘
                                                                │
                    ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
                    │                                           ▼                                           │
                    │  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
                    │  │                         STORAGE LAYER (MinIO - S3 Compatible)                   │ │
                    │  │                                                                                  │ │
                    │  │   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐           │ │
                    │  │   │   BRONZE (raw)   │──▶│  SILVER (staging)│──▶│  GOLD (curated)  │           │ │
                    │  │   │                  │   │                  │   │                  │           │ │
                    │  │   │  Raw ingested    │   │  Cleaned &       │   │  Business-ready  │           │ │
                    │  │   │  data (immutable)│   │  validated data  │   │  aggregations    │           │ │
                    │  │   └──────────────────┘   └──────────────────┘   └──────────────────┘           │ │
                    │  └─────────────────────────────────────────────────────────────────────────────────┘ │
                    │                                           │                                           │
                    │  ┌────────────────────────────────────────┼────────────────────────────────────────┐ │
                    │  │                                        ▼                                        │ │
                    │  │                         DATA WAREHOUSE (PostgreSQL 16)                          │ │
                    │  │                                                                                  │ │
                    │  │   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │ │
                    │  │   │  raw schema  │──▶│staging schema│──▶│intermediate  │──▶│ marts schema │    │ │
                    │  │   │              │   │              │   │   schema     │   │              │    │ │
                    │  │   │ Loaded from  │   │ dbt staging  │   │ dbt complex  │   │ dbt final    │    │ │
                    │  │   │ Bronze layer │   │ models       │   │ transforms   │   │ outputs      │    │ │
                    │  │   └──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘    │ │
                    │  └─────────────────────────────────────────────────────────────────────────────────┘ │
                    │                                                                                       │
                    └───────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           SUPPORTING SERVICES                                                │
│                                                                                                              │
│  ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐     │
│  │   Apache Superset   │   │   Great Expectations │   │    OpenMetadata     │   │     Alerting        │     │
│  │   (BI & Dashboards) │   │   (Data Quality)     │   │   (Data Catalog)    │   │   (Email + Slack)   │     │
│  │                     │   │                      │   │                     │   │                     │     │
│  │  • Dashboards       │   │  • Expectations      │   │  • Metadata Catalog │   │  • Task failures    │     │
│  │  • SQL Lab          │   │  • Checkpoints       │   │  • Data Lineage     │   │  • Quality alerts   │     │
│  │  • Visualizations   │   │  • Data Docs         │   │  • RBAC             │   │  • SLA monitoring   │     │
│  └─────────────────────┘   └─────────────────────┘   └─────────────────────┘   └─────────────────────┘     │
│                                                                                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Apache Airflow (Orchestration)

**Version**: 2.9.3
**Executor**: LocalExecutor
**Port**: 8080

Airflow serves as the central orchestration hub, managing all data workflows:

| Component | Purpose |
|-----------|---------|
| Scheduler | Triggers DAG runs based on schedules and dependencies |
| Webserver | Provides UI for monitoring and managing workflows |
| Workers | Execute individual tasks (LocalExecutor mode) |

**Key Configuration**:
- Parallelism: 32 concurrent tasks
- Max active runs per DAG: 3
- DAG directory scan interval: 60 seconds
- RBAC enabled for access control

### 2. MinIO (Object Storage)

**Port**: 9000 (API), 9001 (Console)

S3-compatible object storage implementing the medallion architecture:

| Bucket | Layer | Purpose |
|--------|-------|---------|
| `raw` | Bronze | Immutable raw ingested data |
| `staging` | Silver | Cleaned, validated, deduplicated data |
| `curated` | Gold | Business-ready aggregations and marts |

### 3. PostgreSQL (Data Warehouse)

**Version**: 16
**Port**: 5432

Serves dual purposes:
1. **Airflow Metadata**: Stores DAG runs, task instances, connections
2. **Data Warehouse**: Houses transformed data across schemas

| Schema | Purpose |
|--------|---------|
| `public` | Airflow metadata |
| `raw` | Data loaded from Bronze layer |
| `staging` | dbt staging models |
| `intermediate` | dbt intermediate transformations |
| `marts` | Final business-ready tables |
| `data_platform` | Platform metadata (ingestion logs, watermarks, alerts) |

### 4. Apache Superset (Visualization)

**Port**: 8088

Business intelligence and visualization layer:
- Interactive dashboards connected to PostgreSQL marts
- SQL Lab for ad-hoc queries
- Role-based access (Admin, Analyst, Viewer)

### 5. OpenMetadata (Data Governance)

**Port**: 8585

Data catalog and governance platform:
- Metadata extraction from PostgreSQL, MinIO, and dbt
- Data lineage tracking
- RBAC with team-based access
- Glossary and classification management

### 6. Great Expectations (Data Quality)

Validation framework integrated throughout the pipeline:
- Expectation suites for each data layer
- Checkpoint-based validation
- Data Docs for interactive quality reports

## Architectural Patterns

### Medallion Architecture

The platform implements a three-layer medallion architecture:

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    BRONZE     │────▶│    SILVER     │────▶│     GOLD      │
│   (Raw Data)  │     │ (Cleaned Data)│     │  (Analytics)  │
└───────────────┘     └───────────────┘     └───────────────┘
     │                      │                      │
     ▼                      ▼                      ▼
• Immutable           • Deduplicated         • Aggregated
• Source format       • Type-converted       • Business logic
• Append-only         • Validated            • SLA-bound
• Full history        • Schema enforced      • Consumption-ready
```

### Modular Design

```
pipelines/
├── dags/           # Workflow definitions
├── operators/      # Custom Airflow operators
├── utils/          # Shared utility modules
├── ingestion/      # Ingestion-specific modules
└── loaders/        # Data loading modules
```

### Infrastructure as Code

All infrastructure is defined in `docker-compose.yml`, enabling:
- Reproducible local development environments
- Consistent staging/production deployments
- Version-controlled configuration

## Service Dependencies

```
                           ┌──────────────┐
                           │  PostgreSQL  │
                           └──────┬───────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼
   ┌──────────────┐      ┌──────────────┐       ┌──────────────┐
   │   Airflow    │      │   Superset   │       │ OpenMetadata │
   └──────────────┘      └──────────────┘       └──────┬───────┘
          │                                            │
          │                                            ▼
          │                                    ┌──────────────┐
          │                                    │Elasticsearch │
          ▼                                    └──────────────┘
   ┌──────────────┐
   │    MinIO     │
   └──────────────┘
```

## Network Configuration

All services communicate on a shared Docker network (`data-platform`):

| Service | Internal DNS | External Port |
|---------|--------------|---------------|
| PostgreSQL | `postgres:5432` | 5432 |
| MinIO | `minio:9000` | 9000, 9001 |
| Airflow | `airflow-webserver:8080` | 8080 |
| Superset | `superset:8088` | 8088 |
| OpenMetadata | `openmetadata:8585` | 8585 |
| Elasticsearch | `elasticsearch:9200` | 9200 |

## Security Considerations

### Authentication

| Service | Method | Configuration |
|---------|--------|---------------|
| Airflow | Username/Password + RBAC | Environment variables |
| Superset | Username/Password + Roles | Environment variables |
| OpenMetadata | Basic/LDAP/SAML/OIDC | `OPENMETADATA_AUTH_PROVIDER` |
| MinIO | Access Key/Secret Key | Environment variables |
| PostgreSQL | Username/Password | Environment variables |

### Secrets Management

Production deployments should:
1. Generate unique `FERNET_KEY` and `SECRET_KEY` values
2. Use secure passwords (not defaults)
3. Enable HTTPS/TLS for all web interfaces
4. Configure proper CORS settings
5. Use external secrets management (Vault, AWS Secrets Manager)

## Scalability Considerations

### Current Setup (Development/Small Scale)

- LocalExecutor: Tasks run on scheduler node
- Single PostgreSQL instance
- Single MinIO instance

### Production Scaling Options

1. **Airflow**: Switch to CeleryExecutor or KubernetesExecutor
2. **PostgreSQL**: Use managed service (RDS, Cloud SQL) with read replicas
3. **MinIO**: Deploy in distributed mode or use managed S3
4. **Superset**: Scale horizontally with load balancer
5. **OpenMetadata**: Deploy on Kubernetes with horizontal pod autoscaling

## Technology Stack Summary

| Layer | Technology | Version |
|-------|------------|---------|
| Orchestration | Apache Airflow | 2.9.3 |
| Object Storage | MinIO | latest |
| Data Warehouse | PostgreSQL | 16 |
| Transformation | dbt | 1.x |
| Data Quality | Great Expectations | 0.x |
| Visualization | Apache Superset | latest |
| Data Catalog | OpenMetadata | 1.x |
| Search Backend | Elasticsearch | 8.x |
| Containerization | Docker Compose | 3.8 |

## Related Documentation

- [Data Flow](data-flow.md) - Detailed data flow from ingestion to consumption
- [Developer Guide](developer-guide.md) - Getting started for developers
- [User Guide](user-guide.md) - Guide for data analysts
- [Monitoring Guide](monitoring-guide.md) - Platform observability
- [Data Governance Guide](data-governance-guide.md) - Governance setup
