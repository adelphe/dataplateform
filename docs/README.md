# Data Platform Documentation

Welcome to the Data Platform documentation. This guide will help you understand, use, and extend the platform.

## New to the Platform?

Start with our **step-by-step tutorial** designed for beginners:

| Document | Description |
|----------|-------------|
| [**Getting Started Tutorial**](./tutorial/getting-started-tutorial.md) | Complete walkthrough from setup to building pipelines |
| [Quick Reference](./tutorial/quick-reference.md) | One-page cheat sheet for commands and URLs |
| [Glossary](./tutorial/glossary.md) | Definitions of key terms and concepts |

## Documentation Overview

### Core Documentation

| Document | Audience | Description |
|----------|----------|-------------|
| [Architecture](./architecture.md) | All | System design, components, and how they connect |
| [Data Flow](./data-flow.md) | All | How data moves through the platform |
| [Developer Guide](./developer-guide.md) | Developers | Setting up development environment, coding standards |
| [User Guide](./user-guide.md) | Analysts | Using Superset and querying data |

### Component Guides

| Guide | Description |
|-------|-------------|
| [Airflow Setup](./airflow-setup.md) | Configuring and managing Apache Airflow |
| [MinIO Setup](./minio-setup.md) | Object storage configuration and usage |
| [Superset Guide](./superset-guide.md) | Creating dashboards and visualizations |
| [Data Governance](./data-governance-guide.md) | Cataloging, lineage, and compliance |
| [Monitoring Guide](./monitoring-guide.md) | Observability, alerting, and health checks |
| [CI/CD](./ci-cd.md) | Continuous integration and deployment |

### Development Guides

| Guide | Description |
|-------|-------------|
| [DAG Development](./dag-development-guide.md) | Creating and testing Airflow DAGs |
| [Ingestion Patterns](./ingestion-patterns.md) | Best practices for data ingestion |
| [Storage Patterns](./storage-patterns.md) | Organizing data in the medallion architecture |

### Operational Runbooks

Step-by-step procedures for common operations:

| Runbook | Description |
|---------|-------------|
| [Adding Data Sources](./runbooks/adding-data-sources.md) | How to add new data sources |
| [Creating Pipelines](./runbooks/creating-pipelines.md) | Building new data pipelines |
| [Troubleshooting](./runbooks/troubleshooting.md) | Diagnosing and fixing common issues |

### Data Catalog Templates

Templates for documenting data assets in OpenMetadata:

| Template | Description |
|----------|-------------|
| [Dataset Template](./data-catalog/dataset-template.md) | Documenting tables and datasets |
| [Column Documentation](./data-catalog/column-documentation-template.md) | Documenting column definitions |
| [Data Product Template](./data-catalog/data-product-template.md) | Documenting data products |
| [Glossary Term Template](./data-catalog/glossary-term-template.md) | Defining business terms |

## Quick Start

```bash
# Clone and setup
git clone <repository-url> dataplateform
cd dataplateform

# Start the platform
make setup
make start

# Check status
make status
```

**Access the services:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO | http://localhost:9001 | minio / minio123 |
| OpenMetadata | http://localhost:8585 | admin / admin |

## Learning Path

### Beginner Path (2-3 hours)
1. [Getting Started Tutorial](./tutorial/getting-started-tutorial.md) - Sections 1-7
2. Run your first DAG
3. Create a simple dashboard

### Intermediate Path (3-4 hours)
1. Complete the [Getting Started Tutorial](./tutorial/getting-started-tutorial.md)
2. [DAG Development Guide](./dag-development-guide.md)
3. [Ingestion Patterns](./ingestion-patterns.md)

### Advanced Path (4-6 hours)
1. [Architecture](./architecture.md) deep dive
2. [Data Governance Guide](./data-governance-guide.md)
3. [Monitoring Guide](./monitoring-guide.md)
4. [CI/CD](./ci-cd.md)

## Platform Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA PLATFORM                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Data Sources → Bronze (MinIO) → Silver (Staging) → Gold (Marts)│
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Airflow   │  │   Superset  │  │     OpenMetadata       │  │
│  │ Orchestrate │  │ Visualize   │  │     Govern             │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ PostgreSQL  │  │    MinIO    │  │   Great Expectations   │  │
│  │  Database   │  │   Storage   │  │   Data Quality         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Contributing to Documentation

Found an error or want to improve the docs?

1. Fork the repository
2. Make your changes
3. Submit a pull request

### Documentation Style Guide
- Use clear, concise language
- Include examples where possible
- Keep code snippets runnable
- Update the table of contents when adding sections
