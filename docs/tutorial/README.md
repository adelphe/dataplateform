# Data Platform Tutorial

Welcome to the Data Platform Tutorial series. These guides will help you understand and use the platform effectively.

## Getting Started

If you're new to the platform, start here:

1. **[Complete Tutorial](./getting-started-tutorial.md)** - A comprehensive step-by-step guide covering:
   - Platform architecture and concepts
   - Setting up and running the platform
   - Understanding the medallion architecture
   - Running your first DAGs
   - Data ingestion, transformation, and quality
   - Creating dashboards
   - Building your own pipeline from scratch

2. **[Quick Reference](./quick-reference.md)** - One-page cheat sheet with:
   - Service URLs and credentials
   - Common commands
   - Key file locations
   - Troubleshooting tips

3. **[Glossary](./glossary.md)** - Definitions of key terms used throughout the platform:
   - Data engineering concepts (DAG, ETL, medallion architecture)
   - Tool-specific terms (operators, models, expectations)
   - Common acronyms

## Learning Path

### Beginner (1-2 hours)
1. Read Sections 1-6 of the tutorial (Introduction through Medallion Architecture)
2. Complete the hands-on setup
3. Run the Hello World DAG

### Intermediate (2-3 hours)
4. Read Sections 7-10 (Pipelines, Ingestion, dbt, Data Quality)
5. Run the example pipelines
6. Create a simple dashboard in Superset

### Advanced (2-3 hours)
7. Complete Section 13 (Build Your Own Pipeline)
8. Explore OpenMetadata for governance
9. Set up custom data quality checks

## Prerequisites

Before starting, ensure you have:
- Docker Desktop installed (with 8GB+ RAM allocated)
- Basic command line familiarity
- Basic SQL knowledge

## Quick Start

```bash
# Clone and setup
cd /path/to/dataplateform
make setup
make start

# Wait for services to be healthy (~2-3 minutes)
make status

# Access the platform
# Airflow: http://localhost:8080 (admin/admin)
# Superset: http://localhost:8088 (admin/admin)
# MinIO: http://localhost:9001 (minio/minio123)
```

## Additional Resources

### Platform Documentation
- [Architecture Overview](../architecture.md)
- [Developer Guide](../developer-guide.md)
- [DAG Development Guide](../dag-development-guide.md)
- [Data Governance Guide](../data-governance-guide.md)

### Runbooks
- [Adding Data Sources](../runbooks/adding-data-sources.md)
- [Creating Pipelines](../runbooks/creating-pipelines.md)
- [Troubleshooting](../runbooks/troubleshooting.md)

### External Documentation
- [Apache Airflow](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Great Expectations](https://docs.greatexpectations.io/)
- [Apache Superset](https://superset.apache.org/docs/)
- [MinIO](https://min.io/docs/minio/linux/index.html)

## Support

If you encounter issues:
1. Check the [Troubleshooting section](./getting-started-tutorial.md#14-troubleshooting) in the tutorial
2. Review the [Quick Reference troubleshooting table](./quick-reference.md#common-troubleshooting)
3. Check service logs: `docker compose logs <service>`
4. Consult the detailed [Troubleshooting Runbook](../runbooks/troubleshooting.md)

## Feedback

Found an issue or have suggestions for improving this tutorial? Please open an issue or submit a pull request.
