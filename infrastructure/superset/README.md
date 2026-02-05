# Superset Infrastructure

This directory contains initialization scripts and configuration for Apache Superset, the BI/visualization layer of the data platform.

## Directory Structure

```
infrastructure/superset/
├── README.md                 # This file
├── init-superset.sh          # Main initialization orchestrator
├── setup_database.py         # PostgreSQL warehouse connection setup
├── setup_roles.py            # RBAC roles and sample users
├── import_dashboards.py      # Dashboard, chart, and dataset creation
└── dashboards/               # Dashboard JSON export files
    ├── customer_insights.json
    ├── product_performance.json
    └── daily_operations.json
```

## File Descriptions

### init-superset.sh

Main bash script that orchestrates the initialization process:
- Waits for Superset to be ready
- Runs database connection setup
- Configures roles and permissions
- Imports pre-built dashboards

### setup_database.py

Creates the `postgres_warehouse` database connection pointing to the PostgreSQL data warehouse containing dbt mart models.

### setup_roles.py

Configures RBAC (Role-Based Access Control):
- **Analyst** role: Create/edit dashboards, charts, datasets; SQL Lab access
- **Viewer** role: Read-only access to dashboards and charts
- Creates sample users for each role

### import_dashboards.py

Programmatically creates:
- Datasets for each dbt mart model
- Pre-configured charts for common visualizations
- Three dashboards: Customer Insights, Product Performance, Daily Operations

### dashboards/

JSON export files containing dashboard configurations. These serve as:
- Documentation of dashboard structure
- Backup/restore capability
- Templates for creating similar dashboards

## Customization

### Adding a New Database Connection

Edit `setup_database.py`:

```python
# Add new database configuration
new_db = Database(
    database_name="my_new_database",
    sqlalchemy_uri="postgresql://user:pass@host:5432/db",
    expose_in_sqllab=True,
)
db.session.add(new_db)
```

### Creating Custom Roles

Edit `setup_roles.py`:

```python
ROLE_DEFINITIONS = {
    "MyCustomRole": {
        "description": "Custom role description",
        "permissions": [
            ("can_read", "Dashboard"),
            ("can_write", "Chart"),
            # Add more permissions as needed
        ],
    },
}
```

### Adding New Dashboards

1. Create a JSON file in `dashboards/`:
   ```json
   {
     "dashboard_title": "My Dashboard",
     "slug": "my-dashboard",
     "charts": [...],
     "dataset": {...}
   }
   ```

2. Update `import_dashboards.py` to include the new dashboard configuration.

### Modifying Chart Configurations

Edit the `CHARTS` list in `import_dashboards.py`:

```python
{
    "slice_name": "My New Chart",
    "viz_type": "echarts_bar",
    "datasource_name": "mart_customer_analytics",
    "params": {
        "metrics": ["my_metric"],
        "groupby": ["dimension"],
        # Chart-specific configuration
    },
    "dashboard": "Customer Insights",
}
```

## Running Manually

Re-run initialization scripts without restarting Superset:

```bash
# From project root
make superset-init

# Or individually
docker compose exec superset python /app/superset_init/setup_database.py
docker compose exec superset python /app/superset_init/setup_roles.py
docker compose exec superset python /app/superset_init/import_dashboards.py
```

## Troubleshooting

### Scripts fail during startup

Check Superset logs:
```bash
make superset-logs
```

Common issues:
- PostgreSQL not ready: Increase wait time in health checks
- Permission errors: Ensure files are executable
- Import errors: Check Python syntax and dependencies

### Database connection fails

Verify PostgreSQL is accessible:
```bash
docker compose exec superset python -c "
from sqlalchemy import create_engine
engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
print(engine.connect())
"
```

### Dashboards not appearing

1. Check if datasets were created:
   ```bash
   docker compose exec superset superset fab list-datasets
   ```

2. Verify database connection exists:
   ```bash
   docker compose exec superset superset fab list-databases
   ```

3. Re-run import:
   ```bash
   docker compose exec superset python /app/superset_init/import_dashboards.py
   ```

## Related Documentation

- [Superset User Guide](../../docs/superset-guide.md) - Comprehensive usage documentation
- [dbt Transformations](../../transformations/README.md) - Source data models
- [Airflow Setup](../../docs/airflow-setup.md) - Orchestration layer
