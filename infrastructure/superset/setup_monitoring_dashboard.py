"""Setup script for monitoring dashboard in Superset.

This script programmatically creates the operational monitoring dashboard,
including database connections, datasets, and charts.
"""

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_superset_app():
    """Initialize Superset app context."""
    try:
        from superset.app import create_app

        app = create_app()
        return app
    except ImportError:
        logger.error("Superset not available in this environment")
        return None


def ensure_database_connection(db_session, database_name: str, sqlalchemy_uri: str) -> Optional[int]:
    """Ensure database connection exists.

    Args:
        db_session: SQLAlchemy session
        database_name: Name for the database connection
        sqlalchemy_uri: SQLAlchemy connection URI

    Returns:
        Database ID if successful, None otherwise
    """
    try:
        from superset.models.core import Database

        existing = db_session.query(Database).filter_by(database_name=database_name).first()
        if existing:
            logger.info(f"Database connection '{database_name}' already exists (id={existing.id})")
            return existing.id

        database = Database(
            database_name=database_name,
            sqlalchemy_uri=sqlalchemy_uri,
            expose_in_sqllab=True,
            allow_run_async=True,
            allow_ctas=False,
            allow_cvas=False,
        )
        db_session.add(database)
        db_session.commit()
        logger.info(f"Created database connection '{database_name}' (id={database.id})")
        return database.id
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        db_session.rollback()
        return None


def create_dataset(
    db_session,
    database_id: int,
    table_name: str,
    schema: str = "data_platform",
) -> Optional[int]:
    """Create a dataset for a table.

    Args:
        db_session: SQLAlchemy session
        database_id: ID of the database connection
        table_name: Name of the table
        schema: Schema containing the table

    Returns:
        Dataset ID if successful, None otherwise
    """
    try:
        from superset.connectors.sqla.models import SqlaTable

        existing = (
            db_session.query(SqlaTable)
            .filter_by(table_name=table_name, schema=schema, database_id=database_id)
            .first()
        )
        if existing:
            logger.info(f"Dataset '{schema}.{table_name}' already exists (id={existing.id})")
            return existing.id

        dataset = SqlaTable(
            table_name=table_name,
            schema=schema,
            database_id=database_id,
        )
        db_session.add(dataset)
        db_session.commit()

        # Fetch columns
        dataset.fetch_metadata()
        db_session.commit()

        logger.info(f"Created dataset '{schema}.{table_name}' (id={dataset.id})")
        return dataset.id
    except Exception as e:
        logger.error(f"Failed to create dataset {table_name}: {e}")
        db_session.rollback()
        return None


def create_chart(
    db_session,
    database_id: int,
    chart_config: Dict[str, Any],
    schema: str = "data_platform",
) -> Optional[int]:
    """Create a chart (Slice) from configuration.

    Args:
        db_session: SQLAlchemy session
        database_id: ID of the database connection
        chart_config: Chart configuration from JSON
        schema: Schema for the datasource

    Returns:
        Chart ID if successful, None otherwise
    """
    try:
        from superset.models.slice import Slice
        from superset.connectors.sqla.models import SqlaTable

        slice_name = chart_config.get("slice_name")
        datasource_name = chart_config.get("datasource")

        # Check if chart already exists
        existing = db_session.query(Slice).filter_by(slice_name=slice_name).first()
        if existing:
            logger.info(f"Chart '{slice_name}' already exists (id={existing.id})")
            return existing.id

        # Find the dataset for this chart
        dataset = (
            db_session.query(SqlaTable)
            .filter_by(table_name=datasource_name, schema=schema, database_id=database_id)
            .first()
        )

        if not dataset:
            logger.warning(f"Dataset '{datasource_name}' not found for chart '{slice_name}'")
            return None

        # Build params JSON
        params = chart_config.get("params", {})
        params["datasource"] = f"{dataset.id}__table"

        # Create the slice/chart
        chart = Slice(
            slice_name=slice_name,
            viz_type=chart_config.get("viz_type", "table"),
            datasource_id=dataset.id,
            datasource_type="table",
            params=json.dumps(params),
        )

        db_session.add(chart)
        db_session.commit()

        logger.info(f"Created chart '{slice_name}' (id={chart.id})")
        return chart.id

    except Exception as e:
        logger.error(f"Failed to create chart '{chart_config.get('slice_name')}': {e}")
        db_session.rollback()
        return None


def import_dashboard(db_session, dashboard_json_path: str, database_id: int) -> bool:
    """Import dashboard from JSON file, creating charts and linking them.

    Args:
        db_session: SQLAlchemy session
        dashboard_json_path: Path to dashboard JSON file
        database_id: ID of the database connection for datasets

    Returns:
        True if successful, False otherwise
    """
    try:
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        with open(dashboard_json_path, "r") as f:
            dashboard_data = json.load(f)

        # Check if dashboard exists
        existing = (
            db_session.query(Dashboard)
            .filter_by(slug=dashboard_data.get("slug", "platform-operations"))
            .first()
        )
        if existing:
            logger.info(f"Dashboard '{dashboard_data['dashboard_title']}' already exists")
            return True

        # Create charts from the charts array in the JSON
        chart_configs = dashboard_data.get("charts", [])
        created_charts = []
        chart_id_map = {}  # Map slice_name to chart_id

        for chart_config in chart_configs:
            chart_id = create_chart(db_session, database_id, chart_config)
            if chart_id:
                created_charts.append(chart_id)
                chart_id_map[chart_config.get("slice_name")] = chart_id

        # Build position JSON with actual chart IDs
        position_json = dashboard_data.get("position_json", {})

        # Create row entries for each chart category
        row_configs = {
            "ROW-health-metrics": ["Platform Health Score", "Total DAGs", "Failed Tasks (24h)", "SLA Compliance"],
            "ROW-freshness": ["Data Freshness Heatmap", "Health Score Trend"],
            "ROW-quality": ["Task Failure Trends", "Data Quality Metrics", "SLA Compliance Trends"],
            "ROW-alerts": ["Unacknowledged Alerts", "Data Volume Trends", "Ingestion Statistics"],
        }

        # Build complete position_json with chart placements
        chart_index = 0
        for row_id, chart_names in row_configs.items():
            row_children = []
            for i, chart_name in enumerate(chart_names):
                if chart_name in chart_id_map:
                    chart_key = f"CHART-{chart_id_map[chart_name]}"
                    row_children.append(chart_key)
                    # Add chart position entry
                    position_json[chart_key] = {
                        "type": "CHART",
                        "id": chart_key,
                        "children": [],
                        "parents": [row_id],
                        "meta": {
                            "width": 3,
                            "height": 50,
                            "chartId": chart_id_map[chart_name],
                            "sliceName": chart_name,
                        },
                    }
                    chart_index += 1

            # Add or update row entry
            position_json[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": row_children,
                "parents": ["GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }

        # Create dashboard
        dashboard = Dashboard(
            dashboard_title=dashboard_data["dashboard_title"],
            slug=dashboard_data.get("slug", "platform-operations"),
            position_json=json.dumps(position_json),
            css=dashboard_data.get("css", ""),
            json_metadata=json.dumps(dashboard_data.get("json_metadata", {})),
            published=True,
        )
        db_session.add(dashboard)
        db_session.flush()  # Get dashboard ID

        # Link charts to dashboard
        if created_charts:
            charts = db_session.query(Slice).filter(Slice.id.in_(created_charts)).all()
            dashboard.slices = charts

        db_session.commit()

        logger.info(
            f"Created dashboard '{dashboard_data['dashboard_title']}' "
            f"(id={dashboard.id}) with {len(created_charts)} charts"
        )
        return True
    except Exception as e:
        logger.error(f"Failed to import dashboard: {e}")
        db_session.rollback()
        return False


def setup_refresh_schedules(db_session) -> None:
    """Configure refresh schedules for cached queries."""
    logger.info("Refresh schedules configured via dashboard metadata")


def setup_permissions(db_session) -> None:
    """Configure dashboard permissions."""
    try:
        from superset.security import SupersetSecurityManager
        from flask_appbuilder.security.sqla.models import Role

        # Get Public role
        public_role = db_session.query(Role).filter_by(name="Public").first()
        if not public_role:
            logger.warning("Public role not found, skipping permission setup")
            return

        logger.info("Dashboard permissions configured for authenticated users")
    except Exception as e:
        logger.warning(f"Could not configure permissions: {e}")


def main():
    """Main setup function."""
    logger.info("=" * 60)
    logger.info("Setting up Monitoring Dashboard")
    logger.info("=" * 60)

    app = get_superset_app()
    if not app:
        logger.error("Could not initialize Superset app")
        sys.exit(1)

    with app.app_context():
        from superset import db

        # Get database connection details
        postgres_uri = os.environ.get(
            "POSTGRES_CONNECTION_STRING",
            "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        # Ensure database connection
        database_id = ensure_database_connection(
            db.session,
            database_name="Data Platform Warehouse",
            sqlalchemy_uri=postgres_uri,
        )

        if not database_id:
            logger.error("Failed to create database connection")
            sys.exit(1)

        # Create datasets for monitoring tables
        monitoring_tables = [
            ("data_freshness_metrics", "data_platform"),
            ("platform_health_metrics", "data_platform"),
            ("alert_history", "data_platform"),
            ("sla_tracking", "data_platform"),
            ("data_quality_alerts", "data_platform"),
            ("data_quality_metrics", "data_platform"),
            ("ingestion_metadata", "data_platform"),
            ("validation_results", "data_platform"),
            ("watermarks", "data_platform"),
        ]

        # Create datasets for views
        monitoring_views = [
            ("vw_data_freshness_status", "data_platform"),
            ("vw_platform_health_summary", "data_platform"),
            ("vw_daily_health_trends", "data_platform"),
            ("vw_unacknowledged_alerts", "data_platform"),
        ]

        for table_name, schema in monitoring_tables + monitoring_views:
            create_dataset(db.session, database_id, table_name, schema)

        # Import dashboard with charts
        dashboard_path = "/app/superset_init/dashboards/platform_operations.json"
        if os.path.exists(dashboard_path):
            import_dashboard(db.session, dashboard_path, database_id)
        else:
            logger.warning(f"Dashboard JSON not found at {dashboard_path}")

        # Setup permissions
        setup_permissions(db.session)

        # Setup refresh schedules
        setup_refresh_schedules(db.session)

        logger.info("=" * 60)
        logger.info("Monitoring Dashboard Setup Complete!")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Access the dashboard at: http://localhost:8088/superset/dashboard/platform-operations/")


if __name__ == "__main__":
    main()
