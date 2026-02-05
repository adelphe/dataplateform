#!/usr/bin/env python3
"""
Import pre-configured dashboards, datasets, and charts into Superset.
Creates visualizations for the dbt mart models:
- mart_customer_analytics
- mart_product_analytics
- mart_daily_summary
"""

import json
import sys
from typing import Optional

# Add Superset to path
sys.path.insert(0, "/app/superset")

from superset import app, db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable, TableColumn, SqlMetric
from superset.models.slice import Slice
from superset.models.dashboard import Dashboard


# Dataset definitions for dbt mart models
DATASETS = [
    {
        "table_name": "mart_customer_analytics",
        "schema": "marts",
        "description": "Customer analytics mart with lifetime value, purchase patterns, and segmentation",
        "columns": [
            {"column_name": "customer_id", "type": "INTEGER", "is_dttm": False},
            {"column_name": "customer_name", "type": "VARCHAR", "is_dttm": False},
            {"column_name": "email", "type": "VARCHAR", "is_dttm": False},
            {"column_name": "total_orders", "type": "INTEGER", "is_dttm": False},
            {"column_name": "total_spend", "type": "NUMERIC", "is_dttm": False},
            {"column_name": "avg_order_value", "type": "NUMERIC", "is_dttm": False},
            {"column_name": "first_order_date", "type": "DATE", "is_dttm": True},
            {"column_name": "last_order_date", "type": "DATE", "is_dttm": True},
            {"column_name": "customer_lifetime_days", "type": "INTEGER", "is_dttm": False},
        ],
        "metrics": [
            {"metric_name": "total_customers", "expression": "COUNT(DISTINCT customer_id)"},
            {"metric_name": "sum_total_spend", "expression": "SUM(total_spend)"},
            {"metric_name": "avg_customer_spend", "expression": "AVG(total_spend)"},
            {"metric_name": "avg_order_value", "expression": "AVG(avg_order_value)"},
        ],
    },
    {
        "table_name": "mart_product_analytics",
        "schema": "marts",
        "description": "Product analytics mart with revenue, units sold, and performance metrics",
        "columns": [
            {"column_name": "product_id", "type": "INTEGER", "is_dttm": False},
            {"column_name": "product_name", "type": "VARCHAR", "is_dttm": False},
            {"column_name": "category", "type": "VARCHAR", "is_dttm": False},
            {"column_name": "total_units_sold", "type": "INTEGER", "is_dttm": False},
            {"column_name": "total_revenue", "type": "NUMERIC", "is_dttm": False},
            {"column_name": "unique_customers", "type": "INTEGER", "is_dttm": False},
            {"column_name": "avg_unit_price", "type": "NUMERIC", "is_dttm": False},
        ],
        "metrics": [
            {"metric_name": "total_products", "expression": "COUNT(DISTINCT product_id)"},
            {"metric_name": "sum_revenue", "expression": "SUM(total_revenue)"},
            {"metric_name": "sum_units_sold", "expression": "SUM(total_units_sold)"},
            {"metric_name": "avg_revenue_per_product", "expression": "AVG(total_revenue)"},
        ],
    },
    {
        "table_name": "mart_daily_summary",
        "schema": "marts",
        "description": "Daily summary mart with revenue, transactions, and operational metrics",
        "columns": [
            {"column_name": "date", "type": "DATE", "is_dttm": True},
            {"column_name": "total_revenue", "type": "NUMERIC", "is_dttm": False},
            {"column_name": "total_transactions", "type": "INTEGER", "is_dttm": False},
            {"column_name": "unique_customers", "type": "INTEGER", "is_dttm": False},
            {"column_name": "avg_transaction_value", "type": "NUMERIC", "is_dttm": False},
            {"column_name": "total_units_sold", "type": "INTEGER", "is_dttm": False},
        ],
        "metrics": [
            {"metric_name": "sum_daily_revenue", "expression": "SUM(total_revenue)"},
            {"metric_name": "sum_transactions", "expression": "SUM(total_transactions)"},
            {"metric_name": "avg_daily_revenue", "expression": "AVG(total_revenue)"},
            {"metric_name": "avg_daily_transactions", "expression": "AVG(total_transactions)"},
        ],
    },
]

# Chart definitions
CHARTS = [
    # Customer Analytics Charts
    {
        "slice_name": "Top Customers by Total Spend",
        "viz_type": "echarts_bar",
        "datasource_name": "mart_customer_analytics",
        "params": {
            "viz_type": "echarts_bar",
            "metrics": ["sum_total_spend"],
            "groupby": ["customer_name"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_label": "Customer",
            "y_axis_label": "Total Spend ($)",
        },
        "dashboard": "Customer Insights",
    },
    {
        "slice_name": "Customer Lifetime Distribution",
        "viz_type": "dist_bar",
        "datasource_name": "mart_customer_analytics",
        "params": {
            "viz_type": "dist_bar",
            "metrics": ["total_customers"],
            "groupby": ["customer_lifetime_days"],
            "row_limit": 50,
            "color_scheme": "supersetColors",
        },
        "dashboard": "Customer Insights",
    },
    {
        "slice_name": "Total Customers KPI",
        "viz_type": "big_number_total",
        "datasource_name": "mart_customer_analytics",
        "params": {
            "viz_type": "big_number_total",
            "metric": "total_customers",
            "subheader": "Total Unique Customers",
            "y_axis_format": ",d",
        },
        "dashboard": "Customer Insights",
    },
    {
        "slice_name": "Average Transaction Value KPI",
        "viz_type": "big_number_total",
        "datasource_name": "mart_customer_analytics",
        "params": {
            "viz_type": "big_number_total",
            "metric": "avg_order_value",
            "subheader": "Avg Order Value",
            "y_axis_format": "$,.2f",
        },
        "dashboard": "Customer Insights",
    },
    # Product Analytics Charts
    {
        "slice_name": "Top Products by Revenue",
        "viz_type": "echarts_bar",
        "datasource_name": "mart_product_analytics",
        "params": {
            "viz_type": "echarts_bar",
            "metrics": ["sum_revenue"],
            "groupby": ["product_name"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "supersetColors",
            "orientation": "horizontal",
        },
        "dashboard": "Product Performance",
    },
    {
        "slice_name": "Revenue by Category",
        "viz_type": "pie",
        "datasource_name": "mart_product_analytics",
        "params": {
            "viz_type": "pie",
            "metric": "sum_revenue",
            "groupby": ["category"],
            "color_scheme": "supersetColors",
            "show_legend": True,
            "show_labels": True,
            "label_type": "key_percent",
        },
        "dashboard": "Product Performance",
    },
    {
        "slice_name": "Product Performance Table",
        "viz_type": "table",
        "datasource_name": "mart_product_analytics",
        "params": {
            "viz_type": "table",
            "metrics": ["sum_revenue", "sum_units_sold", "total_products"],
            "groupby": ["product_name", "category"],
            "row_limit": 100,
            "order_desc": True,
            "include_search": True,
            "page_length": 25,
        },
        "dashboard": "Product Performance",
    },
    {
        "slice_name": "Unique Customers per Product",
        "viz_type": "echarts_bar",
        "datasource_name": "mart_product_analytics",
        "params": {
            "viz_type": "echarts_bar",
            "metrics": [{"label": "unique_customers", "expressionType": "SIMPLE", "column": {"column_name": "unique_customers"}, "aggregate": "SUM"}],
            "groupby": ["product_name"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "bnbColors",
        },
        "dashboard": "Product Performance",
    },
    # Daily Operations Charts
    {
        "slice_name": "Daily Revenue Trend",
        "viz_type": "echarts_timeseries_line",
        "datasource_name": "mart_daily_summary",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "metrics": ["sum_daily_revenue"],
            "x_axis": "date",
            "time_grain_sqla": "P1D",
            "color_scheme": "supersetColors",
            "show_legend": True,
            "rich_tooltip": True,
            "tooltipTimeFormat": "%Y-%m-%d",
        },
        "dashboard": "Daily Operations",
    },
    {
        "slice_name": "Transaction Volume",
        "viz_type": "echarts_area",
        "datasource_name": "mart_daily_summary",
        "params": {
            "viz_type": "echarts_area",
            "metrics": ["sum_transactions"],
            "x_axis": "date",
            "time_grain_sqla": "P1D",
            "color_scheme": "bnbColors",
            "opacity": 0.7,
        },
        "dashboard": "Daily Operations",
    },
    {
        "slice_name": "Total Revenue KPI",
        "viz_type": "big_number_total",
        "datasource_name": "mart_daily_summary",
        "params": {
            "viz_type": "big_number_total",
            "metric": "sum_daily_revenue",
            "subheader": "Total Revenue",
            "y_axis_format": "$,.2f",
        },
        "dashboard": "Daily Operations",
    },
    {
        "slice_name": "Total Transactions KPI",
        "viz_type": "big_number_total",
        "datasource_name": "mart_daily_summary",
        "params": {
            "viz_type": "big_number_total",
            "metric": "sum_transactions",
            "subheader": "Total Transactions",
            "y_axis_format": ",d",
        },
        "dashboard": "Daily Operations",
    },
]

# Dashboard definitions
DASHBOARDS = [
    {
        "dashboard_title": "Customer Insights",
        "slug": "customer-insights",
        "description": "Analytics dashboard for customer behavior, lifetime value, and segmentation",
    },
    {
        "dashboard_title": "Product Performance",
        "slug": "product-performance",
        "description": "Analytics dashboard for product revenue, sales, and performance metrics",
    },
    {
        "dashboard_title": "Daily Operations",
        "slug": "daily-operations",
        "description": "Operational dashboard for daily revenue, transactions, and key metrics",
    },
]


def get_database() -> Optional[Database]:
    """Get the postgres_warehouse database connection."""
    return db.session.query(Database).filter_by(
        database_name="postgres_warehouse"
    ).first()


def create_datasets(database: Database):
    """Create or update datasets for dbt mart models."""
    print("[INFO] Creating datasets for dbt marts...")

    for dataset_config in DATASETS:
        table_name = dataset_config["table_name"]
        schema = dataset_config["schema"]

        # Check if dataset exists
        existing_table = db.session.query(SqlaTable).filter_by(
            table_name=table_name,
            schema=schema,
            database_id=database.id,
        ).first()

        if existing_table:
            print(f"[INFO] Dataset '{schema}.{table_name}' already exists, updating...")
            table = existing_table
        else:
            print(f"[INFO] Creating dataset '{schema}.{table_name}'...")
            table = SqlaTable(
                table_name=table_name,
                schema=schema,
                database_id=database.id,
            )
            db.session.add(table)

        table.description = dataset_config.get("description", "")

        # Add columns
        for col_config in dataset_config.get("columns", []):
            existing_col = None
            for col in table.columns:
                if col.column_name == col_config["column_name"]:
                    existing_col = col
                    break

            if existing_col:
                existing_col.type = col_config.get("type", "VARCHAR")
                existing_col.is_dttm = col_config.get("is_dttm", False)
            else:
                new_col = TableColumn(
                    column_name=col_config["column_name"],
                    type=col_config.get("type", "VARCHAR"),
                    is_dttm=col_config.get("is_dttm", False),
                )
                table.columns.append(new_col)

        # Add metrics
        for metric_config in dataset_config.get("metrics", []):
            existing_metric = None
            for metric in table.metrics:
                if metric.metric_name == metric_config["metric_name"]:
                    existing_metric = metric
                    break

            if existing_metric:
                existing_metric.expression = metric_config["expression"]
            else:
                new_metric = SqlMetric(
                    metric_name=metric_config["metric_name"],
                    expression=metric_config["expression"],
                    metric_type="count" if "COUNT" in metric_config["expression"].upper() else "sum",
                )
                table.metrics.append(new_metric)

        db.session.commit()
        print(f"[INFO] Dataset '{schema}.{table_name}' configured successfully")

    return db.session.query(SqlaTable).filter(
        SqlaTable.database_id == database.id
    ).all()


def create_charts(datasets: list):
    """Create charts for the dashboards."""
    print("[INFO] Creating charts...")

    # Build dataset lookup
    dataset_lookup = {ds.table_name: ds for ds in datasets}
    created_charts = {}

    for chart_config in CHARTS:
        slice_name = chart_config["slice_name"]
        datasource_name = chart_config["datasource_name"]

        # Get datasource
        datasource = dataset_lookup.get(datasource_name)
        if not datasource:
            print(f"[WARN] Datasource '{datasource_name}' not found, skipping chart '{slice_name}'")
            continue

        # Check if chart exists
        existing_chart = db.session.query(Slice).filter_by(
            slice_name=slice_name,
        ).first()

        if existing_chart:
            print(f"[INFO] Chart '{slice_name}' already exists, updating...")
            chart = existing_chart
        else:
            print(f"[INFO] Creating chart '{slice_name}'...")
            chart = Slice(slice_name=slice_name)
            db.session.add(chart)

        chart.viz_type = chart_config["viz_type"]
        chart.datasource_id = datasource.id
        chart.datasource_type = "table"
        chart.params = json.dumps(chart_config["params"])

        db.session.commit()

        # Store for dashboard assignment
        dashboard_name = chart_config.get("dashboard")
        if dashboard_name:
            if dashboard_name not in created_charts:
                created_charts[dashboard_name] = []
            created_charts[dashboard_name].append(chart)

        print(f"[INFO] Chart '{slice_name}' configured successfully")

    return created_charts


def create_dashboards(charts_by_dashboard: dict):
    """Create dashboards and assign charts."""
    print("[INFO] Creating dashboards...")

    for dashboard_config in DASHBOARDS:
        title = dashboard_config["dashboard_title"]
        slug = dashboard_config["slug"]

        # Check if dashboard exists
        existing_dashboard = db.session.query(Dashboard).filter_by(
            slug=slug,
        ).first()

        if existing_dashboard:
            print(f"[INFO] Dashboard '{title}' already exists, updating...")
            dashboard = existing_dashboard
        else:
            print(f"[INFO] Creating dashboard '{title}'...")
            dashboard = Dashboard(
                dashboard_title=title,
                slug=slug,
            )
            db.session.add(dashboard)

        dashboard.description = dashboard_config.get("description", "")

        # Assign charts
        dashboard_charts = charts_by_dashboard.get(title, [])
        dashboard.slices = dashboard_charts

        # Create a simple position layout
        position_json = {
            "DASHBOARD_VERSION_KEY": "v2",
        }
        for i, chart in enumerate(dashboard_charts):
            row = i // 2
            col = i % 2
            position_json[f"CHART-{chart.id}"] = {
                "type": "CHART",
                "id": f"CHART-{chart.id}",
                "children": [],
                "meta": {
                    "width": 6,
                    "height": 50,
                    "chartId": chart.id,
                    "sliceName": chart.slice_name,
                },
            }

        dashboard.position_json = json.dumps(position_json)
        dashboard.published = True

        db.session.commit()
        print(f"[INFO] Dashboard '{title}' configured with {len(dashboard_charts)} charts")


def main():
    """Main function to import dashboards."""
    print("[INFO] Starting dashboard import...")

    with app.app_context():
        # Get database connection
        database = get_database()
        if not database:
            print("[ERROR] Database 'postgres_warehouse' not found. Run setup_database.py first.")
            sys.exit(1)

        # Create datasets
        datasets = create_datasets(database)

        # Create charts
        charts_by_dashboard = create_charts(datasets)

        # Create dashboards
        create_dashboards(charts_by_dashboard)

        print("[INFO] Dashboard import complete!")
        print("")
        print("Created dashboards:")
        for dashboard_config in DASHBOARDS:
            print(f"  - {dashboard_config['dashboard_title']}")


if __name__ == "__main__":
    main()
