#!/usr/bin/env python3
"""
Setup PostgreSQL database connection in Superset.
This script creates the postgres_warehouse connection programmatically.
"""

import os
import sys

# Add Superset to path
sys.path.insert(0, "/app/superset")

from superset import app, db
from superset.models.core import Database


def setup_database_connection():
    """Create or update the PostgreSQL warehouse database connection."""

    database_name = "postgres_warehouse"
    sqlalchemy_uri = "postgresql://airflow:airflow@postgres:5432/airflow"

    with app.app_context():
        # Check if database already exists
        existing_db = db.session.query(Database).filter_by(
            database_name=database_name
        ).first()

        if existing_db:
            print(f"[INFO] Database '{database_name}' already exists, updating...")
            existing_db.sqlalchemy_uri = sqlalchemy_uri
            existing_db.expose_in_sqllab = True
            existing_db.allow_ctas = True
            existing_db.allow_cvas = True
            existing_db.allow_dml = False
            existing_db.allow_run_async = True
        else:
            print(f"[INFO] Creating database connection '{database_name}'...")
            new_db = Database(
                database_name=database_name,
                sqlalchemy_uri=sqlalchemy_uri,
                expose_in_sqllab=True,
                allow_ctas=True,
                allow_cvas=True,
                allow_dml=False,
                allow_run_async=True,
            )
            db.session.add(new_db)

        db.session.commit()
        print(f"[INFO] Database connection '{database_name}' configured successfully")


if __name__ == "__main__":
    setup_database_connection()
