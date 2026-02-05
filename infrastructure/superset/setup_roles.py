#!/usr/bin/env python3
"""
Setup RBAC roles and sample users in Superset.
Creates Admin, Analyst, and Viewer roles with appropriate permissions.
"""

import os
import sys

# Add Superset to path
sys.path.insert(0, "/app/superset")

from superset import app, security_manager
from superset.extensions import db


# Database/datasource access configuration
# These are the names used in Superset for the warehouse database and schemas
WAREHOUSE_DATABASE_NAME = os.environ.get("POSTGRES_DB", "airflow")
WAREHOUSE_SCHEMAS = ["marts", "staging", "intermediate", "raw", "reference"]


# Role definitions with their permissions
ROLE_DEFINITIONS = {
    "Analyst": {
        "description": "Can view and create dashboards, charts, and datasets",
        "permissions": [
            # Dashboard permissions
            ("can_read", "Dashboard"),
            ("can_write", "Dashboard"),
            ("can_export", "Dashboard"),
            # Chart permissions
            ("can_read", "Chart"),
            ("can_write", "Chart"),
            ("can_export", "Chart"),
            # Dataset permissions
            ("can_read", "Dataset"),
            ("can_write", "Dataset"),
            # SQL Lab permissions
            ("can_read", "Query"),
            ("can_write", "Query"),
            ("can_csv", "Superset"),
            ("can_sqllab", "Superset"),
            # Explore permissions
            ("can_explore", "Superset"),
            ("can_explore_json", "Superset"),
            # Database permissions
            ("can_read", "Database"),
            # Menu access
            ("menu_access", "Dashboards"),
            ("menu_access", "Charts"),
            ("menu_access", "Datasets"),
            ("menu_access", "SQL Lab"),
            ("menu_access", "SQL Editor"),
            ("menu_access", "Saved Queries"),
            ("menu_access", "Query Search"),
        ],
    },
    "Viewer": {
        "description": "Read-only access to dashboards and charts",
        "permissions": [
            # Dashboard permissions (read-only)
            ("can_read", "Dashboard"),
            ("can_export", "Dashboard"),
            # Chart permissions (read-only)
            ("can_read", "Chart"),
            ("can_export", "Chart"),
            # Dataset permissions (read-only)
            ("can_read", "Dataset"),
            # Basic explore for filtering
            ("can_explore", "Superset"),
            ("can_explore_json", "Superset"),
            # Menu access
            ("menu_access", "Dashboards"),
            ("menu_access", "Charts"),
        ],
    },
}

# Sample users to create
SAMPLE_USERS = [
    {
        "username": "analyst",
        "first_name": "Sample",
        "last_name": "Analyst",
        "email": "analyst@example.com",
        "password": "analyst123",
        "role": "Analyst",
    },
    {
        "username": "viewer",
        "first_name": "Sample",
        "last_name": "Viewer",
        "email": "viewer@example.com",
        "password": "viewer123",
        "role": "Viewer",
    },
]


def setup_roles():
    """Create or update custom roles with defined permissions."""

    with app.app_context():
        for role_name, role_config in ROLE_DEFINITIONS.items():
            print(f"[INFO] Setting up role: {role_name}")

            # Get or create the role
            role = security_manager.find_role(role_name)
            if not role:
                role = security_manager.add_role(role_name)
                print(f"[INFO] Created role: {role_name}")
            else:
                print(f"[INFO] Role '{role_name}' already exists, updating permissions...")

            # Clear existing permissions
            role.permissions = []

            # Add permissions
            for permission_name, view_name in role_config["permissions"]:
                try:
                    permission_view = security_manager.find_permission_view_menu(
                        permission_name, view_name
                    )
                    if permission_view:
                        security_manager.add_permission_role(role, permission_view)
                except Exception as e:
                    print(f"[WARN] Could not add permission {permission_name} on {view_name}: {e}")

            db.session.commit()
            print(f"[INFO] Role '{role_name}' configured with {len(role.permissions)} permissions")


def grant_data_access_permissions():
    """Grant database and datasource access permissions to Analyst and Viewer roles.

    This function adds the necessary data access permissions that allow users to
    actually query the warehouse database and view data in dashboards.
    """
    with app.app_context():
        # Roles that need data access
        data_access_roles = ["Analyst", "Viewer"]

        for role_name in data_access_roles:
            role = security_manager.find_role(role_name)
            if not role:
                print(f"[WARN] Role '{role_name}' not found, skipping data access permissions")
                continue

            print(f"[INFO] Granting data access permissions to role: {role_name}")
            permissions_added = 0

            # Grant database access permission for the warehouse database
            # Permission format: "database_access on [PostgresWarehouse].(id:1)"
            # We try multiple possible permission names since the exact format depends on how the DB was registered
            database_access_patterns = [
                f"[{WAREHOUSE_DATABASE_NAME}].(id:1)",
                f"[PostgresWarehouse].(id:1)",
                WAREHOUSE_DATABASE_NAME,
                "PostgresWarehouse",
            ]

            for db_pattern in database_access_patterns:
                try:
                    perm_view = security_manager.find_permission_view_menu(
                        "database_access", db_pattern
                    )
                    if perm_view:
                        security_manager.add_permission_role(role, perm_view)
                        print(f"[INFO]   Added database_access on {db_pattern}")
                        permissions_added += 1
                        break
                except Exception as e:
                    pass  # Try next pattern

            # Grant schema access for each warehouse schema
            for schema in WAREHOUSE_SCHEMAS:
                schema_patterns = [
                    f"[{WAREHOUSE_DATABASE_NAME}].[{schema}]",
                    f"[PostgresWarehouse].[{schema}]",
                    f"{schema}",
                ]

                for schema_pattern in schema_patterns:
                    try:
                        perm_view = security_manager.find_permission_view_menu(
                            "schema_access", schema_pattern
                        )
                        if perm_view:
                            security_manager.add_permission_role(role, perm_view)
                            print(f"[INFO]   Added schema_access on {schema_pattern}")
                            permissions_added += 1
                            break
                    except Exception as e:
                        pass  # Try next pattern

            # Grant datasource access for all datasets in the warehouse schemas
            # This is needed for viewing charts/dashboards that use specific datasets
            try:
                from superset.connectors.sqla.models import SqlaTable

                # Get all datasets from the warehouse database
                datasets = db.session.query(SqlaTable).all()
                for dataset in datasets:
                    # Build the datasource permission name
                    datasource_perm = dataset.perm if hasattr(dataset, 'perm') else None
                    if datasource_perm:
                        try:
                            perm_view = security_manager.find_permission_view_menu(
                                "datasource_access", datasource_perm
                            )
                            if perm_view:
                                security_manager.add_permission_role(role, perm_view)
                                print(f"[INFO]   Added datasource_access on {datasource_perm}")
                                permissions_added += 1
                        except Exception as e:
                            pass  # Dataset permission not available yet
            except Exception as e:
                print(f"[WARN] Could not enumerate datasets for datasource_access: {e}")

            # Grant all_datasource_access for simpler setup (covers all datasets)
            try:
                perm_view = security_manager.find_permission_view_menu(
                    "all_datasource_access", "all_datasource_access"
                )
                if perm_view:
                    security_manager.add_permission_role(role, perm_view)
                    print(f"[INFO]   Added all_datasource_access")
                    permissions_added += 1
            except Exception as e:
                pass  # Permission might not exist

            # Grant all_database_access for simpler setup (covers all databases)
            try:
                perm_view = security_manager.find_permission_view_menu(
                    "all_database_access", "all_database_access"
                )
                if perm_view:
                    security_manager.add_permission_role(role, perm_view)
                    print(f"[INFO]   Added all_database_access")
                    permissions_added += 1
            except Exception as e:
                pass  # Permission might not exist

            db.session.commit()
            print(f"[INFO] Added {permissions_added} data access permissions to '{role_name}'")


def setup_sample_users():
    """Create sample users for each role (optional)."""

    with app.app_context():
        for user_config in SAMPLE_USERS:
            username = user_config["username"]

            # Check if user exists
            existing_user = security_manager.find_user(username=username)
            if existing_user:
                print(f"[INFO] User '{username}' already exists, skipping...")
                continue

            # Get the role
            role = security_manager.find_role(user_config["role"])
            if not role:
                print(f"[WARN] Role '{user_config['role']}' not found, skipping user '{username}'")
                continue

            # Create user
            try:
                security_manager.add_user(
                    username=username,
                    first_name=user_config["first_name"],
                    last_name=user_config["last_name"],
                    email=user_config["email"],
                    role=role,
                    password=user_config["password"],
                )
                print(f"[INFO] Created user: {username} with role {user_config['role']}")
            except Exception as e:
                print(f"[WARN] Could not create user {username}: {e}")

        db.session.commit()


def main():
    """Main function to setup roles and users."""
    print("[INFO] Setting up Superset roles and permissions...")

    setup_roles()
    grant_data_access_permissions()
    setup_sample_users()

    print("[INFO] Role setup complete!")
    print("")
    print("Available roles:")
    print("  - Admin: Full access (built-in)")
    print("  - Analyst: Create/edit dashboards, charts, datasets, SQL Lab access")
    print("  - Viewer: Read-only access to dashboards and charts")
    print("")
    print("Sample users created:")
    print("  - analyst / analyst123 (Analyst role)")
    print("  - viewer / viewer123 (Viewer role)")


if __name__ == "__main__":
    main()
