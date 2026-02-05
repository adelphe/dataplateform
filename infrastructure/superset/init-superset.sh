#!/bin/bash
# Superset Initialization Script
# This script configures database connections, roles, and imports dashboards

set -euo pipefail

echo "========================================"
echo "Superset Initialization Script"
echo "========================================"

# Configuration
SUPERSET_URL="http://localhost:8088"
MAX_RETRIES=30
RETRY_INTERVAL=5

# Wait for Superset to be ready
wait_for_superset() {
    echo "[INFO] Waiting for Superset to be ready..."
    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$SUPERSET_URL/health" | grep -q "200"; then
            echo "[INFO] Superset is ready!"
            return 0
        fi
        retries=$((retries + 1))
        echo "[INFO] Waiting for Superset... (attempt $retries/$MAX_RETRIES)"
        sleep $RETRY_INTERVAL
    done
    echo "[ERROR] Superset did not become ready in time"
    return 1
}

# Configure PostgreSQL database connection
configure_database_connection() {
    echo "[INFO] Configuring PostgreSQL warehouse connection..."

    # Use Superset CLI to set database URI
    superset set-database-uri \
        --database-name "postgres_warehouse" \
        --uri "postgresql://airflow:airflow@postgres:5432/airflow" \
        2>/dev/null || {
            echo "[INFO] Database connection may already exist, attempting via Python..."
            python /app/superset_init/setup_database.py
        }

    echo "[INFO] Database connection configured successfully"
}

# Test database connectivity
test_database_connection() {
    echo "[INFO] Testing database connectivity..."

    python << 'EOF'
import sys
from sqlalchemy import create_engine, text

try:
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("[INFO] Database connection test successful")
        sys.exit(0)
except Exception as e:
    print(f"[ERROR] Database connection test failed: {e}")
    sys.exit(1)
EOF
}

# Setup roles and permissions
setup_roles() {
    echo "[INFO] Setting up roles and permissions..."
    python /app/superset_init/setup_roles.py
    echo "[INFO] Roles configured successfully"
}

# Import dashboards
import_dashboards() {
    echo "[INFO] Importing pre-configured dashboards..."
    python /app/superset_init/import_dashboards.py
    echo "[INFO] Dashboards imported successfully"
}

# Main execution
main() {
    echo "[INFO] Starting Superset initialization..."

    # Test database connectivity first
    test_database_connection

    # Configure database connection
    configure_database_connection

    # Setup roles
    setup_roles

    # Import dashboards
    import_dashboards

    echo "========================================"
    echo "Superset initialization complete!"
    echo "========================================"
    echo ""
    echo "Access Superset at: http://localhost:8088"
    echo "Default credentials: admin / admin"
    echo ""
    echo "Available dashboards:"
    echo "  - Customer Insights"
    echo "  - Product Performance"
    echo "  - Daily Operations"
    echo ""
}

main "$@"
