#!/bin/bash
set -e

echo "[INFO] Starting Superset initialization..."

# Install PostgreSQL driver for metadata database
pip install psycopg2-binary --quiet

# Run database migrations
echo "[INFO] Running database migrations..."
superset db upgrade

# Create admin user
echo "[INFO] Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" || true

# Initialize Superset (permissions, roles, etc.)
echo "[INFO] Initializing Superset..."
superset init

# Run custom initialization scripts
echo "[INFO] Running custom initialization scripts..."
python /app/superset_init/setup_database.py || echo "Database setup skipped"
python /app/superset_init/setup_roles.py || echo "Role setup skipped"
python /app/superset_init/import_dashboards.py || echo "Dashboard import skipped"

echo "[INFO] Initialization complete. Starting Gunicorn production server..."

# Start Gunicorn production server (instead of Flask dev server)
# - 4 workers (adjust based on available CPU cores)
# - 120s timeout for long-running queries
# - Access logs to stdout for container logging
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 120 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  --access-logfile - \
  --error-logfile - \
  "superset.app:create_app()"
