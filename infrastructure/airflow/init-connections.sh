#!/usr/bin/env bash
# Initialize Airflow connections for the data platform.
# This script is idempotent - it checks if connections exist before creating them.

set -euo pipefail

echo "=== Initializing Airflow connections ==="

# PostgreSQL connection
if airflow connections get postgres_default >/dev/null 2>&1; then
    echo "Connection 'postgres_default' already exists, skipping"
else
    echo "Creating connection 'postgres_default'"
    airflow connections add postgres_default \
        --conn-type postgres \
        --conn-host postgres \
        --conn-login "${POSTGRES_USER:-airflow}" \
        --conn-password "${POSTGRES_PASSWORD:-airflow}" \
        --conn-port 5432 \
        --conn-schema "${POSTGRES_DB:-airflow}"
fi

# MinIO (S3-compatible) connection
if airflow connections get minio_default >/dev/null 2>&1; then
    echo "Connection 'minio_default' already exists, skipping"
else
    echo "Creating connection 'minio_default'"
    airflow connections add minio_default \
        --conn-type aws \
        --conn-login "${MINIO_ROOT_USER:-minio}" \
        --conn-password "${MINIO_ROOT_PASSWORD:-minio123}" \
        --conn-extra "{\"endpoint_url\": \"http://minio:9000\"}"
fi

echo "=== Airflow connections initialized ==="
