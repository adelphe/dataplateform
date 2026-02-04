#!/bin/bash
set -euo pipefail

# MinIO Bucket Initialization Script
# Creates the medallion architecture bucket structure:
#   - raw (Bronze): Raw ingested data, immutable, original format
#   - staging (Silver): Cleaned, validated, deduplicated data
#   - curated (Gold): Business-ready aggregated data, optimized for analytics

MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_USER="${MINIO_ROOT_USER:-minio}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minio123}"
MINIO_ALIAS="local"

echo "========================================="
echo " MinIO Bucket Initialization"
echo "========================================="

# Wait for MinIO to be ready
echo "[INFO] Waiting for MinIO to be available at ${MINIO_HOST}:${MINIO_PORT}..."
until mc alias set "${MINIO_ALIAS}" "http://${MINIO_HOST}:${MINIO_PORT}" "${MINIO_USER}" "${MINIO_PASS}" >/dev/null 2>&1; do
    echo "[INFO] MinIO not ready yet, retrying in 2 seconds..."
    sleep 2
done
echo "[INFO] MinIO is available."

# Function to create a bucket idempotently
create_bucket() {
    local bucket_name="$1"
    local description="$2"

    if mc ls "${MINIO_ALIAS}/${bucket_name}" >/dev/null 2>&1; then
        echo "[INFO] Bucket '${bucket_name}' already exists (${description})"
    else
        mc mb "${MINIO_ALIAS}/${bucket_name}"
        echo "[OK]   Created bucket '${bucket_name}' (${description})"
    fi
}

# Function to create a placeholder to establish directory structure
create_prefix() {
    local bucket_name="$1"
    local prefix="$2"

    # Create an empty placeholder object to establish the prefix
    echo -n "" | mc pipe "${MINIO_ALIAS}/${bucket_name}/${prefix}/.keep" 2>/dev/null || true
}

echo ""
echo "[STEP 1] Creating medallion architecture buckets..."
echo "-----------------------------------------"

create_bucket "raw"     "Bronze layer - raw ingested data"
create_bucket "staging" "Silver layer - cleaned and validated data"
create_bucket "curated" "Gold layer - business-ready aggregated data"

echo ""
echo "[STEP 2] Creating directory structure..."
echo "-----------------------------------------"

# Bronze layer (raw) - organized by data source type
for prefix in postgres files api streaming; do
    create_prefix "raw" "${prefix}"
    echo "[OK]   Created prefix raw/${prefix}/"
done

# Silver layer (staging) - organized by domain
for prefix in cleaned validated deduplicated; do
    create_prefix "staging" "${prefix}"
    echo "[OK]   Created prefix staging/${prefix}/"
done

# Gold layer (curated) - organized by consumption pattern
for prefix in aggregated reports ml; do
    create_prefix "curated" "${prefix}"
    echo "[OK]   Created prefix curated/${prefix}/"
done

echo ""
echo "[STEP 3] Enabling bucket versioning..."
echo "-----------------------------------------"

for bucket in raw staging curated; do
    mc version enable "${MINIO_ALIAS}/${bucket}" 2>/dev/null && \
        echo "[OK]   Versioning enabled for '${bucket}'" || \
        echo "[WARN] Could not enable versioning for '${bucket}' (may require license)"
done

echo ""
echo "========================================="
echo " Bucket initialization complete!"
echo "========================================="
echo ""
mc ls "${MINIO_ALIAS}/"
echo ""
