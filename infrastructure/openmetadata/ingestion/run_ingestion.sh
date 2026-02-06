#!/bin/bash
# OpenMetadata Ingestion Runner Script
# Runs all configured ingestion workflows on a schedule

set -e

INGESTION_DIR="/opt/airflow/dags"
LOG_DIR="/var/log/openmetadata-ingestion"
mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

wait_for_server() {
    log "Waiting for OpenMetadata server to be ready..."
    local max_attempts=60
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "http://openmetadata-server:8585/api/v1/system/version" > /dev/null 2>&1; then
            log "OpenMetadata server is ready"
            return 0
        fi
        log "Attempt $attempt/$max_attempts: Server not ready, waiting..."
        sleep 10
        attempt=$((attempt + 1))
    done

    log "ERROR: OpenMetadata server did not become ready"
    return 1
}

run_ingestion() {
    local config_file="$1"
    local config_name=$(basename "$config_file" .yaml)
    local log_file="$LOG_DIR/${config_name}_$(date '+%Y%m%d_%H%M%S').log"

    log "Starting ingestion: $config_name"

    if [ -f "$config_file" ]; then
        # Export auth environment variables for the ingestion process
        export OPENMETADATA_AUTH_PROVIDER="${OPENMETADATA_AUTH_PROVIDER:-basic}"
        export OPENMETADATA_JWT_TOKEN="${OPENMETADATA_JWT_TOKEN:-}"
        export OPENMETADATA_USERNAME="${OPENMETADATA_USERNAME:-admin}"
        export OPENMETADATA_PASSWORD="${OPENMETADATA_PASSWORD:-admin}"

        if metadata ingest -c "$config_file" >> "$log_file" 2>&1; then
            log "SUCCESS: $config_name ingestion completed"
        else
            log "ERROR: $config_name ingestion failed. Check $log_file for details"
        fi
    else
        log "WARNING: Config file not found: $config_file"
    fi
}

run_all_ingestions() {
    log "========================================="
    log "Starting OpenMetadata ingestion cycle"
    log "========================================="

    # Run ingestions in order: sources first, then lineage
    run_ingestion "$INGESTION_DIR/postgres_ingestion.yaml"
    run_ingestion "$INGESTION_DIR/minio_ingestion.yaml"
    run_ingestion "$INGESTION_DIR/minio_bronze_ingestion.yaml"
    run_ingestion "$INGESTION_DIR/dbt_ingestion.yaml"
    run_ingestion "$INGESTION_DIR/lineage_ingestion.yaml"

    log "========================================="
    log "Ingestion cycle completed"
    log "========================================="
}

cleanup_old_logs() {
    # Keep logs for 7 days
    find "$LOG_DIR" -name "*.log" -mtime +7 -delete 2>/dev/null || true
}

# Main execution
log "OpenMetadata Ingestion Runner starting..."

# Wait for server to be ready
wait_for_server

# Run initial ingestion
run_all_ingestions

# Setup cron schedule (every 6 hours by default)
INGESTION_CRON="${INGESTION_CRON_SCHEDULE:-0 */6 * * *}"
log "Setting up cron schedule: $INGESTION_CRON"

# Create cron job
echo "$INGESTION_CRON /opt/airflow/dags/run_ingestion.sh --scheduled >> /var/log/openmetadata-ingestion/cron.log 2>&1" > /etc/cron.d/openmetadata-ingestion
chmod 0644 /etc/cron.d/openmetadata-ingestion
crontab /etc/cron.d/openmetadata-ingestion

# Handle scheduled runs (called from cron)
if [ "$1" = "--scheduled" ]; then
    cleanup_old_logs
    run_all_ingestions
    exit 0
fi

# Start cron daemon and keep container alive
log "Starting cron daemon..."
cron

# Keep container running and tail logs
log "Ingestion runner is active. Monitoring logs..."
tail -f "$LOG_DIR"/*.log 2>/dev/null || tail -f /dev/null
