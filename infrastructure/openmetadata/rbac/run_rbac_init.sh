#!/bin/bash
# OpenMetadata RBAC Initialization Script
# Waits for server health and initializes RBAC configuration

set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

OPENMETADATA_SERVER="${OPENMETADATA_SERVER:-http://openmetadata-server:8585}"
RBAC_CONFIG="${RBAC_CONFIG:-/opt/openmetadata/rbac/roles_config.yaml}"
MAX_RETRIES=60
RETRY_INTERVAL=10

log "OpenMetadata RBAC Initializer starting..."
log "Server: $OPENMETADATA_SERVER"
log "Config: $RBAC_CONFIG"

# Wait for OpenMetadata server to be healthy
log "Waiting for OpenMetadata server to be healthy..."
attempt=1
while [ $attempt -le $MAX_RETRIES ]; do
    if curl -sf "${OPENMETADATA_SERVER}/api/v1/system/version" > /dev/null 2>&1; then
        log "OpenMetadata server is healthy"
        break
    fi

    log "Attempt $attempt/$MAX_RETRIES: Server not ready, waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
    attempt=$((attempt + 1))
done

if [ $attempt -gt $MAX_RETRIES ]; then
    log "ERROR: OpenMetadata server did not become healthy after $MAX_RETRIES attempts"
    exit 1
fi

# Additional wait for server to fully initialize
log "Waiting additional 30s for server to fully initialize..."
sleep 30

# Run RBAC initialization
log "Initializing RBAC configuration..."

cd /opt/openmetadata/rbac

# Build the command with authentication
CMD="python init_rbac.py --config $RBAC_CONFIG --server $OPENMETADATA_SERVER"

# Add JWT token if provided (preferred)
if [ -n "$OPENMETADATA_JWT_TOKEN" ]; then
    CMD="$CMD --token $OPENMETADATA_JWT_TOKEN"
# Fallback to basic auth with username/password
elif [ -n "$OPENMETADATA_USERNAME" ] && [ -n "$OPENMETADATA_PASSWORD" ]; then
    CMD="$CMD --username $OPENMETADATA_USERNAME --password $OPENMETADATA_PASSWORD"
fi

# Execute RBAC initialization
if $CMD; then
    log "SUCCESS: RBAC initialization completed"
    exit 0
else
    log "ERROR: RBAC initialization failed"
    exit 1
fi
