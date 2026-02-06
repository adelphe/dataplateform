#!/usr/bin/env bash
#
# Deployment script for Data Platform
#
# Usage:
#   ./scripts/deploy.sh <environment>
#
# Environments:
#   - staging: Deploy to staging environment
#   - production: Deploy to production environment
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - kubectl configured (for k8s deployments)
#   - AWS CLI configured (for cloud deployments)
#   - Required environment variables set

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
DOCKER_REGISTRY="${DOCKER_REGISTRY:-ghcr.io}"
IMAGE_NAME="${IMAGE_NAME:-data-platform/airflow}"
DBT_PROJECT_DIR="${PROJECT_ROOT}/transformations/dbt_project"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Print usage
usage() {
    echo "Usage: $0 <environment>"
    echo ""
    echo "Environments:"
    echo "  staging     Deploy to staging environment"
    echo "  production  Deploy to production environment"
    echo ""
    echo "Options:"
    echo "  --dry-run   Show what would be deployed without making changes"
    echo "  --skip-tests  Skip running tests before deployment"
    echo "  --force     Force deployment even if checks fail"
    echo ""
    echo "Environment variables:"
    echo "  DEPLOY_TOKEN     Deployment authentication token"
    echo "  STAGING_HOST     Staging server hostname"
    echo "  PRODUCTION_HOST  Production server hostname"
    echo "  DOCKER_REGISTRY  Docker registry URL (default: ghcr.io)"
    exit 1
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_deps=()

    # Check required commands
    for cmd in docker docker-compose git; do
        if ! command -v "$cmd" &> /dev/null; then
            missing_deps+=("$cmd")
        fi
    done

    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        exit 1
    fi

    # Check Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running"
        exit 1
    fi

    log_success "All prerequisites met"
}

# Validate environment
validate_environment() {
    local env=$1
    log_info "Validating environment: $env"

    case $env in
        staging)
            if [ -z "${STAGING_HOST:-}" ]; then
                log_warning "STAGING_HOST not set, using default"
            fi
            ;;
        production)
            if [ -z "${PRODUCTION_HOST:-}" ]; then
                log_error "PRODUCTION_HOST must be set for production deployment"
                exit 1
            fi
            if [ -z "${DEPLOY_TOKEN:-}" ]; then
                log_error "DEPLOY_TOKEN must be set for production deployment"
                exit 1
            fi
            ;;
        *)
            log_error "Unknown environment: $env"
            usage
            ;;
    esac

    log_success "Environment validated"
}

# Run pre-deployment tests
run_tests() {
    if [ "${SKIP_TESTS:-false}" = "true" ]; then
        log_warning "Skipping tests (--skip-tests flag set)"
        return 0
    fi

    log_info "Running pre-deployment tests..."

    # Run Python tests
    log_info "Running Python tests..."
    if ! pytest tests/unit/ -v --tb=short; then
        log_error "Unit tests failed"
        return 1
    fi

    # Validate DAGs
    log_info "Validating Airflow DAGs..."
    if ! python -c "
import sys
sys.path.insert(0, '.')
from airflow.models import DagBag
bag = DagBag(dag_folder='pipelines/dags', include_examples=False)
if bag.import_errors:
    for dag, err in bag.import_errors.items():
        print(f'Error in {dag}: {err}')
    sys.exit(1)
print(f'Validated {len(bag.dags)} DAGs')
"; then
        log_error "DAG validation failed"
        return 1
    fi

    # Validate dbt project
    log_info "Validating dbt project..."
    cd "$DBT_PROJECT_DIR"
    if ! dbt parse --profiles-dir . --target ci; then
        log_error "dbt validation failed"
        cd "$PROJECT_ROOT"
        return 1
    fi
    cd "$PROJECT_ROOT"

    log_success "All tests passed"
}

# Build Docker images
build_images() {
    local env=$1
    local tag="${2:-latest}"

    log_info "Building Docker images for $env (tag: $tag)..."

    # Build Airflow image
    docker build \
        -f infrastructure/airflow/Dockerfile \
        -t "${DOCKER_REGISTRY}/${IMAGE_NAME}:${tag}" \
        -t "${DOCKER_REGISTRY}/${IMAGE_NAME}:${env}" \
        --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
        --build-arg VERSION="${tag}" \
        .

    log_success "Docker images built successfully"
}

# Push Docker images
push_images() {
    local env=$1
    local tag="${2:-latest}"

    if [ "${DRY_RUN:-false}" = "true" ]; then
        log_warning "[DRY RUN] Would push images: ${DOCKER_REGISTRY}/${IMAGE_NAME}:${tag}"
        return 0
    fi

    log_info "Pushing Docker images..."

    docker push "${DOCKER_REGISTRY}/${IMAGE_NAME}:${tag}"
    docker push "${DOCKER_REGISTRY}/${IMAGE_NAME}:${env}"

    log_success "Docker images pushed successfully"
}

# Deploy to staging
deploy_staging() {
    log_info "Deploying to staging environment..."

    local host="${STAGING_HOST:-localhost}"
    local tag="staging-$(git rev-parse --short HEAD)"

    # Build and push images
    build_images "staging" "$tag"
    push_images "staging" "$tag"

    if [ "${DRY_RUN:-false}" = "true" ]; then
        log_warning "[DRY RUN] Would deploy to staging at $host"
        return 0
    fi

    # Deploy using docker-compose (for local/VM staging)
    if [ "$host" = "localhost" ]; then
        log_info "Deploying locally with docker-compose..."
        docker-compose -f docker-compose.yml \
            -f docker-compose.staging.yml \
            up -d --build
    else
        # Deploy to remote staging server
        log_info "Deploying to remote staging server: $host"
        ssh "$host" "cd /opt/data-platform && \
            docker-compose pull && \
            docker-compose up -d --force-recreate"
    fi

    # Run dbt migrations
    run_dbt_migrations "staging"

    # Health check
    health_check "staging" "$host"

    log_success "Staging deployment complete"
}

# Deploy to production
deploy_production() {
    log_info "Deploying to production environment..."

    local host="${PRODUCTION_HOST}"
    local tag="v$(date +'%Y%m%d')-$(git rev-parse --short HEAD)"

    # Confirm production deployment
    if [ "${FORCE:-false}" != "true" ]; then
        echo ""
        log_warning "You are about to deploy to PRODUCTION!"
        echo -e "  Host: ${YELLOW}$host${NC}"
        echo -e "  Tag: ${YELLOW}$tag${NC}"
        echo ""
        read -p "Are you sure you want to continue? [y/N] " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Deployment cancelled"
            exit 0
        fi
    fi

    # Build and push images
    build_images "production" "$tag"
    push_images "production" "$tag"

    if [ "${DRY_RUN:-false}" = "true" ]; then
        log_warning "[DRY RUN] Would deploy to production at $host"
        return 0
    fi

    # Create deployment backup
    create_backup "production" "$host"

    # Deploy to production
    log_info "Deploying to production server: $host"
    ssh "$host" "cd /opt/data-platform && \
        docker-compose pull && \
        docker-compose up -d --force-recreate"

    # Run dbt migrations
    run_dbt_migrations "production"

    # Health check
    if ! health_check "production" "$host"; then
        log_error "Health check failed, initiating rollback..."
        rollback "production" "$host"
        exit 1
    fi

    # Tag release
    git tag -a "$tag" -m "Production deployment $tag"
    git push origin "$tag"

    log_success "Production deployment complete"
}

# Run dbt migrations
run_dbt_migrations() {
    local env=$1
    log_info "Running dbt migrations for $env..."

    if [ "${DRY_RUN:-false}" = "true" ]; then
        log_warning "[DRY RUN] Would run dbt migrations"
        return 0
    fi

    cd "$DBT_PROJECT_DIR"

    # Install dependencies
    dbt deps --profiles-dir . --target "$env"

    # Run seeds
    dbt seed --profiles-dir . --target "$env"

    # Run models
    dbt run --profiles-dir . --target "$env"

    # Run tests
    if ! dbt test --profiles-dir . --target "$env"; then
        log_warning "Some dbt tests failed, check results"
    fi

    cd "$PROJECT_ROOT"
    log_success "dbt migrations complete"
}

# Health check
health_check() {
    local env=$1
    local host=$2
    local max_retries=30
    local retry_interval=10

    log_info "Running health checks for $env at $host..."

    local airflow_url="http://${host}:8080/health"
    if [ "$host" = "localhost" ]; then
        airflow_url="http://localhost:8080/health"
    fi

    for ((i=1; i<=max_retries; i++)); do
        if curl -sf "$airflow_url" > /dev/null 2>&1; then
            log_success "Airflow is healthy"
            return 0
        fi
        log_info "Waiting for Airflow to be healthy... ($i/$max_retries)"
        sleep $retry_interval
    done

    log_error "Health check failed after $max_retries attempts"
    return 1
}

# Create backup
create_backup() {
    local env=$1
    local host=$2
    local backup_dir="/opt/data-platform/backups"
    local backup_name="backup-$(date +'%Y%m%d-%H%M%S')"

    log_info "Creating backup before deployment..."

    if [ "${DRY_RUN:-false}" = "true" ]; then
        log_warning "[DRY RUN] Would create backup: $backup_name"
        return 0
    fi

    ssh "$host" "mkdir -p $backup_dir && \
        docker-compose exec -T postgres pg_dump -U airflow airflow > $backup_dir/$backup_name.sql && \
        docker-compose config > $backup_dir/$backup_name-compose.yml"

    log_success "Backup created: $backup_name"
}

# Rollback
rollback() {
    local env=$1
    local host=$2

    log_warning "Rolling back $env deployment..."

    local latest_backup=$(ssh "$host" "ls -t /opt/data-platform/backups/*.sql | head -1")

    if [ -z "$latest_backup" ]; then
        log_error "No backup found for rollback"
        return 1
    fi

    log_info "Restoring from: $latest_backup"

    ssh "$host" "cd /opt/data-platform && \
        docker-compose down && \
        docker-compose up -d postgres && \
        sleep 10 && \
        docker-compose exec -T postgres psql -U airflow airflow < $latest_backup && \
        docker-compose up -d"

    log_success "Rollback complete"
}

# Cleanup old images
cleanup_old_images() {
    log_info "Cleaning up old Docker images..."

    docker image prune -f --filter "label=app=data-platform" --filter "until=168h"

    log_success "Cleanup complete"
}

# Main
main() {
    local environment=""
    DRY_RUN="false"
    SKIP_TESTS="false"
    FORCE="false"

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            staging|production)
                environment="$1"
                shift
                ;;
            --dry-run)
                DRY_RUN="true"
                shift
                ;;
            --skip-tests)
                SKIP_TESTS="true"
                shift
                ;;
            --force)
                FORCE="true"
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    if [ -z "$environment" ]; then
        log_error "Environment not specified"
        usage
    fi

    # Change to project root
    cd "$PROJECT_ROOT"

    echo ""
    echo "=================================="
    echo "  Data Platform Deployment"
    echo "=================================="
    echo "  Environment: $environment"
    echo "  Dry Run: $DRY_RUN"
    echo "  Skip Tests: $SKIP_TESTS"
    echo "=================================="
    echo ""

    # Run deployment steps
    check_prerequisites
    validate_environment "$environment"

    if ! run_tests; then
        if [ "${FORCE:-false}" != "true" ]; then
            log_error "Tests failed, use --force to deploy anyway"
            exit 1
        fi
        log_warning "Proceeding despite test failures (--force)"
    fi

    case $environment in
        staging)
            deploy_staging
            ;;
        production)
            deploy_production
            ;;
    esac

    cleanup_old_images

    echo ""
    log_success "Deployment to $environment completed successfully!"
}

main "$@"
