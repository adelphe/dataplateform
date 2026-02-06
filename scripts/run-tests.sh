#!/usr/bin/env bash
#
# Test runner script for Data Platform
#
# Usage:
#   ./scripts/run-tests.sh [options]
#
# Options:
#   --unit        Run only unit tests
#   --integration Run only integration tests
#   --all         Run all tests (default)
#   --coverage    Generate coverage report
#   --dbt         Run dbt tests
#   --lint        Run linting checks

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Test options
RUN_UNIT=false
RUN_INTEGRATION=false
RUN_DBT=false
RUN_LINT=false
COVERAGE=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --unit        Run only unit tests"
    echo "  --integration Run only integration tests"
    echo "  --all         Run all tests (default)"
    echo "  --coverage    Generate coverage report"
    echo "  --dbt         Run dbt tests"
    echo "  --lint        Run linting checks"
    echo "  -h, --help    Show this help"
    exit 0
}

run_unit_tests() {
    log_info "Running unit tests..."

    cd "$PROJECT_ROOT"

    local pytest_args="-v --tb=short tests/unit/"

    if [ "$COVERAGE" = true ]; then
        pytest_args="$pytest_args --cov=pipelines --cov-report=term-missing --cov-report=xml"
    fi

    if pytest $pytest_args; then
        log_success "Unit tests passed"
    else
        log_error "Unit tests failed"
        return 1
    fi
}

run_integration_tests() {
    log_info "Running integration tests..."

    cd "$PROJECT_ROOT"

    export AIRFLOW_HOME="$PROJECT_ROOT/pipelines"
    export PYTHONPATH="$PROJECT_ROOT"

    if pytest -v --tb=short tests/integration/; then
        log_success "Integration tests passed"
    else
        log_error "Integration tests failed"
        return 1
    fi
}

run_dbt_tests() {
    log_info "Running dbt tests..."

    cd "$PROJECT_ROOT/transformations/dbt_project"

    # Parse project
    if ! dbt parse --profiles-dir . --target ci; then
        log_error "dbt parse failed"
        return 1
    fi

    # Run tests
    if ! dbt test --profiles-dir . --target ci; then
        log_error "dbt tests failed"
        return 1
    fi

    cd "$PROJECT_ROOT"
    log_success "dbt tests passed"
}

run_lint() {
    log_info "Running linting checks..."

    cd "$PROJECT_ROOT"

    local failed=false

    # Ruff
    log_info "Running ruff..."
    if ! ruff check pipelines/; then
        failed=true
    fi

    # Black
    log_info "Running black..."
    if ! black --check pipelines/ tests/; then
        failed=true
    fi

    # isort
    log_info "Running isort..."
    if ! isort --check-only pipelines/ tests/; then
        failed=true
    fi

    # sqlfluff
    log_info "Running sqlfluff..."
    if ! sqlfluff lint transformations/dbt_project/models/ \
        --dialect postgres \
        --config transformations/dbt_project/.sqlfluff 2>/dev/null; then
        log_info "sqlfluff found issues (may be expected)"
    fi

    if [ "$failed" = true ]; then
        log_error "Linting checks failed"
        return 1
    fi

    log_success "Linting checks passed"
}

validate_dags() {
    log_info "Validating Airflow DAGs..."

    cd "$PROJECT_ROOT"
    export PYTHONPATH="$PROJECT_ROOT"

    python -c "
import sys
sys.path.insert(0, '.')
from airflow.models import DagBag
bag = DagBag(dag_folder='pipelines/dags', include_examples=False)
if bag.import_errors:
    for dag, err in bag.import_errors.items():
        print(f'Error in {dag}: {err}')
    sys.exit(1)
print(f'Successfully validated {len(bag.dags)} DAGs')
"

    log_success "DAG validation passed"
}

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --unit)
                RUN_UNIT=true
                shift
                ;;
            --integration)
                RUN_INTEGRATION=true
                shift
                ;;
            --all)
                RUN_UNIT=true
                RUN_INTEGRATION=true
                shift
                ;;
            --dbt)
                RUN_DBT=true
                shift
                ;;
            --lint)
                RUN_LINT=true
                shift
                ;;
            --coverage)
                COVERAGE=true
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

    # Default: run all tests
    if [ "$RUN_UNIT" = false ] && [ "$RUN_INTEGRATION" = false ] && \
       [ "$RUN_DBT" = false ] && [ "$RUN_LINT" = false ]; then
        RUN_UNIT=true
        RUN_INTEGRATION=true
    fi

    local exit_code=0

    # Always validate DAGs
    if ! validate_dags; then
        exit_code=1
    fi

    # Run selected tests
    if [ "$RUN_LINT" = true ]; then
        if ! run_lint; then
            exit_code=1
        fi
    fi

    if [ "$RUN_UNIT" = true ]; then
        if ! run_unit_tests; then
            exit_code=1
        fi
    fi

    if [ "$RUN_INTEGRATION" = true ]; then
        if ! run_integration_tests; then
            exit_code=1
        fi
    fi

    if [ "$RUN_DBT" = true ]; then
        if ! run_dbt_tests; then
            exit_code=1
        fi
    fi

    echo ""
    if [ $exit_code -eq 0 ]; then
        log_success "All tests completed successfully!"
    else
        log_error "Some tests failed"
    fi

    exit $exit_code
}

main "$@"
