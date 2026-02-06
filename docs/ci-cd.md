# CI/CD Pipeline Documentation

This document describes the Continuous Integration and Continuous Deployment (CI/CD) pipeline for the Data Platform.

## Overview

The CI/CD pipeline automates testing, building, and deployment of the data platform components including:

- **Airflow DAGs and custom operators**
- **dbt transformations**
- **Great Expectations data quality checks**
- **Docker images**

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CI/CD Pipeline Flow                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐     │
│   │  Commit  │───▶│   Lint   │───▶│  Tests   │───▶│    Build     │     │
│   └──────────┘    └──────────┘    └──────────┘    └──────────────┘     │
│                         │              │                 │              │
│                         ▼              ▼                 ▼              │
│                   ┌──────────┐   ┌──────────┐     ┌──────────────┐     │
│                   │ Security │   │   DAG    │     │   Docker     │     │
│                   │   Scan   │   │ Validate │     │    Push      │     │
│                   └──────────┘   └──────────┘     └──────────────┘     │
│                                       │                 │              │
│                                       ▼                 ▼              │
│                               ┌────────────┐    ┌──────────────┐      │
│                               │  dbt Test  │    │   Deploy     │      │
│                               └────────────┘    └──────────────┘      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## GitHub Actions Workflow

The main CI/CD workflow is defined in `.github/workflows/ci.yml`.

### Trigger Events

| Event | Branch | Action |
|-------|--------|--------|
| Push | `main` | Full CI + Production Deploy |
| Push | `develop` | Full CI + Staging Deploy |
| Pull Request | `main`, `develop` | CI only (no deploy) |
| Manual | Any | Configurable deployment |

### Jobs

#### 1. Lint (`lint`)

Code quality checks for Python and SQL:

```yaml
- ruff: Python linting
- black: Code formatting
- isort: Import sorting
- yamllint: YAML validation
- sqlfluff: SQL linting for dbt models
```

#### 2. Unit Tests (`unit-tests`)

Runs pytest for operator and utility tests:

```bash
pytest tests/unit/ -v --cov=pipelines --cov-report=xml
```

Coverage reports are uploaded to Codecov.

#### 3. Integration Tests (`integration-tests`)

Runs with PostgreSQL and MinIO services:

```bash
pytest tests/integration/ -v
```

#### 4. DAG Validation (`dag-validation`)

Validates Airflow DAGs can be imported:

- No import errors
- No cyclic dependencies
- Task integrity checks

#### 5. dbt Tests (`dbt-tests`)

Validates dbt project:

```bash
dbt debug      # Validate connection
dbt parse      # Parse project
dbt compile    # Compile models
dbt test       # Run tests
dbt docs       # Generate documentation
```

#### 6. Security Scan (`security-scan`)

- **bandit**: Python security linter
- **safety**: Dependency vulnerability check

#### 7. Build Docker (`build-docker`)

Builds and pushes Docker images to GitHub Container Registry:

```bash
docker build -f infrastructure/airflow/Dockerfile .
docker push ghcr.io/<org>/data-platform/airflow:<tag>
```

#### 8. Deploy (`deploy-staging` / `deploy-production`)

Automated deployment to target environments.

## Pre-commit Hooks

Install pre-commit hooks for local development:

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run all hooks manually
pre-commit run --all-files
```

### Available Hooks

| Hook | Description | Stage |
|------|-------------|-------|
| `check-yaml` | Validate YAML syntax | commit |
| `check-json` | Validate JSON syntax | commit |
| `end-of-file-fixer` | Ensure files end with newline | commit |
| `trailing-whitespace` | Remove trailing whitespace | commit |
| `black` | Format Python code | commit |
| `isort` | Sort imports | commit |
| `ruff` | Lint Python code | commit |
| `mypy` | Type checking | commit |
| `bandit` | Security linting | commit |
| `sqlfluff-lint` | Lint SQL files | commit |
| `detect-secrets` | Detect secrets | commit |
| `hadolint` | Lint Dockerfiles | commit |
| `shellcheck` | Lint shell scripts | commit |
| `dbt-compile` | Validate dbt models | push |
| `airflow-dag-test` | Validate DAG imports | push |

## Testing

### Running Tests Locally

```bash
# Run all tests
./scripts/run-tests.sh --all

# Run only unit tests
./scripts/run-tests.sh --unit

# Run integration tests
./scripts/run-tests.sh --integration

# Run with coverage
./scripts/run-tests.sh --unit --coverage

# Run linting
./scripts/run-tests.sh --lint

# Run dbt tests
./scripts/run-tests.sh --dbt
```

### Test Directory Structure

```
tests/
├── unit/
│   └── operators/
│       ├── test_minio_operator.py
│       ├── test_dbt_operator.py
│       ├── test_data_quality_operator.py
│       ├── test_monitoring_operator.py
│       ├── test_ge_operator.py
│       └── test_file_sensor_operator.py
├── integration/
│   ├── dags/
│   │   ├── test_dag_integrity.py
│   │   └── test_dag_execution.py
│   ├── test_minio_buckets.py
│   └── test_alerting.py
└── conftest.py
```

### Writing Tests

#### Unit Tests

```python
# tests/unit/operators/test_my_operator.py
import pytest
from unittest.mock import MagicMock, patch

from pipelines.operators.my_operator import MyOperator

class TestMyOperator:
    @pytest.fixture
    def mock_context(self):
        return {"ti": MagicMock()}

    def test_operator_initialization(self):
        op = MyOperator(task_id="test", param="value")
        assert op.param == "value"

    @patch("pipelines.operators.my_operator.SomeClient")
    def test_execute_success(self, mock_client, mock_context):
        op = MyOperator(task_id="test")
        result = op.execute(mock_context)
        assert result == "expected"
```

#### Integration Tests

```python
# tests/integration/dags/test_my_dag.py
import pytest
from airflow.models import DagBag

class TestMyDAG:
    @pytest.fixture(scope="class")
    def dag_bag(self):
        return DagBag(dag_folder="pipelines/dags")

    def test_dag_loads(self, dag_bag):
        assert "my_dag_id" in dag_bag.dags
```

## Deployment

### Manual Deployment

```bash
# Deploy to staging
./scripts/deploy.sh staging

# Deploy to production
./scripts/deploy.sh production

# Dry run (show what would happen)
./scripts/deploy.sh staging --dry-run

# Skip tests
./scripts/deploy.sh staging --skip-tests

# Force deployment
./scripts/deploy.sh production --force
```

### Deployment Process

1. **Validation**
   - Check prerequisites (docker, git, etc.)
   - Validate environment variables

2. **Testing**
   - Run unit tests
   - Validate DAGs
   - Validate dbt project

3. **Build**
   - Build Docker images
   - Tag with version/commit SHA

4. **Push**
   - Push images to registry

5. **Deploy**
   - Create backup (production only)
   - Update containers
   - Run dbt migrations
   - Health checks

6. **Rollback** (if needed)
   - Restore from backup
   - Restart services

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DEPLOY_TOKEN` | Deployment authentication | Production |
| `STAGING_HOST` | Staging server hostname | Staging |
| `PRODUCTION_HOST` | Production server hostname | Production |
| `DOCKER_REGISTRY` | Container registry URL | Optional |
| `SLACK_WEBHOOK_URL` | Slack notifications | Optional |

## SQL Linting with sqlfluff

Configuration is in `transformations/dbt_project/.sqlfluff`.

### Running sqlfluff

```bash
# Lint models
sqlfluff lint transformations/dbt_project/models/

# Fix automatically
sqlfluff fix transformations/dbt_project/models/

# Lint specific file
sqlfluff lint transformations/dbt_project/models/staging/stg_transactions.sql
```

### Key Rules

- **Lowercase keywords**: All SQL keywords should be lowercase
- **Explicit aliasing**: Tables must have explicit aliases
- **Trailing commas**: No trailing commas in SELECT
- **Line length**: Max 120 characters
- **Indentation**: 4 spaces

## dbt Testing

### Built-in Tests

Configure in schema YAML files:

```yaml
# models/staging/sources.yml
version: 2

models:
  - name: stg_transactions
    columns:
      - name: transaction_id
        tests:
          - unique
          - not_null
      - name: amount
        tests:
          - not_null
```

### Custom Tests

Create in `tests/` directory:

```sql
-- tests/assert_positive_amounts.sql
select *
from {{ ref('stg_transactions') }}
where amount < 0
```

### Running dbt Tests

```bash
cd transformations/dbt_project

# Run all tests
dbt test

# Run specific model tests
dbt test --select stg_transactions

# Run only data tests
dbt test --select test_type:data

# Run only schema tests
dbt test --select test_type:schema
```

## Monitoring and Alerts

### Slack Notifications

The pipeline sends Slack notifications on:

- Build failures
- Deployment failures
- Test failures

Configure `SLACK_WEBHOOK_URL` secret in GitHub.

### Artifacts

Each CI run produces:

- Test results (JUnit XML)
- Coverage reports
- Security scan reports
- dbt artifacts (manifest.json, catalog.json)

## Troubleshooting

### Common Issues

#### DAG Import Errors

```bash
# Check DAG imports locally
python -c "
from airflow.models import DagBag
bag = DagBag('pipelines/dags')
print(bag.import_errors)
"
```

#### dbt Connection Issues

```bash
# Test dbt connection
cd transformations/dbt_project
dbt debug --profiles-dir . --target ci
```

#### Docker Build Failures

```bash
# Build with verbose output
docker build -f infrastructure/airflow/Dockerfile . --progress=plain
```

### Getting Help

1. Check CI logs in GitHub Actions
2. Run tests locally with verbose output
3. Check the `#data-platform` Slack channel
4. Create an issue in the repository

## Best Practices

### For Developers

1. **Run pre-commit hooks** before pushing
2. **Write tests** for new operators
3. **Update dbt schema tests** when adding models
4. **Follow SQL style guide** (see sqlfluff config)
5. **Use meaningful commit messages** (conventional commits)

### For Reviewers

1. Check CI status before approving
2. Verify test coverage for new code
3. Review dbt model documentation
4. Check for security scan warnings

### For Deployments

1. Always deploy to staging first
2. Monitor health checks after deployment
3. Have rollback plan ready for production
4. Communicate deployments to the team
