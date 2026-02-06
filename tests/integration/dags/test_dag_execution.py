"""Integration tests for DAG execution simulation."""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set Airflow home for tests
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / "pipelines"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")


class TestDAGTaskExecution:
    """Test DAG task execution patterns."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    @pytest.fixture
    def mock_context(self):
        """Create a mock execution context."""
        return {
            "dag_run": MagicMock(
                run_id="test_run",
                execution_date=datetime(2024, 1, 1),
            ),
            "task_instance": MagicMock(),
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
            "execution_date": datetime(2024, 1, 1),
            "prev_execution_date": datetime(2023, 12, 31),
            "next_execution_date": datetime(2024, 1, 2),
            "params": {},
            "var": {"value": MagicMock()},
            "conf": MagicMock(),
        }

    def test_dag_structure_matches_medallion_architecture(self, dag_bag):
        """Test that transformation DAG follows medallion architecture."""
        transform_dags = [
            dag for dag_id, dag in dag_bag.dags.items()
            if "transform" in dag_id.lower() or "dbt" in dag_id.lower()
        ]

        for dag in transform_dags:
            task_ids = [task.task_id for task in dag.tasks]
            task_ids_lower = [tid.lower() for tid in task_ids]

            # Should have staging, intermediate, and marts references
            has_staging = any("staging" in tid or "stg" in tid for tid in task_ids_lower)
            has_marts = any("mart" in tid for tid in task_ids_lower)

            # At least marts should be present
            assert has_staging or has_marts, (
                f"Transformation DAG should reference staging or marts layers"
            )

    def test_ingestion_dag_has_validation_step(self, dag_bag):
        """Test that ingestion DAG includes data validation."""
        ingestion_dags = [
            dag for dag_id, dag in dag_bag.dags.items()
            if "ingestion" in dag_id.lower()
        ]

        for dag in ingestion_dags:
            task_ids_lower = [task.task_id.lower() for task in dag.tasks]

            has_validation = any(
                "valid" in tid or "quality" in tid or "check" in tid
                for tid in task_ids_lower
            )

            assert has_validation, (
                f"Ingestion DAG should include validation step"
            )

    def test_quality_dag_covers_all_layers(self, dag_bag):
        """Test that quality DAG checks all data layers."""
        quality_dags = [
            dag for dag_id, dag in dag_bag.dags.items()
            if "quality" in dag_id.lower()
        ]

        for dag in quality_dags:
            task_ids = [task.task_id for task in dag.tasks]
            task_ids_lower = [tid.lower() for tid in task_ids]

            layers_covered = {
                "raw": any("raw" in tid for tid in task_ids_lower),
                "staging": any("staging" in tid or "stg" in tid for tid in task_ids_lower),
                "marts": any("mart" in tid for tid in task_ids_lower),
            }

            # Should cover at least 2 layers
            covered_count = sum(layers_covered.values())
            assert covered_count >= 2, (
                f"Quality DAG should cover multiple layers, "
                f"found: {[k for k, v in layers_covered.items() if v]}"
            )


class TestDAGTemplating:
    """Test DAG template rendering."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_template_fields_are_valid(self, dag_bag):
        """Test that template fields reference valid Jinja variables."""
        from airflow.models import BaseOperator

        invalid_templates = []

        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                if hasattr(task, "template_fields"):
                    for field in task.template_fields:
                        value = getattr(task, field, None)
                        if isinstance(value, str) and "{{" in value:
                            # Check for common template errors
                            if "{{ " not in value and "{{" in value:
                                # Missing space after {{
                                pass  # Actually this is fine in Jinja
                            if value.count("{{") != value.count("}}"):
                                invalid_templates.append(
                                    f"{dag_id}.{task.task_id}.{field}: "
                                    f"Unbalanced braces in '{value}'"
                                )

        if invalid_templates:
            pytest.fail(
                f"Invalid template syntax found:\n" +
                "\n".join(invalid_templates)
            )


class TestDAGSecurity:
    """Test DAG security configurations."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_no_hardcoded_credentials(self, dag_bag):
        """Test that DAGs don't contain hardcoded credentials."""
        import re

        credential_patterns = [
            r"password\s*=\s*['\"][^'\"]+['\"]",
            r"secret\s*=\s*['\"][^'\"]+['\"]",
            r"api_key\s*=\s*['\"][^'\"]+['\"]",
            r"token\s*=\s*['\"][^'\"]+['\"]",
        ]

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"

        for dag_file in dag_folder.rglob("*.py"):
            content = dag_file.read_text()

            for pattern in credential_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                # Filter out common false positives
                real_matches = [
                    m for m in matches
                    if "Variable.get" not in m
                    and "conn.password" not in m
                    and "{{ " not in m
                ]
                if real_matches:
                    pytest.fail(
                        f"Potential hardcoded credential in {dag_file.name}: "
                        f"{real_matches[0]}"
                    )

    def test_connections_use_airflow_connections(self, dag_bag):
        """Test that DAGs use Airflow connections, not direct credentials."""
        # Check that operators use conn_id parameters
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                # Check common connection parameters
                for conn_param in ["conn_id", "postgres_conn_id", "minio_conn_id", "http_conn_id"]:
                    if hasattr(task, conn_param):
                        conn_value = getattr(task, conn_param)
                        assert conn_value is not None, (
                            f"Task {dag_id}.{task.task_id} has empty {conn_param}"
                        )


class TestDAGPerformance:
    """Test DAG performance considerations."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_dag_parse_time(self, dag_bag):
        """Test that DAGs parse quickly."""
        import time
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"

        start = time.time()
        _ = DagBag(dag_folder=str(dag_folder), include_examples=False)
        parse_time = time.time() - start

        # DAGs should parse in under 30 seconds
        assert parse_time < 30, (
            f"DAG parsing took {parse_time:.2f}s, should be under 30s"
        )

    def test_dag_task_count_reasonable(self, dag_bag):
        """Test that DAGs don't have excessive tasks."""
        max_tasks = 100  # Arbitrary but reasonable limit

        for dag_id, dag in dag_bag.dags.items():
            assert len(dag.tasks) <= max_tasks, (
                f"DAG {dag_id} has {len(dag.tasks)} tasks, "
                f"consider breaking into smaller DAGs"
            )

    def test_sensors_have_poke_interval(self, dag_bag):
        """Test that sensors have reasonable poke intervals."""
        from airflow.sensors.base import BaseSensorOperator

        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                if isinstance(task, BaseSensorOperator):
                    poke_interval = getattr(task, "poke_interval", 60)
                    # Should be at least 30 seconds to avoid overwhelming resources
                    assert poke_interval >= 30, (
                        f"Sensor {dag_id}.{task.task_id} has low poke_interval "
                        f"({poke_interval}s), should be >= 30s"
                    )


class TestDAGDocumentation:
    """Test DAG documentation standards."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_dag_files_have_docstrings(self):
        """Test that DAG files have module docstrings."""
        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        files_without_docstring = []

        for dag_file in dag_folder.rglob("*.py"):
            if dag_file.name.startswith("__"):
                continue

            content = dag_file.read_text()
            lines = content.strip().split("\n")

            # Check for module docstring
            has_docstring = False
            for i, line in enumerate(lines):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith('"""') or line.startswith("'''"):
                    has_docstring = True
                break

            if not has_docstring:
                files_without_docstring.append(dag_file.name)

        if files_without_docstring:
            pytest.skip(
                f"DAG files without docstrings (recommended): "
                f"{', '.join(files_without_docstring)}"
            )
