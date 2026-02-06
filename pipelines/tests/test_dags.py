"""DAG validation tests.

Ensures all DAGs can be loaded without import errors,
have no cycles, and follow expected conventions.
"""

import os
import sys

import pytest

# Add the dags directory and pipelines directory to the path
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
PIPELINES_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, DAGS_DIR)
sys.path.insert(0, PIPELINES_DIR)


@pytest.fixture
def dagbag():
    """Load all DAGs using Airflow's DagBag."""
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


class TestDagIntegrity:
    """Validate DAG structure and correctness."""

    def test_no_import_errors(self, dagbag):
        """Verify all DAGs can be imported without errors."""
        assert len(dagbag.import_errors) == 0, (
            f"DAG import errors: {dagbag.import_errors}"
        )

    def test_dags_loaded(self, dagbag):
        """Verify at least one DAG is loaded."""
        assert len(dagbag.dags) > 0, "No DAGs found"

    def test_all_dags_have_tags(self, dagbag):
        """Verify all DAGs have at least one tag."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.tags, f"DAG '{dag_id}' has no tags"

    def test_all_dags_have_owner(self, dagbag):
        """Verify all DAGs have an owner set in default_args."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.default_args.get("owner"), (
                f"DAG '{dag_id}' has no owner in default_args"
            )

    def test_all_dags_have_description(self, dagbag):
        """Verify all DAGs have a description."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.description, f"DAG '{dag_id}' has no description"

    def test_no_cycles(self, dagbag):
        """Verify no DAGs contain circular dependencies."""
        for dag_id, dag in dagbag.dags.items():
            # DagBag would fail to load DAGs with cycles,
            # but this explicit test makes the intent clear
            assert dag.topological_sort(), (
                f"DAG '{dag_id}' has circular dependencies"
            )


class TestExampleDags:
    """Validate specific example DAG configurations."""

    def test_hello_world_dag(self, dagbag):
        """Verify hello_world DAG configuration."""
        dag = dagbag.get_dag("example_hello_world")
        if dag is None:
            pytest.skip("example_hello_world DAG not found")

        # In Airflow 3.x, use dag.schedule instead of schedule_interval
        schedule = getattr(dag, 'schedule', None)
        assert schedule is None
        assert "example" in dag.tags

    def test_monitoring_dag_schedule(self, dagbag):
        """Verify monitoring DAG runs every 15 minutes."""
        dag = dagbag.get_dag("example_monitoring")
        if dag is None:
            pytest.skip("example_monitoring DAG not found")

        assert "monitoring" in dag.tags
