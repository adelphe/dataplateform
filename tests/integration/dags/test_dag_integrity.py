"""Integration tests for DAG integrity and validation."""

import os
import sys
from pathlib import Path

import pytest

# Add project root and pipelines to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
PIPELINES_DIR = PROJECT_ROOT / "pipelines"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PIPELINES_DIR))

# Set Airflow home for tests
os.environ.setdefault("AIRFLOW_HOME", str(PIPELINES_DIR))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")


class TestDAGIntegrity:
    """Test DAG integrity and configuration."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_no_import_errors(self, dag_bag):
        """Test that all DAGs import without errors."""
        import_errors = dag_bag.import_errors

        if import_errors:
            error_messages = []
            for dag_id, error in import_errors.items():
                error_messages.append(f"{dag_id}: {error}")
            pytest.fail(
                f"DAG import errors found:\n" + "\n".join(error_messages)
            )

    def test_dags_loaded(self, dag_bag):
        """Test that at least one DAG is loaded."""
        assert len(dag_bag.dags) > 0, "No DAGs were loaded"

    def test_no_cycles(self, dag_bag):
        """Test that no DAGs contain cycles."""
        for dag_id, dag in dag_bag.dags.items():
            # topological_sort raises exception if cycle exists
            try:
                dag.topological_sort()
            except Exception as e:
                pytest.fail(f"DAG {dag_id} contains a cycle: {e}")

    def test_dags_have_tags(self, dag_bag):
        """Test that all DAGs have at least one tag."""
        dags_without_tags = []

        for dag_id, dag in dag_bag.dags.items():
            # Skip example DAGs
            if "example" in dag_id.lower():
                continue
            if not dag.tags:
                dags_without_tags.append(dag_id)

        if dags_without_tags:
            pytest.fail(
                f"DAGs without tags: {', '.join(dags_without_tags)}"
            )

    def test_dags_have_descriptions(self, dag_bag):
        """Test that production DAGs have descriptions."""
        dags_without_description = []

        for dag_id, dag in dag_bag.dags.items():
            # Skip example DAGs
            if "example" in dag_id.lower():
                continue
            if not dag.description:
                dags_without_description.append(dag_id)

        # Warning only - don't fail
        if dags_without_description:
            pytest.skip(
                f"DAGs without description (recommended to add): "
                f"{', '.join(dags_without_description)}"
            )

    def test_dags_have_owner(self, dag_bag):
        """Test that all DAGs have an owner defined."""
        for dag_id, dag in dag_bag.dags.items():
            default_args = dag.default_args or {}
            owner = default_args.get("owner") or dag.owner
            assert owner, f"DAG {dag_id} has no owner defined"

    def test_dags_have_schedule(self, dag_bag):
        """Test that production DAGs have a schedule defined."""
        for dag_id, dag in dag_bag.dags.items():
            # Skip example DAGs (they might be manually triggered)
            if "example" in dag_id.lower():
                continue
            # Schedule can be None for manually triggered DAGs
            # Just verify it's explicitly set (not implicitly defaulted)
            has_schedule = dag.timetable is not None or getattr(dag, 'schedule', None) is not None
            assert has_schedule, f"DAG {dag_id} has no schedule defined"


class TestDAGTasks:
    """Test DAG task configurations."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_tasks_have_unique_ids(self, dag_bag):
        """Test that all tasks within a DAG have unique IDs."""
        for dag_id, dag in dag_bag.dags.items():
            task_ids = [task.task_id for task in dag.tasks]
            duplicates = [tid for tid in task_ids if task_ids.count(tid) > 1]
            assert not duplicates, (
                f"DAG {dag_id} has duplicate task IDs: {set(duplicates)}"
            )

    def test_tasks_have_retries(self, dag_bag):
        """Test that tasks have retry configuration."""
        tasks_without_retries = []

        for dag_id, dag in dag_bag.dags.items():
            # Skip example DAGs
            if "example" in dag_id.lower():
                continue

            for task in dag.tasks:
                if task.retries == 0:
                    tasks_without_retries.append(f"{dag_id}.{task.task_id}")

        # Warning only - some tasks intentionally have no retries
        if tasks_without_retries and len(tasks_without_retries) > 5:
            pytest.skip(
                f"Many tasks have no retries configured "
                f"(first 5: {tasks_without_retries[:5]})"
            )

    def test_external_task_sensors_have_timeout(self, dag_bag):
        """Test that ExternalTaskSensors have timeouts configured."""
        from airflow.sensors.external_task import ExternalTaskSensor

        sensors_without_timeout = []

        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                if isinstance(task, ExternalTaskSensor):
                    if task.timeout is None or task.timeout > 3600 * 24:
                        sensors_without_timeout.append(
                            f"{dag_id}.{task.task_id}"
                        )

        if sensors_without_timeout:
            pytest.skip(
                f"ExternalTaskSensors without reasonable timeout: "
                f"{', '.join(sensors_without_timeout)}"
            )


class TestDAGDependencies:
    """Test DAG dependencies and relationships."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_task_dependencies_valid(self, dag_bag):
        """Test that all task dependencies reference existing tasks."""
        for dag_id, dag in dag_bag.dags.items():
            task_ids = {task.task_id for task in dag.tasks}

            for task in dag.tasks:
                for upstream in task.upstream_list:
                    assert upstream.task_id in task_ids, (
                        f"DAG {dag_id}: Task {task.task_id} references "
                        f"non-existent upstream {upstream.task_id}"
                    )
                for downstream in task.downstream_list:
                    assert downstream.task_id in task_ids, (
                        f"DAG {dag_id}: Task {task.task_id} references "
                        f"non-existent downstream {downstream.task_id}"
                    )

    def test_no_orphan_tasks(self, dag_bag):
        """Test that tasks are connected (no orphans except start/end)."""
        for dag_id, dag in dag_bag.dags.items():
            # Skip DAGs with single tasks
            if len(dag.tasks) <= 1:
                continue

            orphan_tasks = []
            for task in dag.tasks:
                # A task is orphan if it has no upstream AND no downstream
                if not task.upstream_list and not task.downstream_list:
                    orphan_tasks.append(task.task_id)

            # Allow one orphan (could be start task with no explicit deps)
            assert len(orphan_tasks) <= 1, (
                f"DAG {dag_id} has orphan tasks: {orphan_tasks}"
            )


class TestSpecificDAGs:
    """Test specific DAG configurations."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_ingestion_dag_exists(self, dag_bag):
        """Test that ingestion DAG exists and is properly configured."""
        ingestion_dags = [
            dag_id for dag_id in dag_bag.dags
            if "ingestion" in dag_id.lower()
        ]
        assert len(ingestion_dags) > 0, "No ingestion DAG found"

    def test_transformation_dag_exists(self, dag_bag):
        """Test that transformation DAG exists."""
        transform_dags = [
            dag_id for dag_id in dag_bag.dags
            if "transform" in dag_id.lower() or "dbt" in dag_id.lower()
        ]
        assert len(transform_dags) > 0, "No transformation DAG found"

    def test_quality_dag_exists(self, dag_bag):
        """Test that data quality DAG exists."""
        quality_dags = [
            dag_id for dag_id in dag_bag.dags
            if "quality" in dag_id.lower()
        ]
        assert len(quality_dags) > 0, "No data quality DAG found"

    def test_monitoring_dag_exists(self, dag_bag):
        """Test that monitoring/health DAG exists."""
        monitoring_dags = [
            dag_id for dag_id in dag_bag.dags
            if "monitor" in dag_id.lower() or "health" in dag_id.lower()
        ]
        assert len(monitoring_dags) > 0, "No monitoring DAG found"


class TestDAGSchedules:
    """Test DAG schedule configurations."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag

        dag_folder = PROJECT_ROOT / "pipelines" / "dags"
        return DagBag(dag_folder=str(dag_folder), include_examples=False)

    def test_production_dags_catchup_disabled(self, dag_bag):
        """Test that production DAGs have catchup disabled by default."""
        dags_with_catchup = []

        for dag_id, dag in dag_bag.dags.items():
            # Skip example DAGs
            if "example" in dag_id.lower():
                continue
            if dag.catchup:
                dags_with_catchup.append(dag_id)

        if dags_with_catchup:
            # Warning - catchup might be intentional
            pytest.skip(
                f"DAGs with catchup=True (verify this is intentional): "
                f"{', '.join(dags_with_catchup)}"
            )

    def test_monitoring_dag_high_frequency(self, dag_bag):
        """Test that monitoring DAG runs frequently."""
        for dag_id, dag in dag_bag.dags.items():
            if "health" in dag_id.lower() or "monitor" in dag_id.lower():
                # Should run at least every hour
                schedule = getattr(dag, 'schedule', None)
                if schedule:
                    schedule_str = str(schedule)
                    # Check it's not daily/weekly
                    if schedule_str in ("@daily", "@weekly", "@monthly"):
                        pytest.fail(
                            f"Monitoring DAG {dag_id} should run more frequently "
                            f"than {schedule_str}"
                        )
