"""Unit tests for dbt operators."""

import pytest
from unittest.mock import MagicMock, patch

from pipelines.operators.dbt_operator import (
    DbtOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtSeedOperator,
    DbtDocsOperator,
    DbtSnapshotOperator,
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
)


class TestDbtOperator:
    """Test cases for base DbtOperator."""

    def test_basic_command_construction(self):
        """Test basic dbt command is constructed correctly."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
        )

        assert "dbt" in operator.bash_command
        assert "run" in operator.bash_command
        assert f"--project-dir {DBT_PROJECT_DIR}" in operator.bash_command
        assert f"--profiles-dir {DBT_PROFILES_DIR}" in operator.bash_command
        assert "--target dev" in operator.bash_command

    def test_command_with_select(self):
        """Test dbt command includes select flag."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            select="staging",
        )

        assert "--select staging" in operator.bash_command

    def test_command_with_exclude(self):
        """Test dbt command includes exclude flag."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            exclude="test_model",
        )

        assert "--exclude test_model" in operator.bash_command

    def test_command_with_full_refresh_boolean(self):
        """Test dbt command includes full-refresh when True."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            full_refresh=True,
        )

        assert "--full-refresh" in operator.bash_command

    def test_command_with_full_refresh_jinja(self):
        """Test dbt command handles Jinja template for full_refresh."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            full_refresh="{{ params.full_refresh }}",
        )

        assert "{% if params.full_refresh %}--full-refresh{% endif %}" in operator.bash_command

    def test_command_with_vars(self):
        """Test dbt command includes vars."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            dbt_vars={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        )

        assert "--vars" in operator.bash_command
        assert "start_date" in operator.bash_command
        assert "end_date" in operator.bash_command

    def test_command_with_warn_error(self):
        """Test dbt command includes warn-error flag."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            warn_error=True,
        )

        assert "--warn-error" in operator.bash_command

    def test_command_with_no_partial_parse(self):
        """Test dbt command includes no-partial-parse flag."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            no_partial_parse=True,
        )

        assert "--no-partial-parse" in operator.bash_command

    def test_custom_project_and_profiles_dir(self):
        """Test dbt command uses custom project and profiles directories."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            project_dir="/custom/project",
            profiles_dir="/custom/profiles",
        )

        assert "--project-dir /custom/project" in operator.bash_command
        assert "--profiles-dir /custom/profiles" in operator.bash_command

    def test_custom_target(self):
        """Test dbt command uses custom target."""
        operator = DbtOperator(
            task_id="test_dbt",
            dbt_command="run",
            target="prod",
        )

        assert "--target prod" in operator.bash_command


class TestDbtRunOperator:
    """Test cases for DbtRunOperator."""

    def test_run_command(self):
        """Test DbtRunOperator creates run command."""
        operator = DbtRunOperator(task_id="test_dbt_run")

        assert "dbt" in operator.bash_command
        assert "run" in operator.bash_command

    def test_run_with_select(self):
        """Test DbtRunOperator with model selection."""
        operator = DbtRunOperator(
            task_id="test_dbt_run",
            select="staging.stg_customers",
        )

        assert "--select staging.stg_customers" in operator.bash_command

    def test_run_with_full_refresh(self):
        """Test DbtRunOperator with full refresh."""
        operator = DbtRunOperator(
            task_id="test_dbt_run",
            full_refresh=True,
        )

        assert "--full-refresh" in operator.bash_command

    def test_run_with_vars(self):
        """Test DbtRunOperator with variables."""
        operator = DbtRunOperator(
            task_id="test_dbt_run",
            dbt_vars={"execution_date": "{{ ds }}"},
        )

        assert "--vars" in operator.bash_command


class TestDbtTestOperator:
    """Test cases for DbtTestOperator."""

    def test_test_command(self):
        """Test DbtTestOperator creates test command."""
        operator = DbtTestOperator(task_id="test_dbt_test")

        assert "dbt" in operator.bash_command
        assert "test" in operator.bash_command

    def test_test_with_select(self):
        """Test DbtTestOperator with model selection."""
        operator = DbtTestOperator(
            task_id="test_dbt_test",
            select="marts",
        )

        assert "--select marts" in operator.bash_command

    def test_test_with_warn_error(self):
        """Test DbtTestOperator with warn-error for strict mode."""
        operator = DbtTestOperator(
            task_id="test_dbt_test",
            warn_error=True,
        )

        assert "--warn-error" in operator.bash_command


class TestDbtSeedOperator:
    """Test cases for DbtSeedOperator."""

    def test_seed_command(self):
        """Test DbtSeedOperator creates seed command."""
        operator = DbtSeedOperator(task_id="test_dbt_seed")

        assert "dbt" in operator.bash_command
        assert "seed" in operator.bash_command

    def test_seed_with_select(self):
        """Test DbtSeedOperator with specific seeds."""
        operator = DbtSeedOperator(
            task_id="test_dbt_seed",
            select="country_codes",
        )

        assert "--select country_codes" in operator.bash_command

    def test_seed_with_full_refresh(self):
        """Test DbtSeedOperator with full refresh."""
        operator = DbtSeedOperator(
            task_id="test_dbt_seed",
            full_refresh=True,
        )

        assert "--full-refresh" in operator.bash_command


class TestDbtDocsOperator:
    """Test cases for DbtDocsOperator."""

    def test_docs_generate_command(self):
        """Test DbtDocsOperator creates docs generate command."""
        operator = DbtDocsOperator(task_id="test_dbt_docs")

        assert "dbt" in operator.bash_command
        assert "docs generate" in operator.bash_command


class TestDbtSnapshotOperator:
    """Test cases for DbtSnapshotOperator."""

    def test_snapshot_command(self):
        """Test DbtSnapshotOperator creates snapshot command."""
        operator = DbtSnapshotOperator(task_id="test_dbt_snapshot")

        assert "dbt" in operator.bash_command
        assert "snapshot" in operator.bash_command

    def test_snapshot_with_select(self):
        """Test DbtSnapshotOperator with specific snapshots."""
        operator = DbtSnapshotOperator(
            task_id="test_dbt_snapshot",
            select="customer_snapshot",
        )

        assert "--select customer_snapshot" in operator.bash_command


class TestDbtOperatorIntegration:
    """Integration-style tests for dbt operators."""

    def test_operator_inheritance(self):
        """Test all dbt operators inherit from DbtOperator."""
        assert issubclass(DbtRunOperator, DbtOperator)
        assert issubclass(DbtTestOperator, DbtOperator)
        assert issubclass(DbtSeedOperator, DbtOperator)
        assert issubclass(DbtDocsOperator, DbtOperator)
        assert issubclass(DbtSnapshotOperator, DbtOperator)

    def test_full_pipeline_simulation(self):
        """Test simulated dbt pipeline command generation."""
        # Seed
        seed_op = DbtSeedOperator(task_id="seed", full_refresh=True)
        assert "seed" in seed_op.bash_command
        assert "--full-refresh" in seed_op.bash_command

        # Run staging
        run_staging = DbtRunOperator(task_id="run_staging", select="staging")
        assert "run" in run_staging.bash_command
        assert "--select staging" in run_staging.bash_command

        # Test staging
        test_staging = DbtTestOperator(task_id="test_staging", select="staging")
        assert "test" in test_staging.bash_command

        # Run marts
        run_marts = DbtRunOperator(
            task_id="run_marts",
            select="marts",
            dbt_vars={"execution_date": "2024-01-01"},
        )
        assert "--select marts" in run_marts.bash_command
        assert "--vars" in run_marts.bash_command

        # Generate docs
        docs_op = DbtDocsOperator(task_id="generate_docs")
        assert "docs generate" in docs_op.bash_command
