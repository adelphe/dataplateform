"""Airflow operator for executing dbt commands."""

import logging

from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/transformations/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/transformations/dbt_project"


class DbtOperator(BashOperator):
    """Execute a dbt command within the Airflow environment.

    Wraps BashOperator to construct and run dbt CLI commands with
    the correct project and profiles directory paths.

    Args:
        dbt_command: The dbt command to run (e.g., "run --models staging.*").
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to the directory containing profiles.yml.
        target: dbt target environment (e.g., "dev", "prod").
    """

    template_fields = ("bash_command", "env")

    def __init__(
        self,
        dbt_command: str,
        project_dir: str = DBT_PROJECT_DIR,
        profiles_dir: str = DBT_PROFILES_DIR,
        target: str = "dev",
        **kwargs,
    ):
        self.dbt_command = dbt_command
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target

        full_command = (
            f"dbt {dbt_command} "
            f"--project-dir {project_dir} "
            f"--profiles-dir {profiles_dir} "
            f"--target {target}"
        )

        super().__init__(bash_command=full_command, **kwargs)
