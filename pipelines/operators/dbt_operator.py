"""Airflow operators for executing dbt commands.

Provides a base DbtOperator that wraps BashOperator with dbt-specific
features (model selection, vars, full-refresh), plus specialized
subclasses for common dbt workflows:

- DbtRunOperator: Run dbt models with optional select/exclude/full-refresh.
- DbtTestOperator: Execute dbt tests with severity control.
- DbtSeedOperator: Load seed/reference data.
- DbtDocsOperator: Generate dbt documentation.
- DbtSnapshotOperator: Run dbt snapshots for SCD tracking.
"""

import json
import logging
from typing import Dict, Optional, Union

from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/transformations/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/transformations/dbt_project"


class DbtOperator(BashOperator):
    """Execute a dbt command within the Airflow environment.

    Wraps BashOperator to construct and run dbt CLI commands with
    the correct project and profiles directory paths. Supports model
    selection, variable passing, and full-refresh for incremental models.

    Args:
        dbt_command: The dbt sub-command to run (e.g., "run", "test").
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to the directory containing profiles.yml.
        target: dbt target environment (e.g., "dev", "prod").
        select: Models/tags to include (``--select``).
        exclude: Models/tags to exclude (``--exclude``).
        full_refresh: Force full-refresh on incremental models.  Pass
            ``True`` to always full-refresh, or a Jinja expression string
            (e.g. ``"{{ params.full_refresh }}"``) for runtime evaluation.
        dbt_vars: Dictionary of variables passed via ``--vars``.
        warn_error: Treat dbt warnings as errors.
        no_partial_parse: Disable partial parsing.
    """

    template_fields = ("bash_command", "env")

    def __init__(
        self,
        dbt_command: str,
        project_dir: str = DBT_PROJECT_DIR,
        profiles_dir: str = DBT_PROFILES_DIR,
        target: str = "dev",
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: Union[bool, str] = False,
        dbt_vars: Optional[Dict] = None,
        warn_error: bool = False,
        no_partial_parse: bool = False,
        **kwargs,
    ):
        self.dbt_command = dbt_command
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.select = select
        self.exclude = exclude
        self.full_refresh = full_refresh
        self.dbt_vars = dbt_vars
        self.warn_error = warn_error
        self.no_partial_parse = no_partial_parse

        full_command = self._build_command()
        log.info("dbt command: %s", full_command)

        super().__init__(bash_command=full_command, **kwargs)

    def _build_command(self) -> str:
        """Assemble the full dbt CLI string from operator parameters.

        When ``full_refresh`` is a Jinja template string (e.g.
        ``"{{ params.full_refresh }}"``), it is embedded as a Jinja
        conditional so the flag is only appended when the expression
        evaluates to a truthy value at render time.
        """
        parts = ["dbt"]

        if self.warn_error:
            parts.append("--warn-error")
        if self.no_partial_parse:
            parts.append("--no-partial-parse")

        parts.append(self.dbt_command)
        parts.append(f"--project-dir {self.project_dir}")
        parts.append(f"--profiles-dir {self.profiles_dir}")
        parts.append(f"--target {self.target}")

        if self.select:
            parts.append(f"--select {self.select}")
        if self.exclude:
            parts.append(f"--exclude {self.exclude}")

        if self.full_refresh is True:
            parts.append("--full-refresh")
        elif isinstance(self.full_refresh, str) and self.full_refresh:
            # Convert {{ expr }} to a bare expression for use in {% if %}
            expr = self.full_refresh.strip()
            if expr.startswith("{{") and expr.endswith("}}"):
                expr = expr[2:-2].strip()
            parts.append(f"{{% if {expr} %}}--full-refresh{{% endif %}}")

        if self.dbt_vars:
            parts.append(f"--vars '{json.dumps(self.dbt_vars)}'")

        return " ".join(parts)


class DbtRunOperator(DbtOperator):
    """Run dbt models.

    Convenience wrapper that sets ``dbt_command="run"`` and exposes
    model-selection helpers.

    Args:
        select: Models/tags to include (e.g., "staging", "tag:daily").
        exclude: Models/tags to exclude.
        full_refresh: Force full-refresh on incremental models.
        dbt_vars: Dictionary of variables passed via ``--vars``.
    """

    def __init__(
        self,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: Union[bool, str] = False,
        dbt_vars: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            dbt_command="run",
            select=select,
            exclude=exclude,
            full_refresh=full_refresh,
            dbt_vars=dbt_vars,
            **kwargs,
        )


class DbtTestOperator(DbtOperator):
    """Run dbt tests.

    Supports selecting tests by model/tag and optionally treating
    warnings as errors for strict quality gates.

    Args:
        select: Models/tags whose tests to run.
        exclude: Models/tags whose tests to skip.
        warn_error: Treat warnings as errors (strict mode).
    """

    def __init__(
        self,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        warn_error: bool = False,
        **kwargs,
    ):
        super().__init__(
            dbt_command="test",
            select=select,
            exclude=exclude,
            warn_error=warn_error,
            **kwargs,
        )


class DbtSeedOperator(DbtOperator):
    """Load dbt seed (reference) data.

    Args:
        select: Specific seed files to load.
        full_refresh: Force re-load of all seed data.
    """

    def __init__(
        self,
        select: Optional[str] = None,
        full_refresh: Union[bool, str] = False,
        **kwargs,
    ):
        super().__init__(
            dbt_command="seed",
            select=select,
            full_refresh=full_refresh,
            **kwargs,
        )


class DbtDocsOperator(DbtOperator):
    """Generate dbt documentation.

    Runs ``dbt docs generate`` to produce the documentation catalog
    and manifest files.
    """

    def __init__(self, **kwargs):
        super().__init__(dbt_command="docs generate", **kwargs)


class DbtSnapshotOperator(DbtOperator):
    """Run dbt snapshots for slowly-changing dimension tracking.

    Args:
        select: Specific snapshots to run.
    """

    def __init__(self, select: Optional[str] = None, **kwargs):
        super().__init__(dbt_command="snapshot", select=select, **kwargs)
