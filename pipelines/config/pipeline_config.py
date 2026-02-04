"""Configuration management for data platform pipelines.

Provides typed configuration classes that load values from environment
variables with support for dev/staging/prod environments.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MinIOConfig:
    """MinIO connection configuration."""

    endpoint_url: str = ""
    access_key: str = ""
    secret_key: str = ""
    region: str = "us-east-1"
    raw_bucket: str = "raw"
    processed_bucket: str = "processed"
    curated_bucket: str = "curated"

    def __post_init__(self):
        self.endpoint_url = self.endpoint_url or os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
        self.access_key = self.access_key or os.environ.get("AWS_ACCESS_KEY_ID", "minio")
        self.secret_key = self.secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""

    host: str = ""
    port: int = 5432
    user: str = ""
    password: str = ""
    database: str = ""

    def __post_init__(self):
        self.host = self.host or os.environ.get("POSTGRES_HOST", "postgres")
        self.port = int(os.environ.get("POSTGRES_PORT", str(self.port)))
        self.user = self.user or os.environ.get("POSTGRES_USER", "airflow")
        self.password = self.password or os.environ.get("POSTGRES_PASSWORD", "airflow")
        self.database = self.database or os.environ.get("POSTGRES_DB", "airflow")

    @property
    def connection_string(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class AirflowConfig:
    """Airflow-specific configuration."""

    parallelism: int = 32
    max_active_runs_per_dag: int = 3
    dag_dir_list_interval: int = 60
    logging_level: str = "INFO"

    def __post_init__(self):
        self.parallelism = int(os.environ.get("AIRFLOW__CORE__PARALLELISM", str(self.parallelism)))
        self.max_active_runs_per_dag = int(
            os.environ.get("AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG", str(self.max_active_runs_per_dag))
        )
        self.dag_dir_list_interval = int(
            os.environ.get("AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL", str(self.dag_dir_list_interval))
        )
        self.logging_level = os.environ.get("AIRFLOW__LOGGING__LOGGING_LEVEL", self.logging_level)


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration combining all sub-configs."""

    environment: str = ""
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)

    def __post_init__(self):
        self.environment = self.environment or os.environ.get("ENVIRONMENT", "development")

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    def validate(self) -> list:
        """Validate required configuration parameters.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = []

        if not self.minio.endpoint_url:
            errors.append("MinIO endpoint URL is required")
        if not self.minio.access_key:
            errors.append("MinIO access key is required")
        if not self.postgres.host:
            errors.append("PostgreSQL host is required")
        if not self.postgres.user:
            errors.append("PostgreSQL user is required")

        return errors


def get_config() -> PipelineConfig:
    """Create and validate the pipeline configuration.

    Returns:
        Validated PipelineConfig instance.

    Raises:
        ValueError: If required configuration is missing.
    """
    config = PipelineConfig()
    errors = config.validate()
    if errors:
        raise ValueError(f"Configuration errors: {'; '.join(errors)}")
    return config
