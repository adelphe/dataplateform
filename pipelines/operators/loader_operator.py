"""Airflow operator for loading Parquet files from MinIO into PostgreSQL."""

import logging
import sys

from airflow.models import BaseOperator

sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)


class MinIOToPostgresOperator(BaseOperator):
    """Load a Parquet file from MinIO into a PostgreSQL table.

    Downloads a Parquet file from the specified MinIO bucket, reads it
    into a DataFrame, and writes it to the target PostgreSQL table.

    Args:
        bucket: MinIO bucket name (e.g., "raw").
        object_key: Object key path in MinIO.
        target_table: Target PostgreSQL table name.
        target_schema: Target PostgreSQL schema (default: "raw").
        load_mode: How to handle existing data - "replace" or "append".
    """

    template_fields = ("bucket", "object_key", "target_table", "target_schema")

    def __init__(
        self,
        bucket: str,
        object_key: str,
        target_table: str,
        target_schema: str = "raw",
        load_mode: str = "replace",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object_key = object_key
        self.target_table = target_table
        self.target_schema = target_schema
        self.load_mode = load_mode

    def execute(self, context):
        from loaders.minio_to_postgres import load_parquet_to_postgres

        row_count = load_parquet_to_postgres(
            bucket=self.bucket,
            object_key=self.object_key,
            target_table=self.target_table,
            target_schema=self.target_schema,
            load_mode=self.load_mode,
        )

        log.info(
            "Loaded %d rows from s3://%s/%s into %s.%s",
            row_count,
            self.bucket,
            self.object_key,
            self.target_schema,
            self.target_table,
        )

        context["ti"].xcom_push(key="row_count", value=row_count)
        return row_count
