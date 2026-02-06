"""Custom sensor operator for checking PostgreSQL table readiness."""

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator

log = logging.getLogger(__name__)


class PostgresTableSensorOperator(BaseSensorOperator):
    """Sensor that waits for a PostgreSQL table to exist and optionally have rows.

    Args:
        table_name: Name of the table to check.
        postgres_conn_id: Airflow connection ID for PostgreSQL.
        schema: Database schema containing the table.
        min_row_count: Minimum number of rows required (0 means just check existence).
    """

    template_fields = ("table_name", "schema")

    def __init__(
        self,
        table_name: str,
        postgres_conn_id: str = "postgres_default",
        schema: str = "public",
        min_row_count: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.min_row_count = min_row_count

    def poke(self, context):
        """Check if the table exists and meets the minimum row count."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Check table existence
        existence_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """
        result = hook.get_first(existence_sql, parameters=(self.schema, self.table_name))

        if not result or not result[0]:
            log.info("Table %s.%s does not exist yet", self.schema, self.table_name)
            return False

        log.info("Table %s.%s exists", self.schema, self.table_name)

        if self.min_row_count > 0:
            count_sql = f'SELECT COUNT(*) FROM "{self.schema}"."{self.table_name}";'
            count_result = hook.get_first(count_sql)
            row_count = count_result[0] if count_result else 0

            if row_count < self.min_row_count:
                log.info(
                    "Table %s.%s has %d rows, waiting for at least %d",
                    self.schema,
                    self.table_name,
                    row_count,
                    self.min_row_count,
                )
                return False

            log.info(
                "Table %s.%s has %d rows (>= %d required)",
                self.schema,
                self.table_name,
                row_count,
                self.min_row_count,
            )

        return True
