"""PostgreSQL client utilities for the data platform."""

import os
import logging
from typing import Any, List, Optional
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_engine: Optional[Engine] = None


def get_postgres_connection(connection_string: Optional[str] = None) -> Engine:
    """Create or return a cached SQLAlchemy engine.

    Args:
        connection_string: Optional explicit connection string.
            If not provided, builds from environment variables or
            AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.

    Returns:
        SQLAlchemy Engine instance.
    """
    global _engine
    if _engine is not None and connection_string is None:
        return _engine

    if connection_string is None:
        # Try Airflow's database connection first, then build from env vars
        connection_string = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        if not connection_string:
            user = os.environ.get("POSTGRES_USER", "airflow")
            password = os.environ.get("POSTGRES_PASSWORD", "airflow")
            host = os.environ.get("POSTGRES_HOST", "postgres")
            port = os.environ.get("POSTGRES_PORT", "5432")
            db = os.environ.get("POSTGRES_DB", "airflow")
            connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )

    if connection_string == os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"):
        _engine = engine

    log.info("PostgreSQL engine created")
    return engine


@contextmanager
def _get_connection(engine: Optional[Engine] = None):
    """Context manager for database connections with automatic cleanup."""
    eng = engine or get_postgres_connection()
    conn = eng.connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(sql: str, params: Optional[dict] = None, engine: Optional[Engine] = None) -> Any:
    """Execute a SQL query and return the result.

    Args:
        sql: SQL query string.
        params: Optional query parameters.
        engine: Optional SQLAlchemy engine (uses default if not provided).

    Returns:
        Query result proxy.
    """
    with _get_connection(engine) as conn:
        result = conn.execute(text(sql), params or {})
        log.info("Executed query: %s", sql[:100])
        return result


def fetch_dataframe(sql: str, params: Optional[dict] = None, engine: Optional[Engine] = None) -> pd.DataFrame:
    """Execute a SQL query and return results as a pandas DataFrame.

    Args:
        sql: SQL query string.
        params: Optional query parameters.
        engine: Optional SQLAlchemy engine.

    Returns:
        pandas DataFrame with query results.
    """
    eng = engine or get_postgres_connection()
    df = pd.read_sql(text(sql), eng, params=params or {})
    log.info("Fetched DataFrame with %d rows from query: %s", len(df), sql[:100])
    return df


def bulk_insert(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: str = "append",
    engine: Optional[Engine] = None,
) -> int:
    """Bulk insert a pandas DataFrame into a PostgreSQL table.

    Args:
        df: DataFrame to insert.
        table_name: Target table name.
        schema: Target schema.
        if_exists: Behavior when table exists ('append', 'replace', 'fail').
        engine: Optional SQLAlchemy engine.

    Returns:
        Number of rows inserted.
    """
    eng = engine or get_postgres_connection()
    df.to_sql(
        name=table_name,
        con=eng,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000,
    )
    log.info("Inserted %d rows into %s.%s", len(df), schema, table_name)
    return len(df)
