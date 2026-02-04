"""Structured logging configuration for the data platform."""

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Log formatter that outputs JSON-structured log lines."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add Airflow context if available
        if hasattr(record, "dag_id"):
            log_entry["dag_id"] = record.dag_id
        if hasattr(record, "task_id"):
            log_entry["task_id"] = record.task_id
        if hasattr(record, "execution_date"):
            log_entry["execution_date"] = record.execution_date

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def setup_logging(level: str = None):
    """Configure structured logging for the application.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
            Defaults to AIRFLOW__LOGGING__LOGGING_LEVEL env var or INFO.
    """
    if level is None:
        level = os.environ.get("AIRFLOW__LOGGING__LOGGING_LEVEL", "INFO")

    log_level = getattr(logging, level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Configure console handler with JSON formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(JSONFormatter())

    # Avoid adding duplicate handlers
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        root_logger.addHandler(console_handler)


def get_logger(name: str, dag_id: str = None, task_id: str = None) -> logging.Logger:
    """Get a logger with optional Airflow context.

    Args:
        name: Logger name (typically __name__).
        dag_id: Optional DAG ID for context.
        task_id: Optional task ID for context.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    if dag_id or task_id:
        adapter_extra = {}
        if dag_id:
            adapter_extra["dag_id"] = dag_id
        if task_id:
            adapter_extra["task_id"] = task_id
        return logging.LoggerAdapter(logger, adapter_extra)

    return logger


@contextmanager
def task_logging_context(dag_id: str, task_id: str, execution_date: str = None):
    """Context manager that adds Airflow task context to all log messages.

    Args:
        dag_id: DAG identifier.
        task_id: Task identifier.
        execution_date: Execution date string.

    Usage:
        with task_logging_context("my_dag", "my_task"):
            logger.info("This message includes dag/task context")
    """
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.dag_id = dag_id
        record.task_id = task_id
        if execution_date:
            record.execution_date = execution_date
        return record

    logging.setLogRecordFactory(record_factory)
    try:
        yield
    finally:
        logging.setLogRecordFactory(old_factory)
