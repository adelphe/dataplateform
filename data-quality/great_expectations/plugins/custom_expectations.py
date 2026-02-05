"""
Custom Great Expectations for Data Platform validation.

These expectations extend GE's built-in functionality with domain-specific
validations for the data platform.
"""

from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ExpectColumnValuesToBeValidCurrencyCode(ColumnMapExpectation):
    """
    Expect column values to be valid ISO 4217 currency codes.

    This expectation validates that currency codes are from the allowed set
    used by the data platform (USD, EUR, GBP, etc.).
    """

    # Allowed currency codes for the platform
    VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"}

    # Expectation metadata
    expectation_type = "expect_column_values_to_be_valid_currency_code"

    # Default keyword arguments
    default_kwarg_values = {
        "mostly": 1.0,
        "allowed_currencies": None,
    }

    # Map metric name
    map_metric = "column_values.valid_currency_code"

    # Success keys define which kwargs are used in validation
    success_keys = ("mostly", "allowed_currencies")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)

    @classmethod
    def _prescriptive_renderer(cls, configuration=None, result=None, **kwargs):
        return f"Values must be valid ISO 4217 currency codes"


class ValidCurrencyCodeColumnMapMetricProvider(ColumnMapMetricProvider):
    """Metric provider for valid currency code validation."""

    condition_metric_name = "column_values.valid_currency_code"
    condition_value_keys = ("allowed_currencies",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, allowed_currencies=None, **kwargs):
        valid_codes = set(allowed_currencies) if allowed_currencies else ExpectColumnValuesToBeValidCurrencyCode.VALID_CURRENCIES
        return column.str.upper().isin(valid_codes)


class ExpectColumnValuesToBePositiveAmount(ColumnMapExpectation):
    """
    Expect column values to be positive numeric amounts.

    This is specifically designed for financial amount validation,
    ensuring values are greater than zero.
    """

    expectation_type = "expect_column_values_to_be_positive_amount"

    default_kwarg_values = {
        "mostly": 1.0,
        "strictly_positive": True,
    }

    map_metric = "column_values.positive_amount"
    success_keys = ("mostly", "strictly_positive")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)

    @classmethod
    def _prescriptive_renderer(cls, configuration=None, result=None, **kwargs):
        return "Values must be positive numeric amounts"


class PositiveAmountColumnMapMetricProvider(ColumnMapMetricProvider):
    """Metric provider for positive amount validation."""

    condition_metric_name = "column_values.positive_amount"
    condition_value_keys = ("strictly_positive",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, strictly_positive=True, **kwargs):
        import pandas as pd

        numeric_column = pd.to_numeric(column, errors="coerce")
        if strictly_positive:
            return numeric_column > 0
        return numeric_column >= 0


class ExpectColumnTimestampToBeRecent(ColumnMapExpectation):
    """
    Expect timestamp column values to be within a recent time window.

    This validates data freshness by ensuring timestamps are not older
    than the specified number of days.
    """

    expectation_type = "expect_column_timestamp_to_be_recent"

    default_kwarg_values = {
        "mostly": 1.0,
        "max_age_days": 7,
    }

    map_metric = "column_values.timestamp_recent"
    success_keys = ("mostly", "max_age_days")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        if configuration:
            max_age = configuration.kwargs.get("max_age_days", 7)
            if not isinstance(max_age, (int, float)) or max_age <= 0:
                raise ValueError("max_age_days must be a positive number")

    @classmethod
    def _prescriptive_renderer(cls, configuration=None, result=None, **kwargs):
        max_age = configuration.kwargs.get("max_age_days", 7) if configuration else 7
        return f"Timestamp values must be within the last {max_age} days"


class TimestampRecentColumnMapMetricProvider(ColumnMapMetricProvider):
    """Metric provider for recent timestamp validation."""

    condition_metric_name = "column_values.timestamp_recent"
    condition_value_keys = ("max_age_days",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, max_age_days=7, **kwargs):
        import pandas as pd
        from datetime import datetime, timedelta

        cutoff = datetime.now() - timedelta(days=max_age_days)
        timestamp_column = pd.to_datetime(column, errors="coerce")
        return timestamp_column >= cutoff
