"""Custom expectation: amount zamówienia zgadza się z sumą pozycji ±0.01 PLN.

Zakłada denormalizowaną kolumnę `items_total` (suma z `order_items`).
"""
from __future__ import annotations

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
)
from great_expectations.expectations.metrics import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


class ColumnPairValuesAmountMatchesItems(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.amount_matches_items"
    condition_domain_keys = ("batch_id", "table", "column_A", "column_B")
    condition_value_keys = ("tolerance",)

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, **kwargs):
        tolerance = kwargs.get("tolerance", 0.01)
        return abs(column_A - column_B) <= tolerance


class ExpectOrderAmountMatchesItems(ColumnPairMapExpectation):
    """Expect amount ≈ items_total (±tolerance)."""

    map_metric = "column_pair_values.amount_matches_items"
    success_keys = ("tolerance", "mostly")

    default_kwarg_values = {
        "tolerance": 0.01,
        "mostly": 0.99,
    }

    examples = [
        {
            "data": {"amount": [100.0, 50.0, 25.0], "items_total": [100.0, 50.0, 25.0]},
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {"column_A": "amount", "column_B": "items_total", "tolerance": 0.01},
                    "out": {"success": True},
                }
            ],
        }
    ]
