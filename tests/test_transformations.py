"""Testy jednostkowe dla `src.transformations`. 18 testów łącznie."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row

from src.transformations import (
    VALID_STATUSES,
    compute_daily_revenue,
    dedup_orders,
    enrich_with_customer_segment,
    filter_valid_orders,
)


# ---------------------------------------------------------------------------
# dedup_orders
# ---------------------------------------------------------------------------

def test_dedup_orders_keeps_latest(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, updated_at=datetime(2024, 1, 1, 10, 0)),
        Row(order_id="O1", amount=120.0, updated_at=datetime(2024, 1, 1, 11, 0)),
        Row(order_id="O2", amount=50.0,  updated_at=datetime(2024, 1, 1, 9, 0)),
    ])
    out = dedup_orders(df).orderBy("order_id").collect()
    assert len(out) == 2
    o1 = next(r for r in out if r.order_id == "O1")
    assert o1.amount == 120.0


def test_dedup_orders_handles_null_updated_at(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, updated_at=None),
        Row(order_id="O1", amount=120.0, updated_at=datetime(2024, 1, 1, 11, 0)),
    ])
    out = dedup_orders(df).collect()
    assert len(out) == 1
    assert out[0].amount == 120.0


def test_dedup_orders_raises_without_updated_at(spark):
    df = spark.createDataFrame([Row(order_id="O1", amount=100.0)])
    with pytest.raises(ValueError, match="updated_at"):
        dedup_orders(df)


def test_dedup_orders_empty_df(spark):
    schema = "order_id STRING, amount DOUBLE, updated_at TIMESTAMP"
    df = spark.createDataFrame([], schema=schema)
    assert dedup_orders(df).count() == 0


# ---------------------------------------------------------------------------
# filter_valid_orders
# ---------------------------------------------------------------------------

def test_filter_valid_orders_drops_negative_amount(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0,  status="paid",    customer_id="C1"),
        Row(order_id="O2", amount=-50.0,  status="paid",    customer_id="C2"),
        Row(order_id="O3", amount=0.0,    status="paid",    customer_id="C3"),
    ])
    ids = {r.order_id for r in filter_valid_orders(df).collect()}
    assert ids == {"O1"}


def test_filter_valid_orders_drops_invalid_status(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, status="paid",    customer_id="C1"),
        Row(order_id="O2", amount=100.0, status="UNKNOWN", customer_id="C2"),
        Row(order_id="O3", amount=100.0, status="PAID",    customer_id="C3"),  # case-sensitive
    ])
    ids = {r.order_id for r in filter_valid_orders(df).collect()}
    assert ids == {"O1"}


def test_filter_valid_orders_drops_null_customer(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, status="paid", customer_id="C1"),
        Row(order_id="O2", amount=100.0, status="paid", customer_id=None),
    ])
    ids = {r.order_id for r in filter_valid_orders(df).collect()}
    assert ids == {"O1"}


@pytest.mark.parametrize("status", sorted(VALID_STATUSES))
def test_filter_valid_orders_accepts_all_valid_statuses(spark, status):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, status=status, customer_id="C1"),
    ])
    assert filter_valid_orders(df).count() == 1


@pytest.mark.parametrize("amount,expected", [
    (0.0,   0),
    (0.01,  1),
    (1.0,   1),
    (99999, 1),
])
def test_filter_valid_orders_boundary_amount(spark, amount, expected):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=amount, status="paid", customer_id="C1"),
    ])
    assert filter_valid_orders(df).count() == expected


def test_filter_valid_orders_missing_column_raises(spark):
    df = spark.createDataFrame([Row(order_id="O1", amount=1.0)])
    with pytest.raises(ValueError, match="Brakujące kolumny"):
        filter_valid_orders(df)


def test_filter_valid_orders_empty_df(spark):
    schema = "order_id STRING, amount DOUBLE, status STRING, customer_id STRING"
    df = spark.createDataFrame([], schema=schema)
    assert filter_valid_orders(df).count() == 0


# ---------------------------------------------------------------------------
# compute_daily_revenue
# ---------------------------------------------------------------------------

def test_compute_daily_revenue_counts_only_paying(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, status="paid",      customer_id="C1",
            created_at=datetime(2024, 1, 1, 10, 0)),
        Row(order_id="O2", amount=200.0, status="pending",   customer_id="C2",
            created_at=datetime(2024, 1, 1, 11, 0)),
        Row(order_id="O3", amount=50.0,  status="delivered", customer_id="C1",
            created_at=datetime(2024, 1, 1, 12, 0)),
    ])
    rows = compute_daily_revenue(df).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.day == date(2024, 1, 1)
    assert r.revenue           == 150.0
    assert r.orders_count      == 2
    assert r.unique_customers  == 1
    assert r.avg_order_value   == 75.0


def test_compute_daily_revenue_multiple_days(spark):
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0, status="paid", customer_id="C1",
            created_at=datetime(2024, 1, 1, 10, 0)),
        Row(order_id="O2", amount=200.0, status="paid", customer_id="C2",
            created_at=datetime(2024, 1, 2, 11, 0)),
    ])
    rows = compute_daily_revenue(df).collect()
    assert len(rows) == 2
    assert rows[0].day == date(2024, 1, 1)
    assert rows[1].day == date(2024, 1, 2)


def test_compute_daily_revenue_empty_df(spark):
    schema = "order_id STRING, amount DOUBLE, status STRING, customer_id STRING, created_at TIMESTAMP"
    df = spark.createDataFrame([], schema=schema)
    assert compute_daily_revenue(df).count() == 0


# ---------------------------------------------------------------------------
# enrich_with_customer_segment
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ltv,expected", [
    (0,       "NEW"),
    (1,       "REGULAR"),
    (999,     "REGULAR"),
    (1000,    "PREMIUM"),
    (9999,    "PREMIUM"),
    (10000,   "VIP"),
    (99999,   "VIP"),
])
def test_enrich_segment_boundaries(spark, ltv, expected):
    orders = spark.createDataFrame([Row(order_id="O1", customer_id="C1", amount=100.0)])
    customers = spark.createDataFrame([Row(customer_id="C1", lifetime_value=float(ltv))])
    out = enrich_with_customer_segment(orders, customers).collect()
    assert out[0].customer_segment == expected


def test_enrich_segment_missing_customer_null_ltv(spark):
    orders = spark.createDataFrame([Row(order_id="O1", customer_id="C_unknown", amount=100.0)])
    customers = spark.createDataFrame([Row(customer_id="C1", lifetime_value=1000.0)])
    out = enrich_with_customer_segment(orders, customers).collect()
    assert out[0].customer_segment == "NEW"


def test_enrich_segment_raises_without_customer_id(spark):
    orders = spark.createDataFrame([Row(order_id="O1", amount=100.0)])
    customers = spark.createDataFrame([Row(customer_id="C1", lifetime_value=1000.0)])
    with pytest.raises(ValueError, match="customer_id"):
        enrich_with_customer_segment(orders, customers)


# ---------------------------------------------------------------------------
# Integration test — 2 transformacje w sekwencji
# ---------------------------------------------------------------------------

def test_integration_dedup_then_filter_then_revenue(spark):
    """Pipeline: dedup → filter → daily revenue — end-to-end."""
    df = spark.createDataFrame([
        Row(order_id="O1", amount=100.0,  status="paid", customer_id="C1",
            created_at=datetime(2024, 1, 1, 10, 0), updated_at=datetime(2024, 1, 1, 10, 0)),
        Row(order_id="O1", amount=120.0,  status="paid", customer_id="C1",
            created_at=datetime(2024, 1, 1, 10, 0), updated_at=datetime(2024, 1, 1, 11, 0)),
        Row(order_id="O2", amount=-50.0,  status="paid", customer_id="C2",
            created_at=datetime(2024, 1, 1, 10, 0), updated_at=datetime(2024, 1, 1, 10, 0)),
        Row(order_id="O3", amount=200.0,  status="UNKNOWN", customer_id="C3",
            created_at=datetime(2024, 1, 1, 10, 0), updated_at=datetime(2024, 1, 1, 10, 0)),
    ])
    pipeline = compute_daily_revenue(filter_valid_orders(dedup_orders(df)))
    rows = pipeline.collect()
    assert len(rows) == 1
    assert rows[0].revenue      == 120.0
    assert rows[0].orders_count == 1
