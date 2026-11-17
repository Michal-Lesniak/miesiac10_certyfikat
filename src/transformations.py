"""Transformacje e-commerce (Silver layer) używane przez testy i pipeline."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_STATUSES = {"pending", "paid", "shipped", "delivered", "cancelled", "refunded"}


def dedup_orders(df: DataFrame) -> DataFrame:
    """Deduplikuje zamówienia po `order_id` — zostawia najświeższy `updated_at`."""
    if "updated_at" not in df.columns:
        raise ValueError("DataFrame nie zawiera kolumny `updated_at`")

    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
    return (
        df.withColumn("_rk", F.row_number().over(w))
          .filter(F.col("_rk") == 1)
          .drop("_rk")
    )


def filter_valid_orders(df: DataFrame) -> DataFrame:
    """Odfiltrowuje niepoprawne rekordy: amount > 0, status ze zbioru, customer_id != NULL."""
    required = {"amount", "status", "customer_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brakujące kolumny: {missing}")

    return df.filter(
        (F.col("amount") > 0)
        & F.col("status").isin(*VALID_STATUSES)
        & F.col("customer_id").isNotNull()
    )


def compute_daily_revenue(df: DataFrame) -> DataFrame:
    """Dzienna agregacja przychodu — liczy tylko paid+shipped+delivered."""
    paying = df.filter(F.col("status").isin("paid", "shipped", "delivered"))
    return (
        paying
        .withColumn("day", F.to_date("created_at"))
        .groupBy("day")
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("order_id").alias("orders_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("amount").alias("avg_order_value"),
        )
        .orderBy("day")
    )


def enrich_with_customer_segment(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Join orders z customers; wylicza `customer_segment` wg `lifetime_value`."""
    if "customer_id" not in orders.columns or "customer_id" not in customers.columns:
        raise ValueError("Join wymaga `customer_id` po obu stronach")

    joined = orders.join(customers, on="customer_id", how="left")
    return joined.withColumn(
        "customer_segment",
        F.when(F.col("lifetime_value") >= 10000, F.lit("VIP"))
         .when(F.col("lifetime_value") >= 1000,  F.lit("PREMIUM"))
         .when(F.col("lifetime_value") >  0,     F.lit("REGULAR"))
         .otherwise(F.lit("NEW")),
    )
