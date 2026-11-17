"""Rozszerzony DataObservabilityOrchestrator z filarem Lineage.

Sprawdza freshness tabel upstream (bronze) + zapisuje metryki DQ do Delta.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Iterable, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("month10.observability")


# ---------------------------------------------------------------------------
# Metric data class
# ---------------------------------------------------------------------------

@dataclass
class PipelineMetric:
    metric_name:        str
    metric_value:       float
    dimension:          str           # freshness / volume / schema / distribution / lineage
    table_name:         str
    pipeline_run:       str
    timestamp:          str   = field(default_factory=lambda: datetime.utcnow().isoformat())
    status:             str   = "ok"
    threshold_warn:     Optional[float] = None
    threshold_critical: Optional[float] = None

    def evaluate(self) -> "PipelineMetric":
        if self.threshold_critical is not None and self.metric_value >= self.threshold_critical:
            self.status = "critical"
        elif self.threshold_warn is not None and self.metric_value >= self.threshold_warn:
            self.status = "warn"
        else:
            self.status = "ok"
        return self

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Lineage checker — nowy filar
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LineageEdge:
    upstream:   str            # ecommerce_catalog.bronze.orders_raw
    downstream: str            # ecommerce_catalog.silver.orders
    sla_hours:  float = 2.0


class LineageChecker:
    """Sprawdza freshness tabel upstream względem downstream.

    Metryka: `lineage_freshness_lag_hours` — ile godzin ma najstarsza partycja
    upstream vs. SLA. Status: ok/warn/critical.
    """

    def __init__(self, spark: SparkSession, edges: Iterable[LineageEdge]) -> None:
        self.spark = spark
        self.edges = list(edges)

    def _max_ts(self, table: str) -> Optional[datetime]:
        try:
            row = (
                self.spark.sql(f"DESCRIBE DETAIL {table}")
                .select("lastModified")
                .first()
            )
            if row and row["lastModified"]:
                return row["lastModified"]
        except Exception as e:
            log.warning(f"freshness probe failed for {table}: {e}")
        return None

    def check(self, pipeline_run: str) -> list[PipelineMetric]:
        now = datetime.utcnow()
        metrics: list[PipelineMetric] = []
        for edge in self.edges:
            up_ts = self._max_ts(edge.upstream)
            if up_ts is None:
                lag_hours = 999.0
            else:
                lag_hours = (now - up_ts.replace(tzinfo=None)).total_seconds() / 3600
            metric = PipelineMetric(
                metric_name=f"freshness_lag_hours__{edge.upstream}",
                metric_value=float(lag_hours),
                dimension="lineage",
                table_name=edge.downstream,
                pipeline_run=pipeline_run,
                threshold_warn=edge.sla_hours,
                threshold_critical=edge.sla_hours * 2,
            ).evaluate()
            metrics.append(metric)
        return metrics


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class DataObservabilityOrchestrator:
    """Główny silnik — zbiera metryki DQ i zapisuje je do `monitoring.dq_metrics`."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "ecommerce_catalog",
        schema_monitoring: str = "monitoring",
        table: str = "dq_metrics",
    ) -> None:
        self.spark   = spark
        self.catalog = catalog
        self.schema  = schema_monitoring
        self.table   = f"{catalog}.{schema_monitoring}.{table}"
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                metric_name         STRING,
                metric_value        DOUBLE,
                dimension           STRING,
                table_name          STRING,
                pipeline_run        STRING,
                timestamp           STRING,
                status              STRING,
                threshold_warn      DOUBLE,
                threshold_critical  DOUBLE
            ) USING DELTA
            PARTITIONED BY (dimension)
        """)

    # ---- individual checkers --------------------------------------------

    def check_volume(self, table: str, min_rows: int, pipeline_run: str) -> PipelineMetric:
        count = self.spark.table(table).count()
        return PipelineMetric(
            metric_name="row_count",
            metric_value=float(count),
            dimension="volume",
            table_name=table,
            pipeline_run=pipeline_run,
            # odwracamy logikę — poniżej min_rows jest warn/critical,
            # dlatego liczymy "brakujące wiersze" jako sygnał
            threshold_warn=0,
            threshold_critical=0,
        ).evaluate() if count >= min_rows else PipelineMetric(
            metric_name="row_count",
            metric_value=float(count),
            dimension="volume",
            table_name=table,
            pipeline_run=pipeline_run,
            status="critical",
        )

    def check_null_ratio(self, table: str, column: str, pipeline_run: str) -> PipelineMetric:
        total = self.spark.table(table).count() or 1
        nulls = self.spark.table(table).filter(F.col(column).isNull()).count()
        ratio = nulls / total
        return PipelineMetric(
            metric_name=f"null_ratio__{column}",
            metric_value=float(ratio),
            dimension="schema",
            table_name=table,
            pipeline_run=pipeline_run,
            threshold_warn=0.01,
            threshold_critical=0.05,
        ).evaluate()

    def check_distribution_drift(
        self,
        table: str,
        column: str,
        baseline_mean: float,
        baseline_std:  float,
        pipeline_run:  str,
    ) -> PipelineMetric:
        """Prosty z-score dla średniej aktualnej vs. baseline."""
        row = self.spark.table(table).select(F.mean(column).alias("m")).first()
        current = float(row["m"] or 0.0)
        drift = abs(current - baseline_mean) / (baseline_std or 1.0)
        return PipelineMetric(
            metric_name=f"distribution_drift__{column}",
            metric_value=float(drift),
            dimension="distribution",
            table_name=table,
            pipeline_run=pipeline_run,
            threshold_warn=2.0,
            threshold_critical=3.0,
        ).evaluate()

    # ---- persistence ----------------------------------------------------

    def persist(self, metrics: Iterable[PipelineMetric]) -> int:
        rows = [m.to_dict() for m in metrics]
        if not rows:
            return 0
        df = self.spark.createDataFrame(rows)
        df.write.format("delta").mode("append").saveAsTable(self.table)
        return df.count()

    # ---- end-to-end -----------------------------------------------------

    def run_all(
        self,
        pipeline_run: str,
        tables: dict[str, int],
        lineage: Optional[LineageChecker] = None,
    ) -> list[PipelineMetric]:
        results: list[PipelineMetric] = []
        for table, min_rows in tables.items():
            results.append(self.check_volume(table, min_rows, pipeline_run))
            results.append(self.check_null_ratio(table, "customer_id", pipeline_run))
        if lineage is not None:
            results.extend(lineage.check(pipeline_run))
        self.persist(results)
        return results
