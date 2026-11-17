-- =============================================================================
-- Databricks SQL Dashboard — DQ & Observability
-- Tabela źródłowa: ecommerce_catalog.monitoring.dq_metrics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- WIDGET 1 — Freshness status (Big Number)
-- Pokazuje aktualny lag w godzinach dla najstarszej upstream table.
-- -----------------------------------------------------------------------------
SELECT
    MAX(metric_value) AS max_freshness_lag_hours
FROM ecommerce_catalog.monitoring.dq_metrics
WHERE dimension   = 'lineage'
  AND metric_name LIKE 'freshness_lag_hours%'
  AND timestamp   >= date_format(current_timestamp() - INTERVAL 1 HOUR, "yyyy-MM-dd'T'HH:mm:ss");

-- -----------------------------------------------------------------------------
-- WIDGET 2 — Quality trend (Line Chart, ostatnie 7 dni)
-- Udział metryk w statusie 'critical' per godzina.
-- -----------------------------------------------------------------------------
SELECT
    date_trunc('hour', to_timestamp(timestamp)) AS hour,
    SUM(CASE WHEN status = 'critical' THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS critical_ratio,
    SUM(CASE WHEN status = 'warn'     THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS warn_ratio,
    COUNT(*)                                    AS total_metrics
FROM ecommerce_catalog.monitoring.dq_metrics
WHERE to_timestamp(timestamp) >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY hour
ORDER BY hour;

-- -----------------------------------------------------------------------------
-- WIDGET 3 — Volume anomalies table
-- Tabele, gdzie row_count spadł poniżej minimum SLA.
-- -----------------------------------------------------------------------------
SELECT
    table_name,
    metric_value AS current_row_count,
    status,
    timestamp
FROM ecommerce_catalog.monitoring.dq_metrics
WHERE dimension   = 'volume'
  AND status     IN ('warn', 'critical')
  AND to_timestamp(timestamp) >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC;

-- -----------------------------------------------------------------------------
-- WIDGET 4 — Distribution drift (z-score bar chart per kolumna)
-- -----------------------------------------------------------------------------
SELECT
    regexp_extract(metric_name, '__(.+)$', 1) AS column_name,
    MAX(metric_value) AS max_drift_zscore,
    MAX(status)       AS worst_status
FROM ecommerce_catalog.monitoring.dq_metrics
WHERE dimension = 'distribution'
  AND to_timestamp(timestamp) >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY column_name
ORDER BY max_drift_zscore DESC;
