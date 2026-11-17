# Runbook — Data Quality Incidents (NexaShop e-commerce)

**Owner:** data-team@nexashop
**Aktualizacja:** 2026-11-17
**Dashboard:** Databricks SQL → "NexaShop DQ"

---

## 1. Klasyfikacja alertów

| Poziom | Przykład | Reakcja |
|--------|----------|---------|
| **CRITICAL** | freshness_lag > 2× SLA, row_count = 0, duplikaty w PK | 15 min (page) |
| **WARN** | freshness_lag > SLA, null_ratio > 1%, drift z-score > 2 | 2 h |
| **INFO** | wszystkie pozostałe | następny dzień roboczy |

---

## 2. CRITICAL — co sprawdzić w pierwszej kolejności

### 2.1 `freshness_lag_hours > 4` (SLA = 2h)

**Diagnoza (kolejne 5 minut):**

```sql
-- Jaki jest faktyczny lag per upstream?
SELECT metric_name, metric_value, timestamp
FROM ecommerce_catalog.monitoring.dq_metrics
WHERE dimension = 'lineage'
  AND status    = 'critical'
ORDER BY timestamp DESC LIMIT 20;

-- Czy upstream w ogóle ma nowe wiersze?
DESCRIBE DETAIL ecommerce_catalog.bronze.orders_raw;
```

**Przyczyny i fix:**

| Objaw | Fix | Weryfikacja |
|-------|-----|-------------|
| Auto Loader zatrzymany | `spark.streams.get(<queryId>).start()` w notebooku + restart jobu | `spark.streams.active` |
| Checkpoint corruption | `dbfs rm -r <checkpoint>` + replay z `startingTimestamp` | nowe wiersze w bronze |
| Źródło (Kafka/landing) puste | Sprawdź producer (NexaShop webapp team) | ruch w `_metadata.file_path` |

### 2.2 `row_count = 0` w tabeli silver

1. Sprawdź **Jobs UI** — ostatni run zakończony sukcesem?
2. Jeśli ETL odpadł — zrestartuj task z workflow; nie restartuj ręcznie, bo zepsujesz lineage w MLflow.
3. Jeśli ETL przeszedł ale tabela pusta — prawdopodobnie zły filtr; sprawdź `_is_valid` w bronze.

### 2.3 Duplikaty w PK (unikalność złamana)

```sql
SELECT order_id, COUNT(*) c
FROM ecommerce_catalog.silver.orders
GROUP BY order_id HAVING c > 1;
```

- Jeżeli < 10 duplikatów: **hotfix** przez `MERGE` z `row_number` po `updated_at`.
- Jeżeli dużo: cofnij ostatni `INSERT` (`DESCRIBE HISTORY` → `RESTORE TABLE ... TO VERSION AS OF N-1`), potem zbadaj source.

---

## 3. WARN — procedura 2h

### 3.1 `null_ratio > 1%` na kolumnie FK

1. Sprawdź **profilowanie** w dashboardzie (widget 4).
2. Odpal `dbt test --select source:bronze` — jeżeli problem w bronze, dziurę zostawiamy; jeśli w silver, to bug w transform.
3. Otwórz ticket Jira "DQ-xxxx: nulls in <col>" z linkiem do dashboardu.

### 3.2 Drift z-score > 2 na `amount`

- Zweryfikuj, czy to nie **sezon/promocja**: `SELECT AVG(amount) FROM silver.orders WHERE created_at > now() - INTERVAL 24 HOUR`.
- Jeżeli tak — zaktualizuj baseline (patrz skrypt `baseline_refresh.py`).
- Jeżeli nie — alert do BI team: potencjalny błąd w cennikach.

---

## 4. Eskalacja

1. **0-30 min bez postępu** → PagerDuty P2 → on-call data engineer.
2. **30-90 min bez postępu** → P1 → Team Lead Data (Slack #incidents).
3. **>90 min lub problem z Kafka/Delta** → dyrektor + status na status page.

**Osoby:**
- On-call: rotacja w PagerDuty (service "nexashop-data")
- Team Lead: @agata (Slack)
- Platform (DBX): @marek (Slack #platform)

---

## 5. Postmortem (po każdym CRITICAL)

Szablon w Confluence: `data-platform/postmortems/<YYYY-MM-DD>-<slug>`.

**Wymagane sekcje:**
- Timeline (UTC)
- Root cause
- Impact (ilu klientów / ile PLN / jakie raporty zafałszowane)
- Action items (z właścicielem i deadline)

---

## 6. Komendy awaryjne — ściągawka

```bash
# Zatrzymaj wszystkie streaming queries
databricks jobs cancel-all-runs --job-id <id>

# Przywróć silver.orders do wczoraj
spark.sql("RESTORE TABLE ecommerce_catalog.silver.orders TO TIMESTAMP AS OF '2026-11-09 23:59:59'")

# Pokaż ostatnie expectations fails
SELECT * FROM ecommerce_catalog.dq_failures.silver_orders__amount_positive
ORDER BY dbt_run_started_at DESC LIMIT 100;
```
