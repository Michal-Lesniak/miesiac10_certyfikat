# Zadanie Zaliczeniowe M10 — Data Quality & Observability

Produkcyjny system DQ dla NexaShop: Great Expectations + pytest + dbt tests + observability dashboard.

## Struktura

```
miesiac10_certyfikat/
├── great_expectations/
│   ├── expectations/            # 3 suity: orders, products, customers
│   ├── checkpoints/             # ecommerce_dq_checkpoint.yml
│   └── plugins/custom_expectations/  # ExpectOrderAmountMatchesItems
├── tests/
│   ├── conftest.py              # SparkSession session fixture
│   └── test_transformations.py  # 18 testów (unit + boundary + integration + empty DF)
├── src/
│   └── transformations.py       # 4 funkcje transformacji Silver
├── dbt_project/
│   ├── models/silver/schema.yml # generic tests + severity + where
│   ├── tests/                   # 2 singular tests
│   ├── macros/                  # custom generic test (positive_or_zero)
│   └── dbt_project.yml
├── observability/
│   ├── orchestrator_extended.py # DataObservabilityOrchestrator + LineageChecker
│   └── dashboard_queries.sql    # 4 widgety
├── airflow_dags/
│   └── dq_pipeline_dag.py       # integracja GE w DAG
├── runbook.md
└── README.md
```

## Uruchomienie

### 1. Setup danych (Databricks)

```sql
-- Uruchom ../miesiac_10_data_quality/sql/month10_setup.sql
%run /path/to/month10_setup.sql
```

### 2. Testy lokalne (PySpark + pytest)

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.* pytest pytest-cov chispa
pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html
```

Oczekiwany wynik: **18 passed**, coverage **≥ 70%** (w praktyce ~95% na `src/transformations.py`).

### 3. Great Expectations

```bash
pip install 'great_expectations>=0.18'
great_expectations init
# skopiuj zawartość great_expectations/* do wygenerowanego katalogu
great_expectations checkpoint run ecommerce_dq_checkpoint
great_expectations docs build
```

### 4. dbt

```bash
cd dbt_project
dbt deps
dbt test --store-failures --select silver_orders silver_products
# wyniki w schemacie ecommerce_catalog.dq_failures.*
```

### 5. Observability + Dashboard

```python
# W notebook'u Databricks
from observability.orchestrator_extended import (
    DataObservabilityOrchestrator, LineageChecker, LineageEdge,
)
orc   = DataObservabilityOrchestrator(spark)
lineage = LineageChecker(spark, edges=[
    LineageEdge("ecommerce_catalog.bronze.orders_raw",
                "ecommerce_catalog.silver.orders", sla_hours=2),
])
orc.run_all(pipeline_run="manual-test", tables={"ecommerce_catalog.silver.orders": 10}, lineage=lineage)
```

Dashboard: zaimportuj `observability/dashboard_queries.sql` jako 4 queries → zbuduj widgety.

## Uwagi

- Jeśli nie masz dostępu do Unity Catalog, zmień `ecommerce_catalog.` na `default.` we wszystkich plikach.
- Custom expectation GE wymaga umieszczenia w `plugins/expectations/` w strukturze GE (nie w suite JSON).
- Dashboard trzeba zbudować ręcznie w Databricks SQL — queries są gotowe.
