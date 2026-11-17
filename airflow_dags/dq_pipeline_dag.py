"""Airflow DAG — codzienny data quality run.

Zadania:
1. `ge_validate_silver` — Great Expectations checkpoint na 3 suitach.
2. `observability_run` — zbiera metryki + lineage + zapisuje do Delta.
3. `dbt_test` — generic + singular tests + custom macro.
4. `publish_docs` — wrzuca Data Docs na statyczny storage.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email":            ["data-team@nexashop.example.com"],
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


def run_ge_checkpoint(**_):
    import great_expectations as ge

    context = ge.get_context()
    result  = context.run_checkpoint(checkpoint_name="ecommerce_dq_checkpoint")
    if not result["success"]:
        raise ValueError("GE checkpoint failed — patrz Data Docs")


def run_observability(**_):
    from pyspark.sql import SparkSession

    from observability.orchestrator_extended import (
        DataObservabilityOrchestrator,
        LineageChecker,
        LineageEdge,
    )

    spark = SparkSession.builder.getOrCreate()
    orc   = DataObservabilityOrchestrator(spark)
    lineage = LineageChecker(spark, edges=[
        LineageEdge("ecommerce_catalog.bronze.orders_raw",      "ecommerce_catalog.silver.orders",    sla_hours=2),
        LineageEdge("ecommerce_catalog.bronze.clickstream_raw", "ecommerce_catalog.silver.customers", sla_hours=2),
    ])
    orc.run_all(
        pipeline_run=f"airflow-{datetime.utcnow().isoformat()}",
        tables={
            "ecommerce_catalog.silver.orders":    1,
            "ecommerce_catalog.silver.products":  1,
            "ecommerce_catalog.silver.customers": 1,
        },
        lineage=lineage,
    )


with DAG(
    dag_id="ecommerce_m10_data_quality",
    description="Daily DQ run — GE + dbt + observability",
    start_date=datetime(2026, 11, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dq", "ecommerce", "month10"],
) as dag:

    ge_validate = PythonOperator(
        task_id="ge_validate_silver",
        python_callable=run_ge_checkpoint,
    )

    observability = PythonOperator(
        task_id="observability_run",
        python_callable=run_observability,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/dbt/ecommerce_m10 && "
            "dbt test --store-failures --select silver_orders silver_products"
        ),
    )

    publish_docs = BashOperator(
        task_id="publish_data_docs",
        bash_command="great_expectations docs build && gsutil rsync -r "
                     "great_expectations/data_docs gs://nexashop-ge-docs/",
    )

    ge_validate >> observability >> dbt_test >> publish_docs
