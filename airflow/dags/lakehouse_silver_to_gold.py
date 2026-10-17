"""
lakehouse_silver_to_gold
========================
Standalone Gold DAG for the lakehouse.

Builds the star schema (dimensions + facts) from the silver tables with dbt on Trino
(repo `dbt/`): `dbt build` runs every model in dependency order together with its data
tests, writing Iceberg tables straight into the Polaris catalog. They then pass the gold
DQ gate, whose results feed the Grafana Pipeline Monitoring dashboard.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerting import notify_failure
from gold_tasks import dbt_build
from spark_submit_defaults import dq_kwargs

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="lakehouse_silver_to_gold",
    description="Silver → Gold star schema (dbt on Trino) for Olist + Clickstream",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "dbt", "gold"],
    doc_md=__doc__,
) as dag:
    dbt_build_gold = dbt_build()

    dq_gold = SparkSubmitOperator(task_id="dq_gold", **dq_kwargs("gold"))

    dbt_build_gold >> dq_gold
