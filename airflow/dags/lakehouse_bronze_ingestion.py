"""
lakehouse_bronze_ingestion
==========================
Standalone Bronze DAG for the lakehouse (Olist + Clickstream).

Reads raw CDC data from Kafka into Delta bronze tables and registers the
bronze schema in Unity Catalog + Trino. When it finishes, it triggers the silver DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerting import notify_failure
from spark_submit_defaults import BRONZE_JOBS, common_kwargs, dq_kwargs, register_kwargs
from monitoring_tasks import finish_with_monitoring

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="lakehouse_bronze_ingestion",
    description="Kafka → Bronze ingestion for Olist + Clickstream",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "spark", "bronze"],
    doc_md=__doc__,
) as dag:
    kafka_to_bronze = SparkSubmitOperator(
        task_id="kafka_to_bronze",
        **common_kwargs(
            "LakehouseKafkaToBronze",
            f"{BRONZE_JOBS}/kafka_to_bronze.py",
        ),
    )

    register_bronze_tables = SparkSubmitOperator(
        task_id="register_bronze_tables",
        **register_kwargs("bronze"),
    )

    dq_bronze = SparkSubmitOperator(task_id="dq_bronze", **dq_kwargs("bronze"))

    kafka_to_bronze >> register_bronze_tables >> dq_bronze

    finish_with_monitoring(dq_bronze)
