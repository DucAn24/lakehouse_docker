"""
lakehouse_silver_to_gold
========================
Standalone Gold DAG for the lakehouse.

Builds the dimensional and fact tables from silver data. The Spark jobs run in
sequence to keep the local Spark cluster from running multiple gold jobs at the
same time.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerting import notify_failure
from spark_submit_defaults import GOLD_JOBS, common_kwargs, dq_kwargs, register_kwargs
from monitoring_tasks import finish_with_monitoring

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="lakehouse_silver_to_gold",
    description="Silver → Gold transforms for Olist + Clickstream",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "spark", "gold"],
    doc_md=__doc__,
) as dag:
    s2g_dim_date = SparkSubmitOperator(
        task_id="dim_date",
        **common_kwargs("S2G_DimDate", f"{GOLD_JOBS}/dimensions/dim_date.py"),
    )
    s2g_dim_customer = SparkSubmitOperator(
        task_id="dim_customer",
        **common_kwargs("S2G_DimCustomer", f"{GOLD_JOBS}/dimensions/dim_customer.py"),
    )
    s2g_dim_seller = SparkSubmitOperator(
        task_id="dim_seller",
        **common_kwargs("S2G_DimSeller", f"{GOLD_JOBS}/dimensions/dim_seller.py"),
    )
    s2g_dim_product = SparkSubmitOperator(
        task_id="dim_product",
        **common_kwargs("S2G_DimProduct", f"{GOLD_JOBS}/dimensions/dim_product.py"),
    )

    # Clickstream Gold dimensions
    s2g_dim_device = SparkSubmitOperator(
        task_id="dim_device",
        **common_kwargs("S2G_DimDevice", f"{GOLD_JOBS}/dimensions/dim_device.py"),
    )
    s2g_dim_traffic_source = SparkSubmitOperator(
        task_id="dim_traffic_source",
        **common_kwargs("S2G_DimTrafficSource", f"{GOLD_JOBS}/dimensions/dim_traffic_source.py"),
    )
    s2g_dim_click_customer = SparkSubmitOperator(
        task_id="dim_click_customer",
        **common_kwargs("S2G_DimClickCustomer", f"{GOLD_JOBS}/dimensions/dim_click_customer.py"),
    )
    s2g_dim_click_product = SparkSubmitOperator(
        task_id="dim_click_product",
        **common_kwargs("S2G_DimClickProduct", f"{GOLD_JOBS}/dimensions/dim_click_product.py"),
    )

    s2g_fact_orders = SparkSubmitOperator(
        task_id="fact_orders",
        **common_kwargs("S2G_FactOrders", f"{GOLD_JOBS}/facts/fact_orders.py"),
    )
    s2g_fact_reviews = SparkSubmitOperator(
        task_id="fact_reviews",
        **common_kwargs("S2G_FactReviews", f"{GOLD_JOBS}/facts/fact_reviews.py"),
    )

    # Clickstream Facts
    s2g_fact_click_orders = SparkSubmitOperator(
        task_id="fact_click_orders",
        **common_kwargs("S2G_FactClickOrders", f"{GOLD_JOBS}/facts/fact_click_orders.py"),
    )
    s2g_fact_sessions = SparkSubmitOperator(
        task_id="fact_sessions",
        **common_kwargs("S2G_FactSessions", f"{GOLD_JOBS}/facts/fact_sessions.py"),
    )
    s2g_fact_clickstream_events = SparkSubmitOperator(
        task_id="fact_clickstream_events",
        **common_kwargs("S2G_FactClickstreamEvents", f"{GOLD_JOBS}/facts/fact_clickstream_events.py"),
    )
    s2g_fact_click_reviews = SparkSubmitOperator(
        task_id="fact_click_reviews",
        **common_kwargs("S2G_FactClickReviews", f"{GOLD_JOBS}/facts/fact_click_reviews.py"),
    )

    register_gold_tables = SparkSubmitOperator(
        task_id="register_gold_tables",
        **register_kwargs("gold"),
    )

    gold_tasks = [
        s2g_dim_customer,
        s2g_dim_date,
        s2g_dim_seller,
        s2g_dim_product,
        # Clickstream dimensions
        s2g_dim_device,
        s2g_dim_traffic_source,
        s2g_dim_click_customer,
        s2g_dim_click_product,
        # Facts
        s2g_fact_orders,
        s2g_fact_reviews,
        s2g_fact_click_orders,
        s2g_fact_sessions,
        s2g_fact_clickstream_events,
        s2g_fact_click_reviews,
    ]

    for current_task, next_task in zip(gold_tasks, gold_tasks[1:]):
        current_task >> next_task

    dq_gold = SparkSubmitOperator(task_id="dq_gold", **dq_kwargs("gold"))

    gold_tasks[-1] >> register_gold_tables >> dq_gold

    finish_with_monitoring(dq_gold)
