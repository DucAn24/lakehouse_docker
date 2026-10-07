"""
lakehouse_bronze_to_silver
==========================
Standalone Silver DAG for the lakehouse.

Transforms bronze Delta tables into cleaned silver tables. The Spark jobs run
one after another so a local Spark setup does not need to execute them in
parallel. When finished, this DAG triggers the gold DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerting import notify_failure
from spark_submit_defaults import SILVER_JOBS, common_kwargs, dq_kwargs, register_kwargs
from monitoring_tasks import finish_with_monitoring

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="lakehouse_bronze_to_silver",
    description="Bronze → Silver transforms for Olist + Clickstream",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "spark", "silver"],
    doc_md=__doc__,
) as dag:
    b2s_customers = SparkSubmitOperator(
        task_id="customers",
        **common_kwargs("B2S_Customers", f"{SILVER_JOBS}/olist/olist_customers.py"),
    )
    b2s_sellers = SparkSubmitOperator(
        task_id="sellers",
        **common_kwargs("B2S_Sellers", f"{SILVER_JOBS}/olist/olist_sellers.py"),
    )
    b2s_products = SparkSubmitOperator(
        task_id="products",
        **common_kwargs("B2S_Products", f"{SILVER_JOBS}/olist/olist_products.py"),
    )
    b2s_orders = SparkSubmitOperator(
        task_id="orders",
        **common_kwargs("B2S_Orders", f"{SILVER_JOBS}/olist/olist_orders.py"),
    )
    b2s_order_items = SparkSubmitOperator(
        task_id="order_items",
        **common_kwargs("B2S_OrderItems", f"{SILVER_JOBS}/olist/olist_order_items.py"),
    )
    b2s_order_payments = SparkSubmitOperator(
        task_id="order_payments",
        **common_kwargs("B2S_OrderPayments", f"{SILVER_JOBS}/olist/olist_order_payments.py"),
    )
    b2s_order_reviews = SparkSubmitOperator(
        task_id="order_reviews",
        **common_kwargs("B2S_OrderReviews", f"{SILVER_JOBS}/olist/olist_order_reviews.py"),
    )

    # Clickstream B2S Tasks
    b2s_click_customers = SparkSubmitOperator(
        task_id="click_customers",
        **common_kwargs("B2S_ClickCustomers", f"{SILVER_JOBS}/clickstream/click_customers.py"),
    )
    b2s_click_products = SparkSubmitOperator(
        task_id="click_products",
        **common_kwargs("B2S_ClickProducts", f"{SILVER_JOBS}/clickstream/click_products.py"),
    )
    b2s_click_sessions = SparkSubmitOperator(
        task_id="click_sessions",
        **common_kwargs("B2S_ClickSessions", f"{SILVER_JOBS}/clickstream/click_sessions.py"),
    )
    b2s_click_orders = SparkSubmitOperator(
        task_id="click_orders",
        **common_kwargs("B2S_ClickOrders", f"{SILVER_JOBS}/clickstream/click_orders.py"),
    )
    b2s_click_order_items = SparkSubmitOperator(
        task_id="click_order_items",
        **common_kwargs("B2S_ClickOrderItems", f"{SILVER_JOBS}/clickstream/click_order_items.py"),
    )
    b2s_click_events = SparkSubmitOperator(
        task_id="click_events",
        **common_kwargs("B2S_ClickEvents", f"{SILVER_JOBS}/clickstream/click_events.py"),
    )
    b2s_click_reviews = SparkSubmitOperator(
        task_id="click_reviews",
        **common_kwargs("B2S_ClickReviews", f"{SILVER_JOBS}/clickstream/click_reviews.py"),
    )

    register_silver_tables = SparkSubmitOperator(
        task_id="register_silver_tables",
        **register_kwargs("silver"),
    )

    silver_tasks = [
        b2s_customers,
        b2s_sellers,
        b2s_products,
        b2s_orders,
        b2s_order_items,
        b2s_order_payments,
        b2s_order_reviews,
        # Clickstream B2S sequential list
        b2s_click_customers,
        b2s_click_products,
        b2s_click_sessions,
        b2s_click_orders,
        b2s_click_order_items,
        b2s_click_events,
        b2s_click_reviews,
    ]

    for current_task, next_task in zip(silver_tasks, silver_tasks[1:]):
        current_task >> next_task

    dq_silver = SparkSubmitOperator(task_id="dq_silver", **dq_kwargs("silver"))

    silver_tasks[-1] >> register_silver_tables >> dq_silver

    finish_with_monitoring(dq_silver)
