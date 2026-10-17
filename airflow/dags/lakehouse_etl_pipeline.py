"""
lakehouse_etl_pipeline
======================
Full medallion ETL pipeline for the e-commerce data lakehouse (Olist + Clickstream).

Stage 1 – Kafka → Bronze
    Reads all Debezium CDC topics from Kafka and writes Iceberg tables (Polaris catalog `lakehouse`, data on S3 / SeaweedFS).

Stage 2 – Bronze → Silver  (runs after the Bronze quality gate)
    Cleans, types, and enriches each domain entity.

Stage 3 – Silver → Gold  (runs after the Silver quality gate)
    `dbt build` (repo `dbt/`) builds the dimension and fact tables on Trino and runs their tests.

Each Spark job is submitted to the standalone spark-master via
SparkSubmitOperator. Airflow must be on the same Docker network
('data-network') as the Spark cluster.

Connections required in Airflow UI (Admin → Connections):
  - conn_id: spark_default
    conn_type: Spark
    host: spark://spark-master
    port: 7077
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import TaskGroup

from alerting import notify_failure
from gold_tasks import dbt_build
from spark_submit_defaults import BRONZE_JOBS, SILVER_JOBS, OPS_JOBS, common_kwargs, dq_kwargs

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="lakehouse_etl_pipeline",
    description="Kafka → Bronze → Silver → Gold medallion ETL (Olist + Clickstream)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "spark", "dbt", "etl", "medallion"],
    doc_md=__doc__,
) as dag:
    # =========================================================================
    # Stage 1: Kafka → Bronze
    # =========================================================================
    with TaskGroup("bronze_ingestion") as bronze_group:
        kafka_to_bronze = SparkSubmitOperator(
            task_id="kafka_to_bronze",
            **common_kwargs(
                "LakehouseKafkaToBronze",
                f"{BRONZE_JOBS}/kafka_to_bronze.py",
            ),
        )

        dq_bronze = SparkSubmitOperator(task_id="dq_bronze", **dq_kwargs("bronze"))

        kafka_to_bronze >> dq_bronze

    # =========================================================================
    # Stage 2: Bronze → Silver
    # =========================================================================
    with TaskGroup("bronze_to_silver") as silver_group:
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

        # Run the Spark jobs one after another to keep RAM usage low.
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

        silver_tasks[-1] >> dq_silver

    # =========================================================================
    # Stage 3: Silver → Gold  (dbt on Trino, repo `dbt/`)
    # =========================================================================
    with TaskGroup("silver_to_gold") as gold_group:
        dbt_build_gold = dbt_build()

        dq_gold = SparkSubmitOperator(task_id="dq_gold", **dq_kwargs("gold"))

        dbt_build_gold >> dq_gold

    # =========================================================================
    # Cross-group dependencies
    # =========================================================================
    dq_bronze >> silver_tasks[0]
    dq_silver >> dbt_build_gold

    # =========================================================================
    # Stage 4: Iceberg maintenance (expire snapshots, remove orphan files) — frees S3 space
    # =========================================================================
    maintain_iceberg_tables = SparkSubmitOperator(
        task_id="maintain_iceberg_tables",
        **common_kwargs(
            "LakehouseMaintenance",
            f"{OPS_JOBS}/maintain_tables.py",
        ),
    )

    gold_group >> maintain_iceberg_tables
