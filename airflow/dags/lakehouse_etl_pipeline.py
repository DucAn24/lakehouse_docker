"""
lakehouse_etl_pipeline
======================
Full medallion ETL pipeline for the e-commerce data lakehouse (Olist + Clickstream).

Stage 1 – Kafka → Bronze
    Reads all Debezium CDC topics from Kafka and writes Delta tables to MinIO.

Stage 2 – Bronze → Silver  (runs after the Bronze quality gate)
    Cleans, types, and enriches each domain entity.

Stage 3 – Silver → Gold  (runs after the Silver quality gate)
    Builds dimension and fact tables for analytics.

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

from spark_submit_defaults import BRONZE_JOBS, SILVER_JOBS, GOLD_JOBS, OPS_JOBS, common_kwargs, dq_kwargs, register_kwargs

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
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
    tags=["lakehouse", "spark", "etl", "medallion"],
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

        register_bronze_tables = SparkSubmitOperator(
            task_id="register_bronze_tables",
            **register_kwargs("bronze"),
        )

        dq_bronze = SparkSubmitOperator(task_id="dq_bronze", **dq_kwargs("bronze"))

        kafka_to_bronze >> register_bronze_tables >> dq_bronze

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

        register_silver_tables = SparkSubmitOperator(
            task_id="register_silver_tables",
            **register_kwargs("silver"),
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

        silver_tasks[-1] >> register_silver_tables >> dq_silver

    # =========================================================================
    # Stage 3: Silver → Gold
    # =========================================================================
    with TaskGroup("silver_to_gold") as gold_group:
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

        # Facts
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

        # Run the gold Spark jobs sequentially.
        gold_tasks = [
            s2g_dim_date,
            s2g_dim_customer,
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

    # =========================================================================
    # Cross-group dependencies
    # =========================================================================
    dq_bronze >> silver_tasks[0]
    dq_silver >> gold_tasks[0]

    # =========================================================================
    # Stage 4: Vacuum / Cleanup (Giúp giải phóng ổ cứng)
    # =========================================================================
    vacuum_delta_tables = SparkSubmitOperator(
        task_id="vacuum_delta_tables",
        **common_kwargs(
            "LakehouseVacuum",
            f"{OPS_JOBS}/vacuum_tables.py",
        ),
    )

    gold_group >> vacuum_delta_tables
