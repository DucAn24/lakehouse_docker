import sys

from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
    coalesce,
    broadcast,
    date_format,
    to_date,
    sum as spark_sum,
    count as spark_count,
    first,
)

from pyspark.sql.types import IntegerType, LongType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.fact_orders")

GOLD_PATH = f"{GOLD_BUCKET}/fact_orders/"


def _aggregate_payments(spark):
    """Aggregate order payments to one row per order_id."""
    df_payments = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_order_payments/")
    return df_payments.groupBy("order_id").agg(
        spark_sum("payment_value").alias("total_payment_value"),
        spark_sum("payment_installments").alias("payment_installments"),
        spark_count("payment_sequential").alias("payment_count"),
        first("payment_type").alias("primary_payment_type"),
    )


def _status_category(status_col):
    return (
        when(status_col == "DELIVERED", "COMPLETED")
        .when(status_col.isin("SHIPPED", "INVOICED", "PROCESSING", "APPROVED"), "IN_PROGRESS")
        .when(status_col == "CREATED", "PENDING")
        .when(status_col.isin("CANCELED", "UNAVAILABLE"), "FAILED")
        .otherwise("OTHER")
    )


def run(spark):
    df_order_items = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_order_items/")
    df_orders = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_orders/")
    df_silver_customers = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_customers/")

    df_customer_dim = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_customer/")
    df_seller_dim = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_seller/")
    df_product_dim = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_product/")

    df_customer_lookup = broadcast(df_customer_dim.select("customer_sk", "customer_unique_id"))
    df_seller_lookup = broadcast(df_seller_dim.select("seller_sk", "seller_id"))
    df_product_lookup = broadcast(df_product_dim.select("product_sk", "product_id"))

    df_payments_agg = _aggregate_payments(spark)

    # order_items INNER JOIN orders
    df_fact = df_order_items.join(
        df_orders.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_delay_days",
            "is_delivered_late",
            "is_delivered",
            "actual_delivery_days",
        ),
        "order_id",
        "inner",
    )

    # Bridge: customer_id -> customer_unique_id
    df_fact = df_fact.join(
        df_silver_customers.select("customer_id", "customer_unique_id"),
        "customer_id",
        "left",
    )

    # LEFT JOIN aggregated payments
    df_fact = df_fact.join(df_payments_agg, "order_id", "left")

    # Dimension SK lookups
    df_fact = (
        df_fact.join(df_customer_lookup, "customer_unique_id", "left")
        .join(df_seller_lookup, "seller_id", "left")
        .join(df_product_lookup, "product_id", "left")
    )

    # Final select
    df_fact = df_fact.select(
        col("order_id"),
        col("order_item_id"),
        date_format(to_date("order_purchase_timestamp"), "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        coalesce(col("customer_sk"), lit(-1)).cast(LongType()).alias("customer_sk"),
        coalesce(col("product_sk"), lit(-1)).cast(LongType()).alias("product_sk"),
        coalesce(col("seller_sk"), lit(-1)).cast(LongType()).alias("seller_sk"),
        col("order_status"),
        _status_category(col("order_status")).alias("status_category"),
        col("is_delivered"),
        col("is_delivered_late"),
        col("price").alias("unit_price"),
        col("freight_value"),
        col("total_item_value"),
        col("actual_delivery_days"),
        col("delivery_delay_days"),
        coalesce(col("total_payment_value"), lit(0)).alias("total_payment_value"),
        coalesce(col("payment_installments"), lit(0)).cast(IntegerType()).alias("payment_installments"),
        coalesce(col("primary_payment_type"), lit("UNKNOWN")).alias("primary_payment_type"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_customer_date"),
        col("shipping_limit_date"),
        current_timestamp().alias("etl_loaded_at"),
    )

    df_fact = generate_surrogate_key(df_fact, "order_item_sk")

    write_with_metrics(df_fact, spark, "gold", "fact_orders", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_FactOrders", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
