import sys

from pyspark.sql.functions import col, current_timestamp, date_format, coalesce, lit
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.fact_click_orders")
GOLD_PATH = f"{GOLD_BUCKET}/fact_click_orders/"


def run(spark):
    df_orders = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_orders/")
    df_cust = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_click_customer/")
    df_dev = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_device/")
    df_src = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_traffic_source/")

    # 1. Join with dim_click_customer to resolve customer_sk
    df_joined = df_orders.join(df_cust, df_orders["customer_id"] == df_cust["customer_id"], "left").select(
        df_orders["*"], coalesce(df_cust["customer_sk"], lit(-1)).alias("customer_sk")
    )

    # 2. Join with dim_device
    df_joined = df_joined.join(df_dev, df_joined["device"] == df_dev["device"], "left").select(
        df_joined["*"], coalesce(df_dev["device_sk"], lit(-1)).alias("device_sk")
    )

    # 3. Join with dim_traffic_source
    df_joined = df_joined.join(df_src, df_joined["source"] == df_src["traffic_source"], "left").select(
        df_joined["*"], coalesce(df_src["source_sk"], lit(-1)).alias("source_sk")
    )

    # 4. Resolve date_sk
    df_fact = df_joined.withColumn("date_sk", date_format(col("order_time"), "yyyyMMdd").cast(IntegerType()))

    df_fact = generate_surrogate_key(df_fact, "order_sk")
    df_final = df_fact.select(
        col("order_sk"),
        col("order_id"),
        col("customer_sk"),
        col("date_sk"),
        col("device_sk"),
        col("source_sk"),
        col("payment_method"),
        col("discount_pct"),
        col("subtotal_usd"),
        col("total_usd"),
        col("country"),
        col("order_time").alias("order_timestamp"),
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_final, spark, "gold", "fact_click_orders", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_FactClickOrders", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
