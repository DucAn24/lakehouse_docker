import sys

from pyspark.sql.functions import col, trim, upper, current_timestamp, to_timestamp, coalesce, from_unixtime, when
from pyspark.sql.types import IntegerType, DoubleType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.click_orders")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_orders/"
SILVER_PATH = f"{SILVER_BUCKET}/click_orders/"


def parse_order_time(column):
    order_time_text = trim(column.cast("string"))
    return coalesce(
        when(
            order_time_text.rlike("^[0-9]+$"),
            from_unixtime(order_time_text.cast("long") / 1000).cast("timestamp"),
        ),
        to_timestamp(order_time_text, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(order_time_text, "yyyy-MM-dd'T'HH:mm:ssX"),
        to_timestamp(order_time_text, "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(order_time_text, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(order_time_text),
    )


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["order_id"])

    df_silver = df_dedup.select(
        col("order_id").cast(IntegerType()).alias("order_id"),
        col("customer_id").cast(IntegerType()).alias("customer_id"),
        parse_order_time(col("order_time")).alias("order_time"),
        upper(trim(col("payment_method"))).alias("payment_method"),
        col("discount_pct").cast(DoubleType()).alias("discount_pct"),
        col("subtotal_usd").cast(DoubleType()).alias("subtotal_usd"),
        col("total_usd").cast(DoubleType()).alias("total_usd"),
        upper(trim(col("country"))).alias("country"),
        upper(trim(col("device"))).alias("device"),
        upper(trim(col("source"))).alias("source"),
        current_timestamp().alias("processed_at"),
    ).filter(col("order_id").isNotNull())

    upsert_with_metrics(df_silver, spark, "silver", "click_orders", SILVER_PATH, SILVER_KEYS["click_orders"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickOrders", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
