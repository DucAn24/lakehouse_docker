import sys

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.click_order_items")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_order_items/"
SILVER_PATH = f"{SILVER_BUCKET}/click_order_items/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["order_id", "product_id"])

    df_silver = df_dedup.select(
        col("order_id").cast(IntegerType()).alias("order_id"),
        col("product_id").cast(IntegerType()).alias("product_id"),
        col("unit_price_usd").cast(DoubleType()).alias("unit_price_usd"),
        col("quantity").cast(IntegerType()).alias("quantity"),
        col("line_total_usd").cast(DoubleType()).alias("line_total_usd"),
        current_timestamp().alias("processed_at"),
    ).filter(col("order_id").isNotNull() & col("product_id").isNotNull() & (col("quantity") > 0))

    upsert_with_metrics(df_silver, spark, "silver", "click_order_items", SILVER_PATH, SILVER_KEYS["click_order_items"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickOrderItems", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
