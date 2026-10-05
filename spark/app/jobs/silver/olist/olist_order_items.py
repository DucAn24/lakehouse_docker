import sys

from pyspark.sql.functions import col, trim, when, current_timestamp, round as spark_round
from pyspark.sql.types import DoubleType, IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import safe_to_timestamp
from common.writers import write_with_metrics

logger = get_logger("b2s.order_items")

BRONZE_PATH = f"{BRONZE_BUCKET}/olist.public.olist_order_items/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_order_items/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_clean = df.filter(col("op") != "d").select("after.*")

    df_silver = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        col("order_item_id").cast(IntegerType()).alias("order_item_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("seller_id")).alias("seller_id"),
        safe_to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
        col("price").cast(DoubleType()).alias("price"),
        col("freight_value").cast(DoubleType()).alias("freight_value"),
    ).filter(col("order_id").isNotNull() & col("order_item_id").isNotNull())

    df_silver = (
        df_silver.withColumn("total_item_value", col("price") + col("freight_value"))
        .withColumn(
            "freight_ratio",
            when(col("price") > 0, spark_round(col("freight_value") / col("price"), 4)).otherwise(None),
        )
        .withColumn("processed_at", current_timestamp())
    )

    write_with_metrics(df_silver, spark, "silver", "olist_order_items", SILVER_PATH)


if __name__ == "__main__":
    spark = create_spark_session("B2S_OrderItems", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
