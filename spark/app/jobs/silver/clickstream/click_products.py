import sys

from pyspark.sql.functions import col, trim, upper, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.writers import write_with_metrics

logger = get_logger("b2s.click_products")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_products/"
SILVER_PATH = f"{SILVER_BUCKET}/click_products/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["product_id"])

    # Price validation and margin calculation check
    df_silver = df_dedup.select(
        col("product_id").cast(IntegerType()).alias("product_id"),
        upper(trim(col("category"))).alias("product_category"),
        trim(col("name")).alias("product_name"),
        col("price_usd").cast(DoubleType()).alias("price_usd"),
        col("cost_usd").cast(DoubleType()).alias("cost_usd"),
        col("margin_usd").cast(DoubleType()).alias("margin_usd"),
        current_timestamp().alias("processed_at"),
    ).filter(col("product_id").isNotNull() & (col("price_usd") >= 0.0))

    write_with_metrics(df_silver, spark, "silver", "click_products", SILVER_PATH)


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickProducts", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
