import sys

from pyspark.sql.functions import col, trim, current_timestamp, to_timestamp, when, coalesce, from_unixtime
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.writers import write_with_metrics

logger = get_logger("b2s.click_reviews")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_reviews/"
SILVER_PATH = f"{SILVER_BUCKET}/click_reviews/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["review_id"])

    # Cast fields and calculate sentiment based on rating (1-5 scale)
    df_silver = df_dedup.select(
        col("review_id").cast(IntegerType()).alias("review_id"),
        col("order_id").cast(IntegerType()).alias("order_id"),
        col("product_id").cast(IntegerType()).alias("product_id"),
        col("rating").cast(IntegerType()).alias("rating"),
        trim(col("review_text")).alias("review_text"),
        # Robust parsing: accept epoch ms or ISO / common timestamp formats
        coalesce(
            when(trim(col("review_time").cast("string")).rlike("^[0-9]+$"),
                 from_unixtime(col("review_time").cast("long") / 1000).cast("timestamp")),
            to_timestamp(trim(col("review_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            to_timestamp(trim(col("review_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ssX"),
            to_timestamp(trim(col("review_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(trim(col("review_time").cast("string")), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(trim(col("review_time").cast("string"))),
        ).alias("review_time"),
        when(col("rating") >= 4, "POSITIVE")
        .when(col("rating") == 3, "NEUTRAL")
        .otherwise("NEGATIVE")
        .alias("sentiment"),
        current_timestamp().alias("processed_at"),
    ).filter(col("review_id").isNotNull())

    write_with_metrics(df_silver, spark, "silver", "click_reviews", SILVER_PATH)


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickReviews", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
