import sys

from pyspark.sql.functions import col, trim, when, current_timestamp, round as spark_round
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import safe_to_timestamp, extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.order_reviews")

BRONZE_PATH = f"{BRONZE_BUCKET}/olist.public.olist_order_reviews/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_order_reviews/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["review_id", "order_id"])

    df_silver = df_dedup.select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        col("review_score").cast(IntegerType()).alias("review_score"),
        trim(col("review_comment_title")).alias("review_comment_title"),
        trim(col("review_comment_message")).alias("review_comment_message"),
        safe_to_timestamp(col("review_creation_date")).alias("review_creation_date"),
        safe_to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
    ).filter(col("review_id").isNotNull() & col("order_id").isNotNull())

    df_silver = (
        df_silver.withColumn(
            "review_rating",
            when(col("review_score") >= 4, "POSITIVE")
            .when(col("review_score") == 3, "NEUTRAL")
            .when(col("review_score") <= 2, "NEGATIVE")
            .otherwise("UNKNOWN"),
        )
        .withColumn(
            "has_comment",
            (col("review_comment_title").isNotNull() & (trim(col("review_comment_title")) != ""))
            | (col("review_comment_message").isNotNull() & (trim(col("review_comment_message")) != "")),
        )
        .withColumn(
            "review_response_time_hours",
            when(
                col("review_answer_timestamp").isNotNull() & col("review_creation_date").isNotNull(),
                spark_round(
                    (col("review_answer_timestamp").cast("double") - col("review_creation_date").cast("double"))
                    / 3600.0,
                    2,
                ),
            ).otherwise(None),
        )
        .withColumn("processed_at", current_timestamp())
    )

    upsert_with_metrics(df_silver, spark, "silver", "olist_order_reviews", SILVER_PATH, SILVER_KEYS["olist_order_reviews"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_OrderReviews", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
