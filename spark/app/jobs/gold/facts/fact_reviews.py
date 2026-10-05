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
)
from pyspark.sql.types import IntegerType, LongType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.fact_reviews")

GOLD_PATH = f"{GOLD_BUCKET}/fact_reviews/"


def run(spark):
    df_reviews = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_order_reviews/")
    df_orders = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_orders/")
    df_silver_customers = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_customers/")

    df_customer_dim = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_customer/")
    df_customer_lookup = broadcast(df_customer_dim.select("customer_sk", "customer_unique_id"))

    # reviews INNER JOIN orders (to get customer_id)
    df_fact = df_reviews.join(
        df_orders.select("order_id", "customer_id"),
        "order_id",
        "inner",
    )

    # Bridge: customer_id -> customer_unique_id
    df_fact = df_fact.join(
        df_silver_customers.select("customer_id", "customer_unique_id"),
        "customer_id",
        "left",
    )

    # Dimension SK lookup
    df_fact = df_fact.join(df_customer_lookup, "customer_unique_id", "left")

    # Final select
    df_fact = df_fact.select(
        col("review_id"),
        col("order_id"),
        date_format(to_date("review_creation_date"), "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        coalesce(col("customer_sk"), lit(-1)).cast(LongType()).alias("customer_sk"),
        col("review_score"),
        col("review_rating"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("has_comment").alias("review_has_comment"),
        col("review_response_time_hours"),
        when(col("review_score") >= 4, 1).otherwise(0).alias("is_positive"),
        when(col("review_score") == 3, 1).otherwise(0).alias("is_neutral"),
        when(col("review_score") <= 2, 1).otherwise(0).alias("is_negative"),
        col("review_creation_date"),
        col("review_answer_timestamp"),
        current_timestamp().alias("etl_loaded_at"),
    )

    df_fact = generate_surrogate_key(df_fact, "review_sk")

    write_with_metrics(df_fact, spark, "gold", "fact_reviews", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_FactReviews", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
