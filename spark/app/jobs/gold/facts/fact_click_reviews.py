import sys

from pyspark.sql.functions import col, current_timestamp, date_format, coalesce, lit
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.fact_click_reviews")
GOLD_PATH = f"{GOLD_BUCKET}/fact_click_reviews/"


def run(spark):
    df_reviews = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_reviews/")
    df_orders = spark.read.format("delta").load(f"{GOLD_BUCKET}/fact_click_orders/")
    df_prod = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_click_product/")

    # 1. Join with fact_click_orders to resolve order_sk
    df_joined = df_reviews.join(df_orders, df_reviews["order_id"] == df_orders["order_id"], "left").select(
        df_reviews["*"], coalesce(df_orders["order_sk"], lit(-1)).alias("order_sk")
    )

    # 2. Join with dim_click_product to resolve product_sk
    df_joined = df_joined.join(df_prod, df_joined["product_id"] == df_prod["product_id"], "left").select(
        df_joined["*"], coalesce(df_prod["product_sk"], lit(-1)).alias("product_sk")
    )

    # 3. Resolve date_sk from review_time
    df_fact = df_joined.withColumn("date_sk", date_format(col("review_time"), "yyyyMMdd").cast(IntegerType()))

    df_fact = generate_surrogate_key(df_fact, "review_sk")
    df_final = df_fact.select(
        col("review_sk"),
        col("review_id"),
        col("order_sk"),
        col("product_sk"),
        col("date_sk"),
        col("rating"),
        col("review_text"),
        col("sentiment"),
        col("review_time").alias("review_timestamp"),
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_final, spark, "gold", "fact_click_reviews", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_FactClickReviews", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
