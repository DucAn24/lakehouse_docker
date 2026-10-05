import sys

from pyspark.sql.functions import col, current_timestamp, date_format, coalesce, lit
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.fact_clickstream_events")
GOLD_PATH = f"{GOLD_BUCKET}/fact_clickstream_events/"


def run(spark):
    df_events = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_events/")
    df_sess = spark.read.format("delta").load(f"{GOLD_BUCKET}/fact_sessions/")
    df_prod = spark.read.format("delta").load(f"{GOLD_BUCKET}/dim_click_product/")

    # 1. Join with fact_sessions to resolve session_sk and customer_sk
    df_joined = df_events.join(df_sess, df_events["session_id"] == df_sess["session_id"], "left").select(
        df_events["*"],
        coalesce(df_sess["session_sk"], lit(-1)).alias("session_sk"),
        coalesce(df_sess["customer_sk"], lit(-1)).alias("customer_sk"),
    )

    # 2. Join with dim_click_product to resolve product_sk
    df_joined = df_joined.join(df_prod, df_joined["product_id"] == df_prod["product_id"], "left").select(
        df_joined["*"], coalesce(df_prod["product_sk"], lit(-1)).alias("product_sk")
    )

    # 3. Resolve date_sk from event_timestamp
    df_fact = df_joined.withColumn("date_sk", date_format(col("event_timestamp"), "yyyyMMdd").cast(IntegerType()))

    df_fact = generate_surrogate_key(df_fact, "event_sk")
    df_final = df_fact.select(
        col("event_sk"),
        col("event_id"),
        col("session_sk"),
        col("customer_sk"),
        col("date_sk"),
        col("product_sk"),
        col("event_type"),
        col("quantity"),
        # col("cart_size"),
        col("payment_method"),
        # col("discount_pct"),
        # col("amount_usd"),
        col("event_timestamp"),
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_final, spark, "gold", "fact_clickstream_events", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_FactClickstreamEvents", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
