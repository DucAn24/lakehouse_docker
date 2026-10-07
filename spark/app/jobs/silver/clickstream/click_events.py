import sys

from pyspark.sql.functions import col, trim, upper, current_timestamp, to_timestamp, coalesce, from_unixtime, when, lit
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.click_events")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_events/"
SILVER_PATH = f"{SILVER_BUCKET}/click_events/"


def parse_event_timestamp(column):
    event_timestamp_text = trim(column.cast("string"))
    return coalesce(
        when(
            event_timestamp_text.rlike("^[0-9]+$"),
            from_unixtime(event_timestamp_text.cast("long") / 1000).cast("timestamp"),
        ),
        to_timestamp(event_timestamp_text, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(event_timestamp_text, "yyyy-MM-dd'T'HH:mm:ssX"),
        to_timestamp(event_timestamp_text, "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(event_timestamp_text, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(event_timestamp_text),
    )


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["event_id"])

    df_silver = df_dedup.select(
        col("event_id").cast(IntegerType()).alias("event_id"),
        col("session_id").cast(IntegerType()).alias("session_id"),
        parse_event_timestamp(col("timestamp")).alias("event_timestamp"),
        upper(trim(col("event_type"))).alias("event_type"),
        coalesce(col("product_id").cast(IntegerType()), lit(-1)).alias("product_id"),
        coalesce(col("qty").cast(IntegerType()), lit(0)).alias("quantity"),
        coalesce(upper(trim(col("payment"))), lit("NOT_APPLICABLE")).alias("payment_method"),
        current_timestamp().alias("processed_at"),
    ).filter(col("event_id").isNotNull())

    upsert_with_metrics(df_silver, spark, "silver", "click_events", SILVER_PATH, SILVER_KEYS["click_events"])


if __name__ == "__main__":
    # Event data is larger, partition the spark session appropriately
    spark = create_spark_session("B2S_ClickEvents", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
