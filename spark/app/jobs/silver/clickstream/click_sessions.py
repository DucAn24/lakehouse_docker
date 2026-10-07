import sys

from pyspark.sql.functions import col, trim, upper, current_timestamp, to_timestamp, coalesce, from_unixtime, when
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.click_sessions")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_sessions/"
SILVER_PATH = f"{SILVER_BUCKET}/click_sessions/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["session_id"])

    df_silver = df_dedup.select(
        col("session_id").cast(IntegerType()).alias("session_id"),
        col("customer_id").cast(IntegerType()).alias("customer_id"),
        coalesce(
            when(trim(col("start_time").cast("string")).rlike("^[0-9]+$"),
                 from_unixtime(col("start_time").cast("long") / 1000).cast("timestamp")),
            to_timestamp(trim(col("start_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            to_timestamp(trim(col("start_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ssX"),
            to_timestamp(trim(col("start_time").cast("string")), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(trim(col("start_time").cast("string")), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(trim(col("start_time").cast("string"))),
        ).alias("start_time"),
        upper(trim(col("device"))).alias("device"),
        upper(trim(col("source"))).alias("source"),
        upper(trim(col("country"))).alias("country"),
        current_timestamp().alias("processed_at"),
    ).filter(col("session_id").isNotNull())

    upsert_with_metrics(df_silver, spark, "silver", "click_sessions", SILVER_PATH, SILVER_KEYS["click_sessions"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickSessions", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
