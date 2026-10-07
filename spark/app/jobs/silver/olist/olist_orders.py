import sys

from pyspark.sql.functions import (
    col,
    when,
    trim,
    upper,
    current_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    datediff,
)

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import safe_to_timestamp, extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.orders")

BRONZE_PATH = f"{BRONZE_BUCKET}/olist.public.olist_orders/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_orders/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["order_id"])

    df_typed = df_dedup.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        upper(trim(col("order_status"))).alias("order_status"),
        safe_to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
        safe_to_timestamp(col("order_approved_at")).alias("order_approved_at"),
        safe_to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
        safe_to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
        safe_to_timestamp(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
    ).filter(col("order_id").isNotNull())

    df_silver = (
        df_typed.withColumn("order_year", year(col("order_purchase_timestamp")))
        .withColumn("order_month", month(col("order_purchase_timestamp")))
        .withColumn("order_day", dayofmonth(col("order_purchase_timestamp")))
        .withColumn("order_hour", hour(col("order_purchase_timestamp")))
        .withColumn(
            "approval_delay_days",
            when(
                col("order_approved_at").isNotNull(),
                datediff(col("order_approved_at"), col("order_purchase_timestamp")),
            ).otherwise(None),
        )
        .withColumn(
            "actual_delivery_days",
            when(
                col("order_delivered_customer_date").isNotNull(),
                datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")),
            ).otherwise(None),
        )
        .withColumn(
            "estimated_delivery_days",
            when(
                col("order_estimated_delivery_date").isNotNull(),
                datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")),
            ).otherwise(None),
        )
        .withColumn(
            "delivery_delay_days",
            when(
                col("order_delivered_customer_date").isNotNull() & col("order_estimated_delivery_date").isNotNull(),
                datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")),
            ).otherwise(None),
        )
        .withColumn(
            "is_delivered_late",
            when(col("delivery_delay_days") > 0, True).when(col("delivery_delay_days") <= 0, False).otherwise(None),
        )
        .withColumn("is_delivered", col("order_status") == "DELIVERED")
        .withColumn("processed_at", current_timestamp())
    )

    upsert_with_metrics(df_silver, spark, "silver", "olist_orders", SILVER_PATH, SILVER_KEYS["olist_orders"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_Orders", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
