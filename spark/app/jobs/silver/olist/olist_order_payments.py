import sys

from pyspark.sql.functions import col, trim, upper, when, current_timestamp, round as spark_round
from pyspark.sql.types import DoubleType, IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.writers import write_with_metrics

logger = get_logger("b2s.order_payments")

BRONZE_PATH = f"{BRONZE_BUCKET}/olist.public.olist_order_payments/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_order_payments/"


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_clean = df.filter(col("op") != "d").select("after.*")

    df_silver = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential").cast(IntegerType()).alias("payment_sequential"),
        upper(trim(col("payment_type"))).alias("payment_type"),
        col("payment_installments").cast(IntegerType()).alias("payment_installments"),
        col("payment_value").cast(DoubleType()).alias("payment_value"),
    ).filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))

    df_silver = (
        df_silver.withColumn(
            "installment_value",
            when(
                col("payment_installments").isNotNull()
                & (col("payment_installments") > 0)
                & col("payment_value").isNotNull(),
                spark_round(col("payment_value") / col("payment_installments"), 2),
            ).otherwise(col("payment_value")),
        )
        .withColumn(
            "is_installment_payment",
            when(col("payment_installments").isNotNull(), col("payment_installments") > 1).otherwise(False),
        )
        .withColumn("processed_at", current_timestamp())
    )

    write_with_metrics(df_silver, spark, "silver", "olist_order_payments", SILVER_PATH)


if __name__ == "__main__":
    spark = create_spark_session("B2S_OrderPayments", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
