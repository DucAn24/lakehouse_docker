import sys

from pyspark.sql.functions import col, trim, upper, when, current_timestamp, round as spark_round
from pyspark.sql.types import DoubleType, IntegerType

from common.config import create_spark_session, get_logger
from common.tables import SILVER_KEYS, table_id
from common.writers import upsert_with_metrics

logger = get_logger("b2s.order_payments")

BRONZE_TABLE = table_id("bronze", "olist_order_payments")
SILVER_TABLE = table_id("silver", "olist_order_payments")


def run(spark):
    df = spark.table(BRONZE_TABLE)
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

    upsert_with_metrics(df_silver, spark, "silver", "olist_order_payments", SILVER_TABLE, SILVER_KEYS["olist_order_payments"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_OrderPayments")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
