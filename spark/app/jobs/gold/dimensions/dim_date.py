import sys
from datetime import datetime, timedelta

from pyspark.sql.functions import (
    col,
    when,
    current_timestamp,
    year,
    month,
    dayofmonth,
    dayofweek,
    quarter,
    weekofyear,
    date_format,
    concat_ws,
)
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_date")

GOLD_PATH = f"{GOLD_BUCKET}/dim_date/"


def run(spark):
    df_orders = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_orders/")
    df_orders.createOrReplaceTempView("orders_temp")

    date_range_df = spark.sql("""
        SELECT
            CAST(MIN(order_purchase_timestamp) AS DATE) AS min_date,
            CAST(MAX(order_purchase_timestamp) AS DATE) AS max_date
        FROM orders_temp
        WHERE order_purchase_timestamp IS NOT NULL
    """)

    row = date_range_df.collect()[0]
    min_date_val, max_date_val = row["min_date"], row["max_date"]

    if min_date_val is None or max_date_val is None:
        logger.warning("No valid dates in orders -- using default range 2016-2020")
        start_date = datetime(2016, 1, 1)
        end_date = datetime(2020, 12, 31)
    else:
        start_date = datetime(min_date_val.year, 1, 1)
        end_date = datetime(max_date_val.year, 12, 31)

    logger.info("Date range: %s to %s", start_date.date(), end_date.date())

    date_list = []
    cur = start_date
    while cur <= end_date:
        date_list.append((cur,))
        cur += timedelta(days=1)

    df_dates = spark.createDataFrame(date_list, ["full_date"])

    df_dim = df_dates.select(
        date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        col("full_date"),
        dayofmonth("full_date").alias("day_of_month"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        weekofyear("full_date").alias("week_of_year"),
        month("full_date").alias("month_number"),
        date_format("full_date", "MMMM").alias("month_name"),
        quarter("full_date").alias("quarter"),
        year("full_date").alias("year"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        date_format("full_date", "yyyy-MM").alias("year_month"),
        concat_ws("-Q", year("full_date"), quarter("full_date")).alias("year_quarter"),
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_dim, spark, "gold", "dim_date", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimDate", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
