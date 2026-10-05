import sys

from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    date_add,
    from_unixtime,
    lit,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import BooleanType, IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.writers import write_with_metrics

logger = get_logger("b2s.click_customers")

BRONZE_PATH = f"{BRONZE_BUCKET}/clickstream.public.click_customers/"
SILVER_PATH = f"{SILVER_BUCKET}/click_customers/"


def parse_signup_date(column):
    signup_date_text = trim(regexp_replace(column.cast("string"), r'^["\']|["\']$', ""))

    return coalesce(
        to_date(signup_date_text, "M/d/yyyy"),
        to_date(signup_date_text, "yyyy-MM-dd"),
        to_date(to_timestamp(signup_date_text, "yyyy-MM-dd HH:mm:ss")),

        when(
            signup_date_text.rlike("^[0-9]+$"),
            coalesce(
                when(signup_date_text.cast("long") > 10000000000,
                     to_date(from_unixtime(signup_date_text.cast("long") / 1000))),

                when(signup_date_text.cast("long") > 1000000,
                     to_date(from_unixtime(signup_date_text.cast("long")))),

                when(signup_date_text.cast("long") <= 1000000,
                     date_add(to_date(lit('1970-01-01')), signup_date_text.cast("int")))
            )
        )
    )


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["customer_id"])

    df_silver = df_dedup.select(
        col("customer_id").cast(IntegerType()).alias("customer_id"),
        trim(col("name")).alias("customer_name"),
        trim(col("email")).alias("email"),
        upper(trim(col("country"))).alias("country"),
        col("age").cast(IntegerType()).alias("age"),
        parse_signup_date(col("signup_date")).alias("signup_date"),
        col("marketing_opt_in").cast(BooleanType()).alias("marketing_opt_in"),
        current_timestamp().alias("processed_at"),
    ).filter(col("customer_id").isNotNull())

    write_with_metrics(df_silver, spark, "silver", "click_customers", SILVER_PATH)


if __name__ == "__main__":
    spark = create_spark_session("B2S_ClickCustomers", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
