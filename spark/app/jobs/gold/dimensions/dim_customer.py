import sys

from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key, with_unknown_member
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_customer")

GOLD_PATH = f"{GOLD_BUCKET}/dim_customer/"


def run(spark):
    df_customers = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_customers/")

    window_spec = Window.partitionBy("customer_unique_id").orderBy("customer_id")
    df_dim = (
        df_customers.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
    )

    df_dim = generate_surrogate_key(df_dim, "customer_sk")
    df_dim = df_dim.select(
        "customer_sk",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "customer_region",
        "customer_latitude",
        "customer_longitude",
        current_timestamp().alias("etl_loaded_at"),
    )

    df_final = with_unknown_member(
        df_dim,
        {
            "customer_sk": -1,
            "customer_unique_id": "UNKNOWN",
            "customer_zip_code_prefix": "00000",
            "customer_city": "UNKNOWN",
            "customer_state": "UNKNOWN",
            "customer_region": "OUTROS",
        },
    )

    write_with_metrics(df_final, spark, "gold", "dim_customer", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimCustomer", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
