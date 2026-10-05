import sys
from datetime import datetime

from pyspark.sql.functions import col, current_timestamp
from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_click_customer")
GOLD_PATH = f"{GOLD_BUCKET}/dim_click_customer/"


def run(spark):
    df_customers = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_customers/")

    # Generate customer_sk
    df_dim = generate_surrogate_key(df_customers, "customer_sk")
    df_dim = df_dim.select(
        col("customer_sk"),
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("country"),
        col("age"),
        col("signup_date"),
        col("marketing_opt_in"),
        current_timestamp().alias("etl_loaded_at"),
    )

    # Add UNKNOWN row
    unknown_row = [(-1, -1, "UNKNOWN", "UNKNOWN", "UNKNOWN", None, None, False, datetime.now())]
    df_unknown = spark.createDataFrame(unknown_row, schema=df_dim.schema)
    df_final = df_dim.unionByName(df_unknown)

    write_with_metrics(df_final, spark, "gold", "dim_click_customer", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimClickCustomer", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
