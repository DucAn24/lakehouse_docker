import sys

from pyspark.sql.functions import col, current_timestamp

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_seller")

GOLD_PATH = f"{GOLD_BUCKET}/dim_seller/"


def run(spark):
    df_sellers = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_sellers/")

    df_dim = df_sellers.select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state"),
        col("seller_region"),
        col("seller_latitude"),
        col("seller_longitude"),
    )
    df_dim = generate_surrogate_key(df_dim, "seller_sk")
    df_dim = df_dim.select(
        "seller_sk",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_region",
        "seller_latitude",
        "seller_longitude",
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_dim, spark, "gold", "dim_seller", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimSeller", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
