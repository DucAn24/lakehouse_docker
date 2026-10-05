import sys
from datetime import datetime

from pyspark.sql.functions import col, current_timestamp
from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_click_product")
GOLD_PATH = f"{GOLD_BUCKET}/dim_click_product/"


def run(spark):
    df_products = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_products/")

    df_dim = generate_surrogate_key(df_products, "product_sk")
    df_dim = df_dim.select(
        col("product_sk"),
        col("product_id"),
        col("product_category"),
        col("product_name"),
        col("price_usd"),
        col("cost_usd"),
        col("margin_usd"),
        current_timestamp().alias("etl_loaded_at"),
    )

    # Add UNKNOWN row
    unknown_row = [(-1, -1, "UNKNOWN", "UNKNOWN", None, None, None, datetime.now())]
    df_unknown = spark.createDataFrame(unknown_row, schema=df_dim.schema)
    df_final = df_dim.unionByName(df_unknown)

    write_with_metrics(df_final, spark, "gold", "dim_click_product", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimClickProduct", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
