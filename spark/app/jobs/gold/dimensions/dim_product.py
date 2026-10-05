import sys

from pyspark.sql.functions import col, when, current_timestamp

from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_product")

GOLD_PATH = f"{GOLD_BUCKET}/dim_product/"


def run(spark):
    df_products = spark.read.format("delta").load(f"{SILVER_BUCKET}/olist_products/")

    df_dim = df_products.select(
        col("product_id"),
        col("product_category_name"),
        col("product_category_name_english"),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        col("product_volume_cm3"),
        when(col("product_volume_cm3") < 1000, "SMALL")
        .when(col("product_volume_cm3") < 10000, "MEDIUM")
        .when(col("product_volume_cm3") < 50000, "LARGE")
        .when(col("product_volume_cm3") >= 50000, "EXTRA_LARGE")
        .otherwise("UNKNOWN")
        .alias("size_category"),
        when(col("product_weight_g") < 500, "LIGHT")
        .when(col("product_weight_g") < 2000, "MEDIUM")
        .when(col("product_weight_g") < 10000, "HEAVY")
        .when(col("product_weight_g") >= 10000, "VERY_HEAVY")
        .otherwise("UNKNOWN")
        .alias("weight_category"),
    )

    df_dim = generate_surrogate_key(df_dim, "product_sk")
    df_dim = df_dim.select(
        "product_sk",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "size_category",
        "weight_category",
        current_timestamp().alias("etl_loaded_at"),
    )

    write_with_metrics(df_dim, spark, "gold", "dim_product", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimProduct", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
