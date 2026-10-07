import sys

from pyspark.sql.functions import (
    col,
    trim,
    when,
    current_timestamp,
    coalesce,
    broadcast,
)
from pyspark.sql.types import IntegerType

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.products")

BRONZE_PRODUCTS_PATH = f"{BRONZE_BUCKET}/olist.public.olist_products/"
BRONZE_CATEGORY_PATH = f"{BRONZE_BUCKET}/olist.public.product_category_translation/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_products/"


def _build_category_lookup(spark):
    df = spark.read.format("delta").load(BRONZE_CATEGORY_PATH)
    df_clean = extract_cdc_latest(df, key_cols=["product_category_name"])
    return df_clean.select(
        trim(col("product_category_name")).alias("cat_name"),
        trim(col("product_category_name_english")).alias("product_category_name_english"),
    ).filter(col("cat_name").isNotNull())


def run(spark):
    df = spark.read.format("delta").load(BRONZE_PRODUCTS_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["product_id"])

    df_products = df_dedup.select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_category_name")).alias("product_category_name"),
        col("product_name_length").cast(IntegerType()).alias("product_name_length"),
        col("product_description_length").cast(IntegerType()).alias("product_description_length"),
        col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        col("product_weight_g").cast(IntegerType()).alias("product_weight_g"),
        col("product_length_cm").cast(IntegerType()).alias("product_length_cm"),
        col("product_height_cm").cast(IntegerType()).alias("product_height_cm"),
        col("product_width_cm").cast(IntegerType()).alias("product_width_cm"),
    ).filter(col("product_id").isNotNull())

    df_category = _build_category_lookup(spark)

    df_joined = df_products.join(
        broadcast(df_category),
        df_products["product_category_name"] == df_category["cat_name"],
        "left",
    ).drop("cat_name")

    df_silver = df_joined.select(
        col("product_id"),
        col("product_category_name"),
        coalesce(col("product_category_name_english"), col("product_category_name")).alias(
            "product_category_name_english"
        ),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        when(
            col("product_length_cm").isNotNull()
            & col("product_height_cm").isNotNull()
            & col("product_width_cm").isNotNull(),
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm"),
        )
        .otherwise(None)
        .alias("product_volume_cm3"),
        current_timestamp().alias("processed_at"),
    )

    upsert_with_metrics(df_silver, spark, "silver", "olist_products", SILVER_PATH, SILVER_KEYS["olist_products"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_Products", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
