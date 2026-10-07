import sys

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    when,
    avg,
    round as spark_round,
    row_number,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from common.config import create_spark_session, get_logger, BRONZE_BUCKET, SILVER_BUCKET
from common.transforms import extract_cdc_latest
from common.tables import SILVER_KEYS
from common.writers import upsert_with_metrics

logger = get_logger("b2s.customers")

BRONZE_CUSTOMERS_PATH = f"{BRONZE_BUCKET}/olist.public.olist_customers/"
BRONZE_GEO_PATH = f"{BRONZE_BUCKET}/olist.public.olist_geolocation/"
SILVER_PATH = f"{SILVER_BUCKET}/olist_customers/"

REGION_MAP = {
    "SP": "SUDESTE",
    "RJ": "SUDESTE",
    "MG": "SUDESTE",
    "ES": "SUDESTE",
    "PR": "SUL",
    "SC": "SUL",
    "RS": "SUL",
    "MT": "CENTRO-OESTE",
    "MS": "CENTRO-OESTE",
    "GO": "CENTRO-OESTE",
    "DF": "CENTRO-OESTE",
    "BA": "NORDESTE",
    "SE": "NORDESTE",
    "AL": "NORDESTE",
    "PE": "NORDESTE",
    "PB": "NORDESTE",
    "RN": "NORDESTE",
    "CE": "NORDESTE",
    "PI": "NORDESTE",
    "MA": "NORDESTE",
    "AM": "NORTE",
    "PA": "NORTE",
    "AC": "NORTE",
    "RO": "NORTE",
    "RR": "NORTE",
    "AP": "NORTE",
    "TO": "NORTE",
}


def _build_geo_lookup(spark):
    df = spark.read.format("delta").load(BRONZE_GEO_PATH)
    df_clean = df.filter(col("op") != "d").select("after.*")

    df_typed = df_clean.select(
        trim(col("geolocation_zip_code_prefix")).alias("zip_code_prefix"),
        col("geolocation_lat").cast(DoubleType()).alias("latitude"),
        col("geolocation_lng").cast(DoubleType()).alias("longitude"),
        upper(trim(col("geolocation_city"))).alias("geo_city"),
        upper(trim(col("geolocation_state"))).alias("geo_state"),
    ).filter(
        col("zip_code_prefix").isNotNull() & col("latitude").between(-90, 90) & col("longitude").between(-180, 180)
    )

    df_agg = df_typed.groupBy("zip_code_prefix", "geo_city", "geo_state").agg(
        spark_round(avg("latitude"), 6).alias("avg_latitude"),
        spark_round(avg("longitude"), 6).alias("avg_longitude"),
    )

    w = Window.partitionBy("zip_code_prefix").orderBy(col("avg_latitude"))
    return df_agg.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")


def run(spark):
    df = spark.read.format("delta").load(BRONZE_CUSTOMERS_PATH)
    df_dedup = extract_cdc_latest(df, key_cols=["customer_id"])

    df_customers = df_dedup.select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_unique_id")).alias("customer_unique_id"),
        trim(col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        upper(trim(col("customer_city"))).alias("customer_city"),
        upper(trim(col("customer_state"))).alias("customer_state"),
    ).filter(col("customer_id").isNotNull() & (col("customer_id") != ""))

    df_geo = _build_geo_lookup(spark)

    df_joined = df_customers.join(
        df_geo,
        df_customers["customer_zip_code_prefix"] == df_geo["zip_code_prefix"],
        "left",
    ).drop("zip_code_prefix", "geo_city", "geo_state")

    region_expr = when(col("customer_state") == "SP", "SUDESTE")
    for state, region in REGION_MAP.items():
        if state != "SP":
            region_expr = region_expr.when(col("customer_state") == state, region)
    region_expr = region_expr.otherwise("OUTROS")

    df_silver = df_joined.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state"),
        region_expr.alias("customer_region"),
        col("avg_latitude").alias("customer_latitude"),
        col("avg_longitude").alias("customer_longitude"),
        current_timestamp().alias("processed_at"),
    )

    upsert_with_metrics(df_silver, spark, "silver", "olist_customers", SILVER_PATH, SILVER_KEYS["olist_customers"])


if __name__ == "__main__":
    spark = create_spark_session("B2S_Customers", f"{SILVER_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
