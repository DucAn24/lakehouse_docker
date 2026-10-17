import sys

from pyspark.sql.functions import col, current_timestamp
from common.config import create_spark_session, get_logger, SILVER_BUCKET, GOLD_BUCKET
from common.transforms import generate_surrogate_key, with_unknown_member
from common.writers import write_with_metrics

logger = get_logger("s2g.dim_traffic_source")
GOLD_PATH = f"{GOLD_BUCKET}/dim_traffic_source/"


def run(spark):
    df_sessions = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_sessions/")
    df_orders = spark.read.format("delta").load(f"{SILVER_BUCKET}/click_orders/")

    # Get distinct sources
    df_src_sess = df_sessions.select(col("source")).filter(col("source").isNotNull())
    df_src_ord = df_orders.select(col("source")).filter(col("source").isNotNull())

    df_union = df_src_sess.union(df_src_ord).distinct()

    df_dim = generate_surrogate_key(df_union, "source_sk")
    df_dim = df_dim.select(
        col("source_sk"), col("source").alias("traffic_source"), current_timestamp().alias("etl_loaded_at")
    )

    df_final = with_unknown_member(
        df_dim,
        {"source_sk": -1, "traffic_source": "UNKNOWN"},
    )

    write_with_metrics(df_final, spark, "gold", "dim_traffic_source", GOLD_PATH)


if __name__ == "__main__":
    spark = create_spark_session("S2G_DimTrafficSource", f"{GOLD_BUCKET}/")
    try:
        run(spark)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
