import os
import logging

from pyspark.sql import SparkSession

S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKER", "kafka:9092")

# Unity Catalog (Spark catalog) + Trino (file metastore) — see common.catalog
UC_URI = os.environ.get("UC_URI", "http://unitycatalog:8080")
UC_CATALOG = os.environ.get("UC_CATALOG", "lakehouse")
TRINO_URL = f"http://{os.environ.get('TRINO_HOST', 'trino')}:{os.environ.get('TRINO_PORT', '8080')}"
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_DELTA_CATALOG = "delta"

# Overridable so tests can run the whole pipeline against a local directory
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "s3a://bronze")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "s3a://silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "s3a://gold")

# Pipeline observability paths
METRICS_PATH = f"{GOLD_BUCKET}/_pipeline_metrics/"
QUALITY_LOG_PATH = f"{SILVER_BUCKET}/_data_quality_log/"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    )
    return logging.getLogger(name)


def create_spark_session(app_name: str, warehouse_dir: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        # Unity Catalog stores locations as s3:// — serve that scheme with S3A too
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        # Spark 4 turns ANSI on by default; keep Spark 3 semantics (bad casts -> NULL, not errors)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "10") # Set shuffle partitions to 10 for local cluster (prevents OOM and 200 small files)
        .config(f"spark.sql.catalog.{UC_CATALOG}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{UC_CATALOG}.uri", UC_URI)
        .config(f"spark.sql.catalog.{UC_CATALOG}.token", "")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
