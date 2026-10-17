import os
import logging

from pyspark.sql import SparkSession

S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://s3:8333")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKER", "kafka:9092")

# Iceberg catalog shared by Spark, Trino and any Iceberg REST client: Apache Polaris, catalog `lakehouse`.
# Every table is addressed as <CATALOG>.<layer>.<table> (see common.tables).
CATALOG = os.environ.get("ICEBERG_CATALOG", "lakehouse")
TRINO_URL = f"http://{os.environ.get('TRINO_HOST', 'trino')}:{os.environ.get('TRINO_PORT', '8080')}"
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "lakehouse")


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    )
    return logging.getLogger(name)


def iceberg_catalog_conf() -> dict[str, str]:
    """Spark conf of the `CATALOG` Iceberg catalog, read from the environment at call time.

    ICEBERG_CATALOG_TYPE=rest (default): Polaris REST catalog (POLARIS_URI, POLARIS_CLIENT_ID/SECRET),
    data files on S3 (SeaweedFS) via Iceberg's S3FileIO with this process's own S3 keys (no credential vending).
    ICEBERG_CATALOG_TYPE=hadoop: a file-based catalog under ICEBERG_WAREHOUSE, used by the unit tests.
    """
    env = os.environ
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        # always load the current snapshot: other engines (Trino/dbt) commit to the same tables
        f"{prefix}.cache-enabled": "false",
    }
    if env.get("ICEBERG_CATALOG_TYPE", "rest") == "hadoop":
        conf[f"{prefix}.type"] = "hadoop"
        conf[f"{prefix}.warehouse"] = env["ICEBERG_WAREHOUSE"]
        return conf
    conf.update(
        {
            f"{prefix}.type": "rest",
            f"{prefix}.uri": env.get("POLARIS_URI", "http://polaris:8181/api/catalog"),
            f"{prefix}.warehouse": CATALOG,
            f"{prefix}.credential": f"{env.get('POLARIS_CLIENT_ID', 'root')}:{env.get('POLARIS_CLIENT_SECRET', 's3cr3t')}",
            f"{prefix}.scope": "PRINCIPAL_ROLE:ALL",
            f"{prefix}.token-refresh-enabled": "true",
            f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{prefix}.s3.endpoint": env.get("S3_ENDPOINT", S3_ENDPOINT),
            f"{prefix}.s3.path-style-access": "true",
            f"{prefix}.s3.access-key-id": env.get("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY),
            f"{prefix}.s3.secret-access-key": env.get("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY),
            f"{prefix}.client.region": env.get("AWS_REGION", S3_REGION),
        }
    )
    return conf


def create_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # S3A for Hadoop-side listing of s3:// table locations (Iceberg remove_orphan_files);
        # table data itself goes through the catalog's S3FileIO
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        # Spark 4 turns ANSI on by default; keep Spark 3 semantics (bad casts -> NULL, not errors)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "10") # Set shuffle partitions to 10 for local cluster (prevents OOM and 200 small files)
    )
    for key, value in iceberg_catalog_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
