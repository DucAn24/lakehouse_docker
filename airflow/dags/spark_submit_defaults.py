"""
Shared defaults for SparkSubmitOperator tasks in this data-lakehouse.

All Spark jobs need the same Delta / Kafka / S3 JARs and the same credentials
forwarded from Airflow env vars.  Keep
everything here so the DAG file stays readable.
"""

from __future__ import annotations

import os
import socket

# ---------------------------------------------------------------------------
# Spark packages required by every job
# ---------------------------------------------------------------------------
SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.13:4.0.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
        "org.apache.hadoop:hadoop-aws:3.4.1",
        "io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0",
    ]
)

# ---------------------------------------------------------------------------
# Job locations (spark/app is bind-mounted at SPARK_APP_DIR)
#   common/            shared package, imported as `common.*` via PYTHONPATH
#   jobs/<layer>/...   one spark-submit entry point per table
# ---------------------------------------------------------------------------
SPARK_APP_DIR = "/opt/spark/app"
JOBS_DIR = f"{SPARK_APP_DIR}/jobs"
BRONZE_JOBS = f"{JOBS_DIR}/bronze"
SILVER_JOBS = f"{JOBS_DIR}/silver"
GOLD_JOBS = f"{JOBS_DIR}/gold"
OPS_JOBS = f"{JOBS_DIR}/ops"

# ---------------------------------------------------------------------------
# Spark conf overrides applied to every submit
# ---------------------------------------------------------------------------
SPARK_CONF = {
    "spark.driver.host": socket.gethostbyname(socket.gethostname()),
    "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "512m"),
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "1500m"),
    "spark.driver.maxResultSize": os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "256m"),
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.sql.ansi.enabled": "false",
}

# ---------------------------------------------------------------------------
# Env vars forwarded into every spark-submit process
# ---------------------------------------------------------------------------
SPARK_ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "password123"),
    "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "UC_URI": os.environ.get("UC_URI", "http://unitycatalog:8080"),
    "UC_CATALOG": os.environ.get("UC_CATALOG", "lakehouse"),
    "TRINO_HOST": os.environ.get("TRINO_HOST", "trino"),
    "TRINO_PORT": os.environ.get("TRINO_PORT", "8080"),
    "TRINO_USER": os.environ.get("TRINO_USER", "admin"),
    "KAFKA_BROKER": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    # Makes the `common` package importable by the driver. Jobs use no Python UDFs, so
    # executors never import project code; ship common/ via --py-files if that changes.
    "PYTHONPATH": SPARK_APP_DIR,
}

# ---------------------------------------------------------------------------
# Master URL  (Airflow and Spark must share the same Docker network)
# ---------------------------------------------------------------------------
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")


def common_kwargs(app_name: str, application: str, **extra) -> dict:
    """
    Return a dict of SparkSubmitOperator kwargs shared by all tasks.

    Usage:
        SparkSubmitOperator(
            task_id="...",
            **common_kwargs("MyApp", f"{SILVER_JOBS}/olist/olist_orders.py"),
        )
    """
    return {
        "name": app_name,
        "application": application,
        "conn_id": "spark_default",
        "packages": SPARK_PACKAGES,
        "conf": SPARK_CONF,
        "env_vars": SPARK_ENV_VARS,
        "verbose": False,
        **extra,
    }


def dq_kwargs(layer: str, **extra) -> dict:
    """SparkSubmitOperator kwargs for the data quality gate of one layer (bronze | silver | gold).

    The job exits non-zero when a check fails, so the task fails and blocks downstream layers.
    """
    return common_kwargs(
        f"DQ_{layer.capitalize()}",
        f"{OPS_JOBS}/data_quality.py",
        application_args=["--layer", layer],
        **extra,
    )


def register_kwargs(layer: str, **extra) -> dict:
    """SparkSubmitOperator kwargs that register every table of a layer in Unity Catalog + Trino."""
    return common_kwargs(f"{layer.capitalize()}_RegisterTables", f"{JOBS_DIR}/{layer}/register_tables.py", **extra)
