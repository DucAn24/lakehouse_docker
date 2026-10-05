"""Catalog registration: Unity Catalog (Spark) + Trino file metastore.

Spark writes Delta by path; each layer's ``jobs/<layer>/register_tables.py`` then
registers every table of ``common.tables.TABLES`` as an EXTERNAL table in both catalogs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.request

from pyspark.sql import SparkSession

from common.config import (
    TRINO_DELTA_CATALOG,
    TRINO_URL,
    TRINO_USER,
    UC_CATALOG,
    create_spark_session,
    get_logger,
)
from common.tables import LAYERS, select_tables

logger = logging.getLogger(__name__)


def _to_s3_uri(path: str) -> str:
    """Normalize s3a://bucket/x/ to s3://bucket/x (the only S3 scheme Unity Catalog accepts)."""
    return "s3://" + path.split("://", 1)[1].rstrip("/")


def trino_execute(sql: str) -> list[list]:
    """Run one statement via Trino's REST protocol and return all result rows."""
    request = urllib.request.Request(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode(),
        headers={"X-Trino-User": TRINO_USER},
        method="POST",
    )
    rows: list[list] = []
    while True:
        with urllib.request.urlopen(request, timeout=60) as resp:
            payload = json.load(resp)
        if "error" in payload:
            raise RuntimeError(f"Trino query failed: {payload['error'].get('message')} -- {sql}")
        rows.extend(payload.get("data", []))
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        request = urllib.request.Request(next_uri, headers={"X-Trino-User": TRINO_USER})


def register_catalog_tables(
    spark: SparkSession,
    database: str,
    tables_config: dict[str, str],
) -> tuple[int, int]:
    """Register Delta tables in Unity Catalog (Spark) and Trino's file metastore.

    UC tables are dropped and recreated so their column metadata follows schema
    changes (DROP on an external table keeps the data). Trino reads the schema
    from the Delta log at query time, so only missing tables are registered there.

    Returns (registered_count, failed_count).
    """
    uc_schema = f"{UC_CATALOG}.{database}"
    trino_schema = f"{TRINO_DELTA_CATALOG}.{database}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {uc_schema}")
    trino_execute(f"CREATE SCHEMA IF NOT EXISTS {trino_schema}")
    trino_tables = {row[0] for row in trino_execute(f"SHOW TABLES FROM {trino_schema}")}

    registered, failed = 0, 0
    for table_name, path in tables_config.items():
        location = _to_s3_uri(path)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {uc_schema}.{table_name}")
            spark.sql(f"""
                CREATE TABLE {uc_schema}.{table_name}
                USING DELTA
                LOCATION '{location}'
            """)
            if table_name not in trino_tables:
                trino_execute(f"""
                    CALL {TRINO_DELTA_CATALOG}.system.register_table(
                        schema_name => '{database}',
                        table_name => '{table_name}',
                        table_location => '{location}'
                    )
                """)
            logger.info("Registered %s.%s -> %s", database, table_name, location)
            registered += 1
        except Exception:
            logger.exception("Failed to register %s.%s", database, table_name)
            failed += 1

    spark.sql(f"SHOW TABLES IN {uc_schema}").show(truncate=False)
    logger.info(
        "Catalog registration for [%s]: %d success, %d failed",
        database,
        registered,
        failed,
    )
    return registered, failed


def main(layer: str) -> None:
    """CLI entry for jobs/<layer>/register_tables.py: register all (or --table) tables of a layer."""
    assert layer in LAYERS, layer
    log = get_logger(f"{layer}.register_tables")
    parser = argparse.ArgumentParser(description=f"Register {layer} Delta tables in Unity Catalog + Trino")
    parser.add_argument("--table", help=f"Register only this single {layer} table")
    args = parser.parse_args()

    try:
        targets = select_tables(layer, args.table)
    except KeyError as exc:
        log.error("%s", exc)
        sys.exit(1)

    spark = create_spark_session(f"{layer.capitalize()}_RegisterTables", f"s3a://{layer}/")
    try:
        _, failed = register_catalog_tables(spark, layer, targets)
        if failed:
            sys.exit(1)
        log.info("%s catalog registration completed", layer.capitalize())
    except Exception:
        log.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
