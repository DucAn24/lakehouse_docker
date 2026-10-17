"""Iceberg catalog helpers (Polaris REST catalog `lakehouse`) + a minimal Trino client.

Spark, Trino/dbt and every other Iceberg client share one catalog, so a table exists for all of
them as soon as it is written — there is no registration step.
"""

from __future__ import annotations

import json
import logging
import urllib.request

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from common.config import TRINO_URL, TRINO_USER

logger = logging.getLogger(__name__)

# Iceberg spec v2: readable and writable by every engine in the stack (Trino, PyIceberg, DuckDB)
TABLE_PROPERTIES = {"format-version": "2"}


def trino_execute(sql: str, url: str = TRINO_URL) -> list[list]:
    """Run one statement via Trino's REST protocol and return all result rows."""
    request = urllib.request.Request(
        f"{url}/v1/statement",
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


def ensure_namespace(spark: SparkSession, table: str) -> None:
    """Create the namespace of `<catalog>.<namespace>.<table>` if missing.

    polaris-init creates the standard namespaces up front; this keeps fresh catalogs (tests) working.
    """
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def create_or_replace(df: DataFrame, table: str, properties: dict[str, str] | None = None) -> None:
    """Write `df` as the new content and schema of `table` (a new snapshot if it already exists)."""
    ensure_namespace(df.sparkSession, table)
    writer = df.writeTo(table).using("iceberg")
    for key, value in {**TABLE_PROPERTIES, **(properties or {})}.items():
        writer = writer.tableProperty(key, value)
    writer.createOrReplace()


def append_rows(df: DataFrame, table: str) -> None:
    """Append `df` to `table`, creating it on first write; new columns in `df` are added to the table."""
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        ensure_namespace(spark, table)
        writer = df.writeTo(table).using("iceberg")
        for key, value in {**TABLE_PROPERTIES, "write.spark.accept-any-schema": "true"}.items():
            writer = writer.tableProperty(key, value)
        try:
            writer.create()
            return
        except AnalysisException:
            # Created concurrently by another job: fall through to append
            logger.info("%s was created concurrently, appending", table)
    df.writeTo(table).option("mergeSchema", "true").append()
