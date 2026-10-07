"""Delta writers that record pipeline metrics.

``write_with_metrics`` rewrites a table; ``upsert_with_metrics`` MERGEs into it (silver) and keeps
the Delta Change Data Feed on so downstream jobs can read row-level changes with ``read_changes``.
"""

from __future__ import annotations

import logging
import time

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, xxhash64

from common.metrics import record_metric

logger = logging.getLogger(__name__)


def write_with_metrics(
    df: DataFrame,
    spark: SparkSession,
    layer: str,
    table_name: str,
    path: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> int:
    """Write a DataFrame to Delta and record execution metrics.

    Returns the row count written.
    """
    start = time.time()
    row_count = -1
    try:
        writer = df.write.format("delta").mode(mode)
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        writer.save(path)

        row_count = df.count()
        duration = round(time.time() - start, 2)

        record_metric(spark, layer, table_name, row_count, duration, "success")
        logger.info(
            "Wrote %s.%s — %d rows in %.1fs",
            layer,
            table_name,
            row_count,
            duration,
        )
    except Exception as exc:
        duration = round(time.time() - start, 2)
        record_metric(spark, layer, table_name, 0, duration, "failed", str(exc))
        raise

    return row_count


# Columns that change on every run without the row's content changing; excluded from change detection.
AUDIT_COLUMNS = ("processed_at",)

CDF_PROPERTY = "delta.enableChangeDataFeed"


def _signature(df: DataFrame) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def _enable_change_feed(spark: SparkSession, path: str) -> None:
    props = DeltaTable.forPath(spark, path).detail().first()["properties"] or {}
    if props.get(CDF_PROPERTY) != "true":
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({CDF_PROPERTY} = true)")


def upsert_with_metrics(
    df: DataFrame,
    spark: SparkSession,
    layer: str,
    table_name: str,
    path: str,
    keys: list[str],
) -> int:
    """Sync ``df`` (the full desired table state) into the Delta table at ``path`` and record metrics.

    Rows are matched on ``keys``: new keys are inserted, rows whose content changed are updated
    (audit columns such as ``processed_at`` are ignored for that comparison, so an unchanged row
    keeps its old ``processed_at`` and is not rewritten), and keys missing from ``df`` are deleted.
    With the change feed enabled this yields a row-level insert/update/delete history.

    A missing table or a changed schema falls back to a full overwrite (the new schema replaces the old).
    Returns the row count of the table after the write.
    """
    start = time.time()
    # The merge scans the source more than once; pin it so non-deterministic columns
    # (current_timestamp) are identical in each scan.
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        counts: dict[str, int | None] = {"inserted": None, "updated": None, "deleted": None}
        mode = "merge"
        if DeltaTable.isDeltaTable(spark, path) and _signature(spark.read.format("delta").load(path)) == _signature(df):
            _enable_change_feed(spark, path)
            counts = _merge(spark, df, path, keys)
        else:
            mode = "overwrite"
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
            _enable_change_feed(spark, path)

        row_count = spark.read.format("delta").load(path).count()
        if mode == "overwrite":
            counts = {"inserted": row_count, "updated": 0, "deleted": 0}
        duration = round(time.time() - start, 2)
        record_metric(
            spark,
            layer,
            table_name,
            row_count,
            duration,
            "success",
            write_mode=mode,
            rows_inserted=counts["inserted"],
            rows_updated=counts["updated"],
            rows_deleted=counts["deleted"],
        )
        logger.info(
            "%s %s.%s — %d rows (+%s ~%s -%s) in %.1fs",
            mode,
            layer,
            table_name,
            row_count,
            counts["inserted"],
            counts["updated"],
            counts["deleted"],
            duration,
        )
        return row_count
    except Exception as exc:
        record_metric(spark, layer, table_name, 0, round(time.time() - start, 2), "failed", str(exc), write_mode="merge")
        raise
    finally:
        df.unpersist()


def _merge(spark: SparkSession, df: DataFrame, path: str, keys: list[str]) -> dict[str, int | None]:
    tracked = [c for c in df.columns if c not in AUDIT_COLUMNS]
    on_keys = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    changed = xxhash64(*[col(f"t.`{c}`") for c in tracked]) != xxhash64(*[col(f"s.`{c}`") for c in tracked])
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), on_keys)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    metrics = target.history(1).first()["operationMetrics"] or {}

    def count(name: str) -> int | None:
        value = metrics.get(name)
        return int(value) if value is not None else None

    return {
        "inserted": count("numTargetRowsInserted"),
        "updated": count("numTargetRowsUpdated"),
        "deleted": count("numTargetRowsDeleted"),
    }


def read_changes(spark: SparkSession, path: str, starting_version: int, ending_version: int | None = None) -> DataFrame:
    """Row-level changes of a table written by ``upsert_with_metrics`` since ``starting_version``.

    Adds Delta's ``_change_type`` (insert | update_preimage | update_postimage | delete),
    ``_commit_version`` and ``_commit_timestamp`` columns. The feed starts at the version where
    it was enabled (the initial overwrite carries no change records).
    """
    reader = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", starting_version)
    if ending_version is not None:
        reader = reader.option("endingVersion", ending_version)
    return reader.load(path)
