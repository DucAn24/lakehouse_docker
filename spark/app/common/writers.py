"""Iceberg writers that record pipeline metrics.

``upsert_with_metrics`` MERGEs the full desired state into a silver table; every write is an Iceberg
snapshot, so downstream jobs can read row-level changes between two snapshots with ``read_changes``.
"""

from __future__ import annotations

import logging
import time
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, xxhash64

from common.catalog import create_or_replace
from common.metrics import record_metric

logger = logging.getLogger(__name__)

# Columns that change on every run without the row's content changing; excluded from change detection.
AUDIT_COLUMNS = ("processed_at",)


def _signature(df: DataFrame) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def upsert_with_metrics(
    df: DataFrame,
    spark: SparkSession,
    layer: str,
    table_name: str,
    table: str,
    keys: list[str],
) -> int:
    """Sync ``df`` (the full desired table state) into the Iceberg table ``table`` and record metrics.

    Rows are matched on ``keys``: new keys are inserted, rows whose content changed are updated
    (audit columns such as ``processed_at`` are ignored for that comparison, so an unchanged row
    keeps its old ``processed_at`` and is not rewritten), and keys missing from ``df`` are deleted.
    Each write is one snapshot, so ``read_changes`` yields a row-level insert/update/delete history.

    A missing table or a changed schema falls back to a full overwrite (the new schema replaces the old).
    Returns the row count of the table after the write.
    """
    start = time.time()
    # The merge and its counters scan the source more than once; pin it so non-deterministic
    # columns (current_timestamp) are identical in each scan.
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if spark.catalog.tableExists(table) and _signature(spark.table(table)) == _signature(df):
            mode = "merge"
            counts = _merge(spark, df, table, table_name, keys)
        else:
            mode = "overwrite"
            create_or_replace(df, table)

        row_count = spark.table(table).count()
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


def _merge(spark: SparkSession, df: DataFrame, table: str, table_name: str, keys: list[str]) -> dict[str, int]:
    tracked = [c for c in df.columns if c not in AUDIT_COLUMNS]

    # Iceberg's MERGE reports no inserted/updated/deleted split, so count against the current
    # snapshot first (each silver table has a single writer, so nothing changes in between).
    source, target = df.alias("s"), spark.table(table).alias("t")
    on: Column = reduce(lambda a, b: a & b, [col(f"s.`{k}`").eqNullSafe(col(f"t.`{k}`")) for k in keys])
    changed = xxhash64(*[col(f"t.`{c}`") for c in tracked]) != xxhash64(*[col(f"s.`{c}`") for c in tracked])
    counts = {
        "inserted": source.join(target, on, "left_anti").count(),
        "updated": source.join(target, on, "inner").filter(changed).count(),
        "deleted": target.join(source, on, "left_anti").count(),
    }

    view = f"_upsert_source_{table_name}"
    df.createOrReplaceTempView(view)
    on_sql = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    hash_t = ", ".join(f"t.`{c}`" for c in tracked)
    hash_s = ", ".join(f"s.`{c}`" for c in tracked)
    try:
        spark.sql(f"""
            MERGE INTO {table} t
            USING {view} s
            ON {on_sql}
            WHEN MATCHED AND xxhash64({hash_t}) <> xxhash64({hash_s}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(view)
    return counts


def snapshot_ids(spark: SparkSession, table: str) -> list[int]:
    """Snapshot ids of ``table``, oldest first."""
    return [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()]


def read_changes(
    spark: SparkSession,
    table: str,
    keys: list[str],
    start_snapshot_id: int,
    end_snapshot_id: int | None = None,
) -> DataFrame:
    """Row-level changes of a table written by ``upsert_with_metrics`` after ``start_snapshot_id``
    (exclusive) up to ``end_snapshot_id`` (inclusive, default: the current snapshot).

    Built on Iceberg's ``create_changelog_view``: adds ``_change_type`` (INSERT | DELETE |
    UPDATE_BEFORE | UPDATE_AFTER, updates paired on ``keys``), ``_change_ordinal`` and
    ``_commit_snapshot_id``. Rows a copy-on-write rewrite carried over unchanged are dropped.
    """
    catalog, name = table.split(".", 1)
    options = {"start-snapshot-id": str(start_snapshot_id)}
    if end_snapshot_id is not None:
        options["end-snapshot-id"] = str(end_snapshot_id)
    options_sql = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    keys_sql = ", ".join(f"'{k}'" for k in keys)
    view = f"_changes_{name.replace('.', '_')}"
    spark.sql(f"""
        CALL {catalog}.system.create_changelog_view(
            table => '{name}',
            options => map({options_sql}),
            changelog_view => '{view}',
            identifier_columns => array({keys_sql}),
            compute_updates => true
        )
    """)
    return spark.table(view)
