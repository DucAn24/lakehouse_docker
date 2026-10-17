"""upsert_with_metrics: incremental MERGE into an Iceberg table; read_changes between snapshots."""

from __future__ import annotations

import pytest
from pyspark.sql.functions import col, current_timestamp, lit

from common.tables import METRICS_TABLE
from common.writers import read_changes, snapshot_ids, upsert_with_metrics

SCHEMA = "id int, name string, qty int"


def _frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA).withColumn("processed_at", current_timestamp())


def _upsert(spark, table, rows, name="unit_upsert"):
    return upsert_with_metrics(_frame(spark, rows), spark, "silver", name, table, ["id"])


def _read(spark, table):
    return {r.id: r for r in spark.table(table).collect()}


def _metric(spark, name):
    rows = spark.table(METRICS_TABLE).filter(col("table_name") == name).orderBy("recorded_at").collect()
    return rows[-1]


def test_first_run_creates_iceberg_v2_table(spark, tmp_table):
    assert _upsert(spark, tmp_table, [(1, "a", 1), (2, "b", 2)], "first_run") == 2
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {tmp_table}").collect()}
    assert props["format-version"] == "2"
    assert len(snapshot_ids(spark, tmp_table)) == 1
    metric = _metric(spark, "first_run")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("overwrite", 2, 0, 0)


def test_rerun_with_same_data_changes_nothing(spark, tmp_table):
    _upsert(spark, tmp_table, [(1, "a", 1), (2, "b", 2)], "same_data")
    before = _read(spark, tmp_table)
    _upsert(spark, tmp_table, [(1, "a", 1), (2, "b", 2)], "same_data")
    after = _read(spark, tmp_table)
    assert {k: v.processed_at for k, v in before.items()} == {k: v.processed_at for k, v in after.items()}
    metric = _metric(spark, "same_data")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("merge", 0, 0, 0)


def test_merge_inserts_updates_and_deletes(spark, tmp_table):
    _upsert(spark, tmp_table, [(1, "a", 1), (2, "b", 2), (3, "c", 3)], "mixed")
    before = _read(spark, tmp_table)
    start = snapshot_ids(spark, tmp_table)[-1]

    row_count = _upsert(spark, tmp_table, [(1, "a", 1), (2, "B", 2), (4, "d", 4)], "mixed")

    after = _read(spark, tmp_table)
    assert row_count == 3
    assert set(after) == {1, 2, 4}
    assert after[2].name == "B"
    assert after[1].processed_at == before[1].processed_at  # untouched row keeps its audit timestamp
    assert after[2].processed_at > before[2].processed_at
    metric = _metric(spark, "mixed")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("merge", 1, 1, 1)

    changes = read_changes(spark, tmp_table, ["id"], start).filter(col("_change_type") != "UPDATE_BEFORE")
    assert {(r.id, r._change_type) for r in changes.collect()} == {(2, "UPDATE_AFTER"), (3, "DELETE"), (4, "INSERT")}


def test_time_travel_reads_the_previous_snapshot(spark, tmp_table):
    _upsert(spark, tmp_table, [(1, "a", 1)], "time_travel")
    first = snapshot_ids(spark, tmp_table)[-1]
    _upsert(spark, tmp_table, [(1, "a", 1), (2, "b", 2)], "time_travel")
    assert spark.read.option("snapshot-id", first).table(tmp_table).count() == 1
    assert spark.table(tmp_table).count() == 2


def test_schema_change_falls_back_to_overwrite(spark, tmp_table):
    _upsert(spark, tmp_table, [(1, "a", 1)], "schema_change")
    widened = _frame(spark, [(1, "a", 1), (2, "b", 2)]).withColumn("extra", lit("x"))
    upsert_with_metrics(widened, spark, "silver", "schema_change", tmp_table, ["id"])
    df = spark.table(tmp_table)
    assert "extra" in df.columns
    assert df.count() == 2
    assert _metric(spark, "schema_change").write_mode == "overwrite"


def test_duplicate_source_keys_fail_and_are_recorded(spark, tmp_table):
    _upsert(spark, tmp_table, [(1, "a", 1)], "dupes")
    with pytest.raises(Exception):
        _upsert(spark, tmp_table, [(1, "a", 1), (1, "z", 9)], "dupes")
    assert _metric(spark, "dupes").status == "failed"
    assert [(r.id, r.name) for r in spark.table(tmp_table).collect()] == [(1, "a")]
