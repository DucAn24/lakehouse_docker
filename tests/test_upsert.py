"""upsert_with_metrics: incremental MERGE into a Delta table with the change feed on."""

from __future__ import annotations

import pytest
from pyspark.sql.functions import col, current_timestamp, lit

from common.config import METRICS_PATH
from common.writers import read_changes, upsert_with_metrics

SCHEMA = "id int, name string, qty int"


def _frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA).withColumn("processed_at", current_timestamp())


def _upsert(spark, path, rows, table="unit_upsert"):
    return upsert_with_metrics(_frame(spark, rows), spark, "silver", table, path, ["id"])


def _read(spark, path):
    return {r.id: r for r in spark.read.format("delta").load(path).collect()}


def _metric(spark, table):
    rows = spark.read.format("delta").load(METRICS_PATH).filter(col("table_name") == table).orderBy("recorded_at").collect()
    return rows[-1]


def test_first_run_creates_table_with_change_feed(spark, tmp_delta):
    assert _upsert(spark, tmp_delta, [(1, "a", 1), (2, "b", 2)], "first_run") == 2
    props = spark.sql(f"DESCRIBE DETAIL delta.`{tmp_delta}`").first()["properties"]
    assert props["delta.enableChangeDataFeed"] == "true"
    metric = _metric(spark, "first_run")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("overwrite", 2, 0, 0)


def test_rerun_with_same_data_changes_nothing(spark, tmp_delta):
    _upsert(spark, tmp_delta, [(1, "a", 1), (2, "b", 2)], "same_data")
    before = _read(spark, tmp_delta)
    _upsert(spark, tmp_delta, [(1, "a", 1), (2, "b", 2)], "same_data")
    after = _read(spark, tmp_delta)
    assert {k: v.processed_at for k, v in before.items()} == {k: v.processed_at for k, v in after.items()}
    metric = _metric(spark, "same_data")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("merge", 0, 0, 0)


def test_merge_inserts_updates_and_deletes(spark, tmp_delta):
    _upsert(spark, tmp_delta, [(1, "a", 1), (2, "b", 2), (3, "c", 3)], "mixed")
    before = _read(spark, tmp_delta)
    start = spark.sql(f"DESCRIBE HISTORY delta.`{tmp_delta}`").agg({"version": "max"}).first()[0] + 1

    row_count = _upsert(spark, tmp_delta, [(1, "a", 1), (2, "B", 2), (4, "d", 4)], "mixed")

    after = _read(spark, tmp_delta)
    assert row_count == 3
    assert set(after) == {1, 2, 4}
    assert after[2].name == "B"
    assert after[1].processed_at == before[1].processed_at  # untouched row keeps its audit timestamp
    assert after[2].processed_at > before[2].processed_at
    metric = _metric(spark, "mixed")
    assert (metric.write_mode, metric.rows_inserted, metric.rows_updated, metric.rows_deleted) == ("merge", 1, 1, 1)

    changes = read_changes(spark, tmp_delta, start).filter(col("_change_type") != "update_preimage")
    assert {(r.id, r._change_type) for r in changes.collect()} == {(2, "update_postimage"), (3, "delete"), (4, "insert")}


def test_schema_change_falls_back_to_overwrite(spark, tmp_delta):
    _upsert(spark, tmp_delta, [(1, "a", 1)], "schema_change")
    widened = _frame(spark, [(1, "a", 1), (2, "b", 2)]).withColumn("extra", lit("x"))
    upsert_with_metrics(widened, spark, "silver", "schema_change", tmp_delta, ["id"])
    df = spark.read.format("delta").load(tmp_delta)
    assert "extra" in df.columns
    assert df.count() == 2
    assert _metric(spark, "schema_change").write_mode == "overwrite"


def test_duplicate_source_keys_fail_and_are_recorded(spark, tmp_delta):
    _upsert(spark, tmp_delta, [(1, "a", 1)], "dupes")
    with pytest.raises(Exception):
        _upsert(spark, tmp_delta, [(1, "a", 1), (1, "z", 9)], "dupes")
    assert _metric(spark, "dupes").status == "failed"
    assert [(r.id, r.name) for r in spark.read.format("delta").load(tmp_delta).collect()] == [(1, "a")]
