"""common.transforms and common.writers / common.metrics against a local Spark + Delta."""

from __future__ import annotations

import pytest
from pyspark.sql.functions import col, date_format

from common.metrics import record_metric
from common.transforms import extract_cdc_latest, generate_surrogate_key, safe_to_timestamp
from common.writers import write_with_metrics


def _cdc(spark, rows):
    """rows: (op, ts_ms, id, value) -> Debezium-like frame with `before` (deletes only) and `after`."""
    return spark.createDataFrame(
        [(op, ts, (i, None) if op == "d" else None, (i, v) if op != "d" else None) for op, ts, i, v in rows],
        "op string, ts_ms long, before struct<id:string,value:string>, after struct<id:string,value:string>",
    )


def test_safe_to_timestamp_handles_epoch_ms_and_text(spark):
    df = spark.createDataFrame(
        [("1704103200000",), ("2024-01-01 10:00:00",), (None,), ("garbage",)], "raw string"
    ).select(date_format(safe_to_timestamp(col("raw")), "yyyy-MM-dd HH:mm:ss").alias("ts"))
    ts = [r.ts for r in df.collect()]
    assert ts[0] == ts[1] == "2024-01-01 10:00:00"
    assert ts[2] is None
    assert ts[3] is None  # spark.sql.ansi.enabled=false: a bad cast is NULL, not an error


def test_extract_cdc_latest_keeps_newest_version(spark):
    df = _cdc(spark, [("c", 1, "a", "old"), ("u", 5, "a", "new"), ("r", 1, "b", "only")])
    out = extract_cdc_latest(df, key_cols=["id"])
    assert {r.id: r.value for r in out.collect()} == {"a": "new", "b": "only"}
    assert "ts_ms" not in out.columns


def test_extract_cdc_latest_removes_deleted_keys(spark):
    df = _cdc(spark, [("r", 1, "a", "v"), ("r", 1, "b", "v"), ("d", 9, "b", None)])
    assert {r.id for r in extract_cdc_latest(df, key_cols=["id"]).collect()} == {"a"}


def test_extract_cdc_latest_reinsert_after_delete_wins(spark):
    df = _cdc(spark, [("r", 1, "a", "v1"), ("d", 2, "a", None), ("c", 3, "a", "v2")])
    assert [(r.id, r.value) for r in extract_cdc_latest(df, key_cols=["id"]).collect()] == [("a", "v2")]


def test_extract_cdc_latest_without_before_column_only_filters_deletes(spark):
    df = _cdc(spark, [("r", 1, "a", "v"), ("d", 9, "b", None)]).drop("before")
    assert {r.id for r in extract_cdc_latest(df, key_cols=["id"]).collect()} == {"a"}


def test_extract_cdc_latest_one_row_per_key(spark):
    df = _cdc(spark, [("c", 1, "a", "v1"), ("u", 2, "a", "v2"), ("u", 3, "a", "v3")])
    rows = extract_cdc_latest(df, key_cols=["id"]).collect()
    assert [(r.id, r.value) for r in rows] == [("a", "v3")]


def test_generate_surrogate_key_is_unique_and_starts_at_one(spark):
    df = generate_surrogate_key(spark.range(50).toDF("n"), "sk")
    keys = [r.sk for r in df.collect()]
    assert len(set(keys)) == 50
    assert min(keys) >= 1


def test_write_with_metrics_writes_data_and_records_success(spark, tmp_delta):
    df = spark.createDataFrame([(1,), (2,), (3,)], "n int")
    assert write_with_metrics(df, spark, "silver", "unit_table", tmp_delta) == 3
    assert spark.read.format("delta").load(tmp_delta).count() == 3

    from common.config import METRICS_PATH

    metric = (
        spark.read.format("delta")
        .load(METRICS_PATH)
        .filter((col("table_name") == "unit_table") & (col("status") == "success"))
        .collect()
    )
    assert [m.row_count for m in metric] == [3]


def test_write_with_metrics_overwrites_on_rerun(spark, tmp_delta):
    write_with_metrics(spark.createDataFrame([(1,), (2,)], "n int"), spark, "silver", "rerun", tmp_delta)
    write_with_metrics(spark.createDataFrame([(9,)], "n int"), spark, "silver", "rerun", tmp_delta)
    assert [r.n for r in spark.read.format("delta").load(tmp_delta).collect()] == [9]


def test_write_with_metrics_records_failure_and_reraises(spark, tmp_delta):
    class Boom(Exception):
        pass

    class BrokenFrame:
        """Stands in for a DataFrame whose write fails."""

        @property
        def write(self):
            raise Boom("disk full")

    with pytest.raises(Boom):
        write_with_metrics(BrokenFrame(), spark, "silver", "broken_table", tmp_delta)

    from common.config import METRICS_PATH

    failed = (
        spark.read.format("delta")
        .load(METRICS_PATH)
        .filter((col("table_name") == "broken_table") & (col("status") == "failed"))
        .collect()
    )
    assert len(failed) == 1
    assert "disk full" in failed[0].error_message


def test_record_metric_never_raises(spark, monkeypatch, tmp_path):
    import common.metrics as metrics

    blocker = tmp_path / "a_file"
    blocker.write_text("x")
    # a Delta path below a regular file cannot be created; the failure must be swallowed
    monkeypatch.setattr(metrics, "METRICS_PATH", (blocker / "metrics").as_uri())
    record_metric(spark, "gold", "t", 1, 0.1)
