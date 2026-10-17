"""common.transforms and common.metrics against a local Spark session (Iceberg catalog)."""

from __future__ import annotations

from pyspark.sql.functions import col, date_format

from common.metrics import record_metric
from common.transforms import extract_cdc_latest, safe_to_timestamp


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


def test_extract_cdc_latest_flat_frame_drops_keys_whose_newest_event_is_delete(spark):
    # select_after=False: windows before filtering, and a ts tie goes to the delete
    df = spark.createDataFrame(
        [("c", 1, "a"), ("d", 2, "a"), ("u", 5, "b"), ("d", 5, "b"), ("c", 1, "c")], "op string, ts_ms long, id string"
    )
    out = extract_cdc_latest(df, key_cols=["id"], select_after=False)
    assert {r.id for r in out.collect()} == {"c"}


def test_extract_cdc_latest_payload_column_named_op_is_preserved(spark):
    df = spark.createDataFrame(
        [("c", 1, {"id": "a", "op": "payload"}, None)],
        "op string, ts_ms long, after struct<id:string,op:string>, before struct<id:string,op:string>",
    )
    (row,) = extract_cdc_latest(df, key_cols=["id"]).collect()
    assert (row.id, row.op) == ("a", "payload")


def test_extract_cdc_latest_one_row_per_key(spark):
    df = _cdc(spark, [("c", 1, "a", "v1"), ("u", 2, "a", "v2"), ("u", 3, "a", "v3")])
    rows = extract_cdc_latest(df, key_cols=["id"]).collect()
    assert [(r.id, r.value) for r in rows] == [("a", "v3")]


def test_record_metric_writes_a_row(spark):
    from common.tables import METRICS_TABLE

    record_metric(spark, "silver", "unit_metric", 3, 0.5)
    rows = spark.table(METRICS_TABLE).filter(col("table_name") == "unit_metric").collect()
    assert [(r.layer, r.row_count, r.status) for r in rows] == [("silver", 3, "success")]


def test_record_metric_never_raises(spark, monkeypatch):
    import common.metrics as metrics

    # a table in a catalog that does not exist cannot be written; the failure must be swallowed
    monkeypatch.setattr(metrics, "METRICS_TABLE", "no_such_catalog.monitoring.pipeline_metrics")
    record_metric(spark, "gold", "t", 1, 0.1)
