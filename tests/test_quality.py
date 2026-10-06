"""Data quality framework: rule definitions and validate_table / run_quality_checks."""

from __future__ import annotations

import pytest
from pyspark.sql.functions import col

from common.config import QUALITY_LOG_PATH
from common.quality import CHECKS, get_bronze_checks, run_quality_checks, validate_table
from common.tables import TABLES


def _save(spark, rows, schema, path):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


def _by_check(results):
    return {r["check"]: r for r in results}


# ---- rule definitions --------------------------------------------------------------------
@pytest.mark.parametrize("layer", list(TABLES))
def test_rules_cover_exactly_the_registry(spark, layer):
    checks = CHECKS[layer]()
    assert set(checks) == set(TABLES[layer])
    for table, cfg in checks.items():
        assert cfg["path"] == TABLES[layer][table]
        assert set(cfg) <= {"path", "primary_keys", "rules", "row_filter", "check_duplicates"}


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_every_silver_and_gold_table_declares_primary_keys(spark, layer):
    assert all(cfg["primary_keys"] for cfg in CHECKS[layer]().values())


def test_bronze_checks_allow_cdc_history_and_target_the_after_image(spark):
    for table, cfg in get_bronze_checks().items():
        assert cfg["check_duplicates"] is False, table
        assert all(pk.startswith("after.") for pk in cfg["primary_keys"]), table


# ---- validate_table ----------------------------------------------------------------------
def test_clean_table_passes(spark, tmp_delta):
    _save(spark, [(1, 10), (2, 20)], "id int, v int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"], {"positive": col("v") > 0})
    assert ok
    assert all(r["passed"] for r in results)


def test_unreadable_path_fails(spark, tmp_delta):
    ok, results = validate_table(spark, "t", tmp_delta, ["id"])
    assert not ok
    assert results[0]["check"] == "readable"


def test_empty_table_fails(spark, tmp_delta):
    _save(spark, [], "id int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"])
    assert not ok
    assert results[0]["check"] == "non_empty"


def test_null_primary_key_fails(spark, tmp_delta):
    _save(spark, [(1,), (None,)], "id int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"])
    assert not ok
    assert not _by_check(results)["pk_not_null_id"]["passed"]


def test_duplicate_primary_key_fails(spark, tmp_delta):
    _save(spark, [(1,), (1,), (2,)], "id int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"])
    assert not ok
    assert _by_check(results)["no_duplicates"]["detail"] == "1 duplicates"


def test_duplicates_tolerated_when_check_disabled(spark, tmp_delta):
    _save(spark, [(1,), (1,)], "id int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"], check_duplicates=False)
    assert ok
    assert "no_duplicates" not in _by_check(results)


def test_composite_key_duplicates(spark, tmp_delta):
    _save(spark, [(1, 1), (1, 2), (1, 2)], "a int, b int", tmp_delta)
    ok, _ = validate_table(spark, "t", tmp_delta, ["a", "b"])
    assert not ok


def test_rule_threshold_is_five_percent(spark, tmp_delta):
    # 5 of 100 violations = 5.0 % -> still passes; 6 of 100 fails
    _save(spark, [(i, 0 if i < 5 else 1) for i in range(100)], "id int, v int", tmp_delta)
    ok, _ = validate_table(spark, "t", tmp_delta, ["id"], {"r": col("v") > 0})
    assert ok

    _save(spark, [(i, 0 if i < 6 else 1) for i in range(100)], "id int, v int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"], {"r": col("v") > 0})
    assert not ok
    assert _by_check(results)["r"]["detail"].startswith("6/100")


def test_null_rule_result_counts_as_violation(spark, tmp_delta):
    _save(spark, [(1, None), (2, None)], "id int, v int", tmp_delta)
    ok, _ = validate_table(spark, "t", tmp_delta, ["id"], {"r": col("v") > 0})
    assert not ok


def test_broken_rule_fails_instead_of_raising(spark, tmp_delta):
    _save(spark, [(1,)], "id int", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["id"], {"r": col("missing_column") > 0})
    assert not ok
    assert _by_check(results)["r"]["detail"].startswith("-1/")


def test_row_filter_restricts_validated_rows(spark, tmp_delta):
    # the bad row is a CDC delete, which bronze validation excludes
    _save(spark, [("c", 1), ("d", None)], "op string, id int", tmp_delta)
    ok, _ = validate_table(spark, "t", tmp_delta, ["id"], row_filter=col("op") != "d")
    assert ok


def test_nested_after_key_is_supported(spark, tmp_delta):
    _save(spark, [("c", (1,)), ("c", (None,))], "op string, after struct<id:int>", tmp_delta)
    ok, results = validate_table(spark, "t", tmp_delta, ["after.id"], check_duplicates=False)
    assert not ok
    assert "pk_not_null_after.id" in _by_check(results)


# ---- run_quality_checks ------------------------------------------------------------------
def test_run_quality_checks_aggregates_and_logs(spark, tmp_path):
    good, bad = (tmp_path / "good").as_uri(), (tmp_path / "bad").as_uri()
    _save(spark, [(1,), (2,)], "id int", good)
    _save(spark, [(1,), (1,)], "id int", bad)

    assert run_quality_checks(spark, "silver", {"good": {"path": good, "primary_keys": ["id"]}})
    assert not run_quality_checks(
        spark,
        "silver",
        {"good": {"path": good, "primary_keys": ["id"]}, "bad": {"path": bad, "primary_keys": ["id"]}},
    )

    log = spark.read.format("delta").load(QUALITY_LOG_PATH)
    failed = log.filter((col("table_name") == "bad") & ~col("passed")).collect()
    assert [r.check_name for r in failed] == ["no_duplicates"]
    assert {r.layer for r in failed} == {"silver"}
