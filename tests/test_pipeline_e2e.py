"""End-to-end run of the whole pipeline on the fixtures in bronze_fixtures.py.

bronze (seeded here) -> every silver job -> silver DQ gate -> dbt gold models -> gold DQ gate.
Silver jobs run through their real `run(spark)` entry points, reading and writing Iceberg tables in
the test `lakehouse` catalog (conftest.py); the dbt project builds gold on Trino over the same Polaris
catalog (conftest `trino_lake`, needs Docker), and Spark reads that gold back for the DQ gate and the
assertions. Kafka ingestion (`kafka_to_bronze`) is out of scope here.

Tests share one pipeline run (module-scoped fixtures) and are ordered by layer.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from pyspark.sql.functions import col

from bronze_fixtures import BRONZE
from common.catalog import create_or_replace
from common.quality import CHECKS, run_quality_checks
from common.tables import METRICS_TABLE, SILVER_KEYS, TABLES, table_id
from common.writers import snapshot_ids

JOBS = Path(__file__).resolve().parents[1] / "spark" / "app" / "jobs"


def _module(layer: str, table: str):
    (path,) = list((JOBS / layer).rglob(f"{table}.py"))
    dotted = ".".join(path.relative_to(JOBS.parent).with_suffix("").parts)
    return importlib.import_module(dotted)


def _seed_bronze(spark):
    assert set(BRONZE) == set(TABLES["bronze"]), "fixtures must cover every bronze table"
    for table, (columns, events) in BRONZE.items():
        after = ", ".join(f"{c}: string" for c in columns)
        schema = f"op string, ts_ms long, after struct<{after}>"
        rows = [(op, ts, values) for op, ts, values in events]
        create_or_replace(spark.createDataFrame(rows, schema), table_id("bronze", table))


@pytest.fixture(scope="module")
def lake(spark):
    _seed_bronze(spark)
    assert run_quality_checks(spark, "bronze", CHECKS["bronze"]()), "bronze quality gate failed"

    def read(layer: str, table: str):
        return spark.table(table_id(layer, table))

    for table in TABLES["silver"]:
        _module("silver", table).run(spark)
    assert run_quality_checks(spark, "silver", CHECKS["silver"]()), "silver quality gate failed"
    return read


@pytest.fixture(scope="module")
def gold(spark, lake, trino_lake, tmp_path_factory):
    """`dbt build` of the gold models on Trino over the silver tables, then the gold DQ gate.

    Mirrors the DAG: Trino sees the silver tables and monitoring.pipeline_metrics Spark wrote, through
    the shared Polaris catalog. Returns the same reader as `lake`.
    """
    trino_lake.dbt_build(tmp_path_factory.mktemp("dbt"))
    assert run_quality_checks(spark, "gold", CHECKS["gold"]()), "gold quality gate failed"
    return lake


def _recorded_metrics(spark) -> set[tuple[str, str]]:
    metrics = spark.table(METRICS_TABLE).filter(col("status") == "success")
    return {(r.layer, r.table_name) for r in metrics.collect()}


# ---- every table is produced -------------------------------------------------------------
def test_every_silver_table_is_written_and_non_empty(lake):
    for table in TABLES["silver"]:
        assert lake("silver", table).count() > 0, f"silver.{table} is empty"


def test_every_gold_table_is_written_and_non_empty(gold):
    for table in TABLES["gold"]:
        assert gold("gold", table).count() > 0, f"gold.{table} is empty"


def test_metrics_recorded_for_every_silver_table(spark, lake):
    assert {("silver", t) for t in TABLES["silver"]} <= _recorded_metrics(spark)


def test_dbt_records_metrics_for_every_gold_model(spark, gold):
    """The dbt on-run-end hook writes one success row per model, with the rows the model wrote."""
    metrics = spark.table(METRICS_TABLE).filter((col("layer") == "gold") & (col("status") == "success"))
    assert {r.table_name: r.row_count for r in metrics.collect()} == {t: gold("gold", t).count() for t in TABLES["gold"]}


def test_jobs_are_idempotent(spark, lake):
    before = lake("silver", "olist_orders").count()
    _module("silver", "olist_orders").run(spark)
    assert lake("silver", "olist_orders").count() == before


def test_silver_rerun_is_an_incremental_noop(spark, lake):
    stamps = {r.order_id: r.processed_at for r in lake("silver", "olist_orders").collect()}
    _module("silver", "olist_orders").run(spark)
    assert {r.order_id: r.processed_at for r in lake("silver", "olist_orders").collect()} == stamps
    last = (
        spark.table(METRICS_TABLE)
        .filter((col("table_name") == "olist_orders") & (col("layer") == "silver"))
        .orderBy(col("recorded_at").desc())
        .first()
    )
    assert (last.write_mode, last.rows_inserted, last.rows_updated, last.rows_deleted) == ("merge", 0, 0, 0)


def test_silver_tables_are_iceberg_v2_and_keys_match_dq(spark, lake):
    checks = CHECKS["silver"]()
    assert set(SILVER_KEYS) == set(TABLES["silver"])
    for table, ident in TABLES["silver"].items():
        assert SILVER_KEYS[table] == checks[table]["primary_keys"], table
        props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {ident}").collect()}
        assert props["format-version"] == "2", table
        assert snapshot_ids(spark, ident), table


def test_trino_and_spark_see_the_same_gold_snapshot(spark, gold, trino_lake):
    """One catalog: Trino reads exactly the rows Spark sees, with no registration step."""
    for table in ("fact_orders", "dim_customer"):
        ((count,),) = trino_lake.execute(f"SELECT count(*) FROM lakehouse.gold.{table}")
        assert count == gold("gold", table).count(), table


# ---- silver business rules ---------------------------------------------------------------
def test_silver_customers_dedup_cleaning_and_geo(lake):
    rows = {r.customer_id: r for r in lake("silver", "olist_customers").collect()}
    assert set(rows) == {"c1", "c2"}
    assert rows["c1"].customer_city == "SAO PAULO"  # latest CDC version, upper-cased
    assert rows["c1"].customer_state == "SP"
    assert rows["c1"].customer_region == "SUDESTE"
    assert rows["c1"].customer_latitude == pytest.approx(-23.56, abs=1e-3)  # avg of the two geo rows
    assert rows["c2"].customer_region == "SUDESTE"  # RJ
    assert rows["c2"].customer_latitude is None  # zip without geolocation


def test_silver_sellers_region(lake):
    (seller,) = lake("silver", "olist_sellers").collect()
    assert (seller.seller_state, seller.seller_region) == ("PR", "SUL")


def test_silver_products_category_translation_and_volume(lake):
    rows = {r.product_id: r for r in lake("silver", "olist_products").collect()}
    assert rows["p1"].product_category_name_english == "health_beauty"
    assert rows["p2"].product_category_name_english == "sem_traducao"  # no translation -> original
    assert rows["p1"].product_volume_cm3 == 10 * 5 * 4


def test_silver_orders_derived_columns(lake):
    rows = {r.order_id: r for r in lake("silver", "olist_orders").collect()}
    o1, o2 = rows["o1"], rows["o2"]
    assert o1.order_status == "DELIVERED"
    assert o1.is_delivered is True
    assert (o1.order_year, o1.order_month, o1.order_day, o1.order_hour) == (2018, 1, 1, 10)
    assert o1.actual_delivery_days == 9
    assert o1.delivery_delay_days == 2
    assert o1.is_delivered_late is True
    assert o2.is_delivered is False
    assert o2.actual_delivery_days is None
    assert o2.is_delivered_late is None


def test_silver_order_items_totals(lake):
    rows = {(r.order_id, r.order_item_id): r for r in lake("silver", "olist_order_items").collect()}
    assert len(rows) == 3
    assert rows[("o1", 1)].total_item_value == pytest.approx(110.0)
    assert rows[("o1", 1)].freight_ratio == pytest.approx(0.1)


def test_silver_payments_installments(lake):
    rows = {r.order_id: r for r in lake("silver", "olist_order_payments").collect()}
    assert rows["o1"].payment_type == "CREDIT_CARD"
    assert rows["o1"].installment_value == pytest.approx(53.33)
    assert rows["o1"].is_installment_payment is True
    assert rows["o2"].is_installment_payment is False


def test_silver_reviews_rating_and_comment_flags(lake):
    rows = {r.review_id: r for r in lake("silver", "olist_order_reviews").collect()}
    assert (rows["r1"].review_rating, rows["r1"].has_comment) == ("POSITIVE", True)
    assert (rows["r2"].review_rating, rows["r2"].has_comment) == ("NEGATIVE", False)
    assert rows["r1"].review_response_time_hours == pytest.approx(36.0)


def test_silver_click_customers_parse_dates_and_types(lake):
    rows = {r.customer_id: r for r in lake("silver", "click_customers").collect()}
    assert rows[1].customer_name == "Ann"
    assert rows[1].country == "US"
    assert str(rows[1].signup_date) == "2020-01-15"  # M/d/yyyy
    assert str(rows[2].signup_date) == "2021-03-02"  # yyyy-MM-dd
    assert rows[1].marketing_opt_in is True
    assert rows[2].marketing_opt_in is False


def test_silver_click_order_items_drops_zero_quantity(lake):
    assert [(r.order_id, r.product_id) for r in lake("silver", "click_order_items").collect()] == [(1, 1)]


def test_silver_click_events_defaults(lake):
    rows = {r.event_id: r for r in lake("silver", "click_events").collect()}
    assert (rows[1].quantity, rows[1].payment_method) == (0, "NOT_APPLICABLE")
    assert (rows[2].quantity, rows[2].payment_method, rows[2].event_type) == (1, "CARD", "PURCHASE")


def test_silver_click_reviews_sentiment_and_text(lake):
    (review,) = lake("silver", "click_reviews").collect()
    assert (review.sentiment, review.review_text) == ("POSITIVE", "nice")


# ---- gold model --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "table,key",
    [
        ("dim_customer", "customer_sk"),
        ("dim_seller", "seller_sk"),
        ("dim_product", "product_sk"),
        ("dim_device", "device_sk"),
        ("dim_traffic_source", "source_sk"),
        ("dim_click_customer", "customer_sk"),
        ("dim_click_product", "product_sk"),
    ],
)
def test_dimensions_have_unique_keys_and_unknown_member(gold, table, key):
    df = gold("gold", table)
    keys = [r[key] for r in df.select(key).collect()]
    assert len(keys) == len(set(keys))
    assert -1 in keys, f"{table} is missing the UNKNOWN (-1) member"


def test_dim_customer_one_row_per_unique_customer(gold):
    ids = [r.customer_unique_id for r in gold("gold", "dim_customer").collect()]
    assert sorted(ids) == ["UNKNOWN", "u1", "u2"]


def test_dim_date_covers_every_fact_year(gold):
    keys = {r.date_sk for r in gold("gold", "dim_date").collect()}
    assert {20180101, 20181231, 20240101, 20241231} <= keys  # olist 2018 + clickstream 2024


def test_fact_orders_grain_and_foreign_keys(gold):
    df = gold("gold", "fact_orders")
    assert df.count() == 3  # one row per order item
    assert df.filter((col("customer_sk") == -1) | (col("product_sk") == -1) | (col("seller_sk") == -1)).count() == 0
    rows = {(r.order_id, r.order_item_id): r for r in df.collect()}
    assert rows[("o1", 1)].status_category == "COMPLETED"
    assert rows[("o2", 1)].status_category == "IN_PROGRESS"
    assert rows[("o1", 1)].total_payment_value == pytest.approx(160.0)
    assert rows[("o1", 1)].date_sk == 20180101


def test_fact_reviews_flags(gold):
    rows = {r.review_id: r for r in gold("gold", "fact_reviews").collect()}
    assert (rows["r1"].is_positive, rows["r1"].is_negative) == (1, 0)
    assert (rows["r2"].is_positive, rows["r2"].is_negative) == (0, 1)


def test_fact_click_orders_resolves_dimensions(gold):
    (order,) = gold("gold", "fact_click_orders").collect()
    assert order.date_sk == 20240101
    assert -1 not in (order.customer_sk, order.device_sk, order.source_sk)


def test_click_facts_keep_their_grain(gold):
    assert gold("gold", "fact_sessions").count() == 2
    assert gold("gold", "fact_clickstream_events").count() == 2
    assert gold("gold", "fact_click_reviews").count() == 1


def test_click_facts_link_to_parent_facts(gold):
    events = gold("gold", "fact_clickstream_events")
    assert events.filter(col("session_sk") == -1).count() == 0
    assert events.filter(col("product_sk") == -1).count() == 0
    assert gold("gold", "fact_click_reviews").filter(col("order_sk") == -1).count() == 0
