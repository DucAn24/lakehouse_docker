"""Reusable data quality framework for Bronze, Silver and Gold layers.

Validates Iceberg tables against configurable rules and writes results to the
quality log table (common.tables.QUALITY_LOG_TABLE).  Returns exit-code-friendly
pass/fail so Airflow can gate downstream steps.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import coalesce, col, lit

from common.catalog import append_rows
from common.config import get_logger
from common.tables import QUALITY_LOG_TABLE, table_id

logger = get_logger("data_quality")


# ---------------------------------------------------------------------------
# Quality check definitions
# ---------------------------------------------------------------------------
# Business keys of each bronze table (key columns inside the Debezium `after` struct).
BRONZE_KEYS: dict[str, list[str]] = {
    "olist_orders": ["order_id"],
    "olist_customers": ["customer_id"],
    "olist_geolocation": [],
    "olist_order_items": ["order_id", "order_item_id"],
    "olist_order_payments": ["order_id", "payment_sequential"],
    "olist_order_reviews": ["review_id", "order_id"],
    "olist_products": ["product_id"],
    "olist_sellers": ["seller_id"],
    "product_category_translation": ["product_category_name"],
    "click_customers": ["customer_id"],
    "click_products": ["product_id"],
    "click_sessions": ["session_id"],
    "click_orders": ["order_id"],
    "click_order_items": ["order_id", "product_id"],
    "click_events": ["event_id"],
    "click_reviews": ["review_id"],
}


def get_bronze_checks() -> dict[str, dict]:
    """Bronze holds raw CDC events, so several rows per key are expected (insert + updates).

    Checks cover the envelope (op / ts_ms / after) and that every non-delete event
    carries its business key.
    """
    checks = {}
    for table, keys in BRONZE_KEYS.items():
        checks[table] = {
            "table": table_id("bronze", table),
            "primary_keys": [f"after.{k}" for k in keys],
            "row_filter": col("op") != "d",
            "check_duplicates": False,
            "rules": {
                "Valid CDC op": col("op").isin(["c", "r", "u"]),
                "Has ts_ms": col("ts_ms") > 0,
                "Has after image": col("after").isNotNull(),
            },
        }
    return checks


def get_silver_checks() -> dict[str, dict]:
    return {
        "olist_customers": {
            "table": table_id("silver", "olist_customers"),
            "primary_keys": ["customer_id"],
            "rules": {
                "Valid State Code": col("customer_state").rlike("^[A-Z]{2}$"),
                "Valid Zip Code": col("customer_zip_code_prefix").rlike("^[0-9]{5}$"),
                "Valid Latitude": (col("customer_latitude").isNull()) | col("customer_latitude").between(-90, 90),
                "Valid Longitude": (col("customer_longitude").isNull()) | col("customer_longitude").between(-180, 180),
            },
        },
        "olist_sellers": {
            "table": table_id("silver", "olist_sellers"),
            "primary_keys": ["seller_id"],
            "rules": {
                "Valid State Code": col("seller_state").rlike("^[A-Z]{2}$"),
                "Valid Latitude": (col("seller_latitude").isNull()) | col("seller_latitude").between(-90, 90),
                "Valid Longitude": (col("seller_longitude").isNull()) | col("seller_longitude").between(-180, 180),
            },
        },
        "olist_products": {
            "table": table_id("silver", "olist_products"),
            "primary_keys": ["product_id"],
            "rules": {
                "Positive Weight": (col("product_weight_g").isNull()) | (col("product_weight_g") > 0),
                "Positive Volume": (col("product_volume_cm3").isNull()) | (col("product_volume_cm3") > 0),
            },
        },
        "olist_orders": {
            "table": table_id("silver", "olist_orders"),
            "primary_keys": ["order_id"],
            "rules": {
                "Valid Status": col("order_status").isin(
                    ["DELIVERED", "SHIPPED", "CANCELED", "UNAVAILABLE", "INVOICED", "PROCESSING", "CREATED", "APPROVED"]
                ),
                "Purchase Before Delivery": (
                    col("order_delivered_customer_date").isNull()
                    | (col("order_purchase_timestamp") <= col("order_delivered_customer_date"))
                ),
            },
        },
        "olist_order_items": {
            "table": table_id("silver", "olist_order_items"),
            "primary_keys": ["order_id", "order_item_id"],
            "rules": {
                "Positive Price": col("price") >= 0,
                "Positive Freight": col("freight_value") >= 0,
                "Valid Total": col("total_item_value") >= 0,
            },
        },
        "olist_order_payments": {
            "table": table_id("silver", "olist_order_payments"),
            "primary_keys": ["order_id", "payment_sequential"],
            "rules": {
                "Valid Payment Type": col("payment_type").isin(["CREDIT_CARD", "BOLETO", "VOUCHER", "DEBIT_CARD"]),
                "Positive Amount": col("payment_value") > 0,
                "Valid Installments": col("payment_installments") >= 1,
            },
        },
        "olist_order_reviews": {
            "table": table_id("silver", "olist_order_reviews"),
            "primary_keys": ["review_id", "order_id"],
            "rules": {
                "Valid Score": col("review_score").between(1, 5),
                "Valid Rating": col("review_rating").isin(["POSITIVE", "NEUTRAL", "NEGATIVE", "UNKNOWN"]),
            },
        },
        # Clickstream
        "click_customers": {
            "table": table_id("silver", "click_customers"),
            "primary_keys": ["customer_id"],
            "rules": {
                "Valid Email": col("email").contains("@"),
                "Valid Age": (col("age").isNull()) | col("age").between(0, 120),
            },
        },
        "click_products": {
            "table": table_id("silver", "click_products"),
            "primary_keys": ["product_id"],
            "rules": {
                "Non-negative Price": col("price_usd") >= 0,
                "Non-negative Cost": col("cost_usd") >= 0,
            },
        },
        "click_sessions": {
            "table": table_id("silver", "click_sessions"),
            "primary_keys": ["session_id"],
            "rules": {
                "Has Start Time": col("start_time").isNotNull(),
                "Has Device": col("device").isNotNull(),
                "Has Source": col("source").isNotNull(),
            },
        },
        "click_orders": {
            "table": table_id("silver", "click_orders"),
            "primary_keys": ["order_id"],
            "rules": {
                "Has Order Time": col("order_time").isNotNull(),
                "Non-negative Total": col("total_usd") >= 0,
                "Non-negative Discount": col("discount_pct") >= 0,
            },
        },
        "click_order_items": {
            "table": table_id("silver", "click_order_items"),
            "primary_keys": ["order_id", "product_id"],
            "rules": {
                "Positive Quantity": col("quantity") >= 1,
                "Non-negative Unit Price": col("unit_price_usd") >= 0,
            },
        },
        "click_events": {
            "table": table_id("silver", "click_events"),
            "primary_keys": ["event_id"],
            "rules": {
                "Has Event Type": col("event_type").isNotNull(),
                "Has Timestamp": col("event_timestamp").isNotNull(),
            },
        },
        "click_reviews": {
            "table": table_id("silver", "click_reviews"),
            "primary_keys": ["review_id"],
            "rules": {
                "Valid Rating": col("rating").between(1, 5),
                "Valid Sentiment": col("sentiment").isin(["POSITIVE", "NEUTRAL", "NEGATIVE"]),
            },
        },
    }


def get_gold_checks() -> dict[str, dict]:
    # Gold builders coalesce missing dimension lookups to -1 (unknown member), so an FK rule
    # is "resolved" = not -1 / not null; up to 5 % unknown is tolerated by the rule threshold.
    def resolved(column: str):
        return coalesce(col(column), lit(-1)) != -1

    return {
        "dim_customer": {"table": table_id("gold", "dim_customer"), "primary_keys": ["customer_sk"], "rules": {}},
        "dim_date": {"table": table_id("gold", "dim_date"), "primary_keys": ["date_sk"], "rules": {}},
        "dim_seller": {"table": table_id("gold", "dim_seller"), "primary_keys": ["seller_sk"], "rules": {}},
        "dim_product": {"table": table_id("gold", "dim_product"), "primary_keys": ["product_sk"], "rules": {}},
        "fact_orders": {
            "table": table_id("gold", "fact_orders"),
            "primary_keys": ["order_item_sk"],
            "rules": {
                "Resolved customer FK": resolved("customer_sk"),
                "Resolved product FK": resolved("product_sk"),
                "Resolved seller FK": resolved("seller_sk"),
                "Resolved date FK": resolved("date_sk"),
            },
        },
        "fact_reviews": {
            "table": table_id("gold", "fact_reviews"),
            "primary_keys": ["review_sk"],
            "rules": {
                "Resolved customer FK": resolved("customer_sk"),
                "Resolved date FK": resolved("date_sk"),
                "Valid review score": col("review_score").between(1, 5),
            },
        },
        # Clickstream
        "dim_device": {"table": table_id("gold", "dim_device"), "primary_keys": ["device_sk"], "rules": {}},
        "dim_traffic_source": {"table": table_id("gold", "dim_traffic_source"), "primary_keys": ["source_sk"], "rules": {}},
        "dim_click_customer": {"table": table_id("gold", "dim_click_customer"), "primary_keys": ["customer_sk"], "rules": {}},
        "dim_click_product": {"table": table_id("gold", "dim_click_product"), "primary_keys": ["product_sk"], "rules": {}},
        "fact_click_orders": {
            "table": table_id("gold", "fact_click_orders"),
            "primary_keys": ["order_sk"],
            "rules": {
                "Resolved customer FK": resolved("customer_sk"),
                "Resolved device FK": resolved("device_sk"),
                "Resolved source FK": resolved("source_sk"),
                "Resolved date FK": resolved("date_sk"),
                "Non-negative Total": col("total_usd") >= 0,
            },
        },
        "fact_sessions": {
            "table": table_id("gold", "fact_sessions"),
            "primary_keys": ["session_sk"],
            "rules": {
                "Resolved customer FK": resolved("customer_sk"),
                "Resolved device FK": resolved("device_sk"),
                "Resolved source FK": resolved("source_sk"),
                "Resolved date FK": resolved("date_sk"),
            },
        },
        "fact_clickstream_events": {
            "table": table_id("gold", "fact_clickstream_events"),
            "primary_keys": ["event_sk"],
            "rules": {
                "Resolved session FK": resolved("session_sk"),
                "Resolved date FK": resolved("date_sk"),
            },
        },
        "fact_click_reviews": {
            "table": table_id("gold", "fact_click_reviews"),
            "primary_keys": ["review_sk"],
            "rules": {
                "Resolved order FK": resolved("order_sk"),
                "Resolved product FK": resolved("product_sk"),
                "Valid rating": col("rating").between(1, 5),
            },
        },
    }


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------
def validate_table(
    spark: SparkSession,
    table_name: str,
    table: str,
    primary_keys: list[str],
    rules: dict | None = None,
    row_filter=None,
    check_duplicates: bool = True,
) -> tuple[bool, list[dict]]:
    """Validate an Iceberg table and return (passed, list_of_results).

    A table *fails* if:
      - It has 0 records
      - Any primary key column has NULLs
      - Any duplicate primary keys exist (unless check_duplicates=False, e.g. raw CDC)
      - Any custom rule has > 5 % violation rate (a NULL rule result counts as a violation)

    ``row_filter`` restricts the validated rows (e.g. drop CDC deletes in Bronze).
    """
    results: list[dict] = []
    passed = True

    try:
        df = spark.table(table)
    except Exception as exc:
        logger.error("Cannot read %s (%s): %s", table_name, table, exc)
        return False, [{"table": table_name, "check": "readable", "passed": False, "detail": str(exc)}]

    if row_filter is not None:
        df = df.filter(row_filter)
    total = df.count()
    if total == 0:
        logger.warning("Table %s is empty", table_name)
        return False, [{"table": table_name, "check": "non_empty", "passed": False, "detail": "0 records"}]

    # PK null check
    for pk in primary_keys:
        nulls = df.filter(col(pk).isNull()).count()
        ok = nulls == 0
        if not ok:
            passed = False
        results.append(
            {
                "table": table_name,
                "check": f"pk_not_null_{pk}",
                "passed": ok,
                "detail": f"{nulls}/{total} nulls",
            }
        )

    # Duplicate check
    if check_duplicates and primary_keys:
        distinct = df.select(*primary_keys).distinct().count()
        dupes = total - distinct
        ok = dupes == 0
        if not ok:
            passed = False
        results.append(
            {
                "table": table_name,
                "check": "no_duplicates",
                "passed": ok,
                "detail": f"{dupes} duplicates",
            }
        )

    # Custom rules (5 % threshold before failing)
    for rule_name, condition in (rules or {}).items():
        try:
            violations = df.filter(~coalesce(condition, lit(False))).count()
            pct = (violations / total * 100) if total > 0 else 0
            ok = pct <= 5.0
        except Exception:
            violations = -1
            pct = 100.0
            ok = False
        if not ok:
            passed = False
        results.append(
            {
                "table": table_name,
                "check": rule_name,
                "passed": ok,
                "detail": f"{violations}/{total} ({pct:.1f}%)",
            }
        )

    status = "PASS" if passed else "FAIL"
    logger.info("%-40s %s  (%d records)", table_name, status, total)
    return passed, results


CHECKS = {
    "bronze": get_bronze_checks,
    "silver": get_silver_checks,
    "gold": get_gold_checks,
}


def run_quality_checks(
    spark: SparkSession,
    layer: str,
    checks: dict[str, dict],
) -> bool:
    """Run all checks for a layer. Returns True if all pass."""
    all_results: list[dict] = []
    all_passed = True

    for table_name, cfg in checks.items():
        ok, results = validate_table(
            spark,
            table_name,
            table=cfg["table"],
            primary_keys=cfg["primary_keys"],
            rules=cfg.get("rules", {}),
            row_filter=cfg.get("row_filter"),
            check_duplicates=cfg.get("check_duplicates", True),
        )
        if not ok:
            all_passed = False
        all_results.extend(results)

    # Persist results to the quality log table
    _write_quality_log(spark, layer, all_results)

    summary = "PASSED" if all_passed else "FAILED"
    logger.info("Quality check for [%s]: %s (%d checks)", layer, summary, len(all_results))
    return all_passed


def _write_quality_log(spark: SparkSession, layer: str, results: list[dict]):
    """Append quality check results to the quality log table."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        Row(
            layer=layer,
            table_name=r["table"],
            check_name=r["check"],
            passed=r["passed"],
            detail=r["detail"],
            checked_at=now,
        )
        for r in results
    ]
    try:
        append_rows(spark.createDataFrame(rows), QUALITY_LOG_TABLE)
    except Exception:
        logger.warning("Failed to write quality log — continuing", exc_info=True)


# ---------------------------------------------------------------------------
# CLI entry points (called by Airflow)
# ---------------------------------------------------------------------------
