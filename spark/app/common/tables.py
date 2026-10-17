"""Single registry of every lakehouse table: layer -> table name -> Iceberg identifier.

Identifiers are `<catalog>.<layer>.<table>` in the Polaris catalog (common.config.CATALOG), the same
names Spark, Trino/dbt and every other Iceberg client use. Jobs, the data quality gates and table
maintenance all resolve tables here; when adding a table, add it here too.
"""

from __future__ import annotations

from common.config import CATALOG

# Bronze tables are named after the source table and fed by its Debezium topic.
_BRONZE_TOPICS = {
    "olist_orders": "olist.public.olist_orders",
    "olist_customers": "olist.public.olist_customers",
    "olist_geolocation": "olist.public.olist_geolocation",
    "olist_order_items": "olist.public.olist_order_items",
    "olist_order_payments": "olist.public.olist_order_payments",
    "olist_order_reviews": "olist.public.olist_order_reviews",
    "olist_products": "olist.public.olist_products",
    "olist_sellers": "olist.public.olist_sellers",
    "product_category_translation": "olist.public.product_category_translation",
    "click_customers": "clickstream.public.click_customers",
    "click_products": "clickstream.public.click_products",
    "click_sessions": "clickstream.public.click_sessions",
    "click_orders": "clickstream.public.click_orders",
    "click_order_items": "clickstream.public.click_order_items",
    "click_events": "clickstream.public.click_events",
    "click_reviews": "clickstream.public.click_reviews",
}

_SILVER_TABLES = [
    "olist_customers",
    "olist_sellers",
    "olist_products",
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "click_customers",
    "click_products",
    "click_sessions",
    "click_orders",
    "click_order_items",
    "click_events",
    "click_reviews",
]

_GOLD_TABLES = [
    "dim_date",
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_device",
    "dim_traffic_source",
    "dim_click_customer",
    "dim_click_product",
    "fact_orders",
    "fact_reviews",
    "fact_click_orders",
    "fact_sessions",
    "fact_clickstream_events",
    "fact_click_reviews",
]

# Business key of each silver table: the MERGE key of `common.writers.upsert_with_metrics`
# (must stay unique, same columns as the silver DQ `primary_keys`).
SILVER_KEYS: dict[str, list[str]] = {
    "olist_customers": ["customer_id"],
    "olist_sellers": ["seller_id"],
    "olist_products": ["product_id"],
    "olist_orders": ["order_id"],
    "olist_order_items": ["order_id", "order_item_id"],
    "olist_order_payments": ["order_id", "payment_sequential"],
    "olist_order_reviews": ["review_id", "order_id"],
    "click_customers": ["customer_id"],
    "click_products": ["product_id"],
    "click_sessions": ["session_id"],
    "click_orders": ["order_id"],
    "click_order_items": ["order_id", "product_id"],
    "click_events": ["event_id"],
    "click_reviews": ["review_id"],
}

BRONZE_TOPICS: dict[str, str] = dict(_BRONZE_TOPICS)

TABLES: dict[str, dict[str, str]] = {
    "bronze": {name: f"{CATALOG}.bronze.{name}" for name in _BRONZE_TOPICS},
    "silver": {name: f"{CATALOG}.silver.{name}" for name in _SILVER_TABLES},
    "gold": {name: f"{CATALOG}.gold.{name}" for name in _GOLD_TABLES},
}

LAYERS = tuple(TABLES)


def table_id(layer: str, table: str) -> str:
    """Iceberg identifier `<catalog>.<layer>.<table>` of a registered table."""
    return TABLES[layer][table]


def select_tables(layer: str, table: str | None = None) -> dict[str, str]:
    """All tables of a layer, or just one (raises KeyError for an unknown table)."""
    tables = TABLES[layer]
    if table is None:
        return dict(tables)
    if table not in tables:
        raise KeyError(f"Unknown {layer} table: {table}")
    return {table: tables[table]}


# Pipeline observability tables (written by common.metrics / common.quality, and by dbt's
# record_run_metrics hook), read by Trino + Grafana. Not a "layer": they have no jobs or DQ rules
# of their own.
MONITORING_SCHEMA = "monitoring"
METRICS_TABLE = f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_metrics"
QUALITY_LOG_TABLE = f"{CATALOG}.{MONITORING_SCHEMA}.data_quality_log"
MONITORING_TABLES: dict[str, str] = {
    "pipeline_metrics": METRICS_TABLE,
    "data_quality_log": QUALITY_LOG_TABLE,
}
