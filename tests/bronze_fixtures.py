"""Tiny Debezium-style bronze data covering every bronze table of the lakehouse.

Each table: (columns, events). An event is (op, ts_ms, values-in-column-order); every
`after` field is a string, like the loosely typed CDC payload the silver jobs have to clean.
The data is small but consistent, so every silver/gold join resolves and every DQ gate passes.

Deliberate edge cases (asserted in test_pipeline_e2e.py):
  - olist_customers c1: inserted then updated (CDC dedup keeps the latest city)
  - olist_customers c2 / olist_sellers s1: zip code 99999 has no geolocation row (lat/lng stay NULL)
  - olist_products p2: category without an English translation (falls back to the original name)
  - olist_orders o2: not delivered yet (delivery columns NULL)
  - olist_order_items o1/1, olist_order_payments o1/1, product_category_translation beleza_saude:
    inserted with a stale value, then updated (silver must keep one row per key: the MERGE key is unique)
"""

from __future__ import annotations

BronzeTable = tuple[list[str], list[tuple[str, int, tuple]]]

BRONZE: dict[str, BronzeTable] = {
    # ---- olist -------------------------------------------------------------------------
    "olist_geolocation": (
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
        [
            ("r", 1, ("01001", "-23.55", "-46.63", "sao paulo", "sp")),
            ("r", 1, ("01001", "-23.57", "-46.65", "sao paulo", "sp")),
            ("r", 1, ("22222", "999", "-43.1", "bad", "rj")),  # invalid latitude -> dropped
        ],
    ),
    "olist_customers": (
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        [
            ("c", 1, ("c1", "u1", "01001", "campinas", "sp")),
            ("u", 2, ("c1", "u1", "01001", "sao paulo", "sp")),
            ("r", 1, ("c2", "u2", "99999", "rio", "rj")),
        ],
    ),
    "olist_sellers": (
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        [("r", 1, ("s1", "99999", "curitiba", "pr"))],
    ),
    "olist_products": (
        [
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        [
            ("r", 1, ("p1", "beleza_saude", "40", "200", "2", "500", "10", "5", "4")),
            ("r", 1, ("p2", "sem_traducao", "30", "100", "1", "300", "20", "10", "5")),
        ],
    ),
    "product_category_translation": (
        ["product_category_name", "product_category_name_english"],
        [
            ("c", 1, ("beleza_saude", "stale_name")),
            ("u", 2, ("beleza_saude", "health_beauty")),
        ],
    ),
    "olist_orders": (
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        [
            (
                "r",
                1,
                (
                    "o1",
                    "c1",
                    "delivered",
                    "2018-01-01 10:00:00",
                    "2018-01-01 12:00:00",
                    "2018-01-03 09:00:00",
                    "2018-01-10 10:00:00",
                    "2018-01-08 00:00:00",
                ),
            ),
            (
                "r",
                1,
                ("o2", "c2", "shipped", "2018-02-01 08:00:00", "2018-02-01 09:00:00", "2018-02-02 09:00:00", None, "2018-02-10 00:00:00"),
            ),
        ],
    ),
    "olist_order_items": (
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
        [
            ("c", 1, ("o1", "1", "p1", "s1", "2018-01-05 00:00:00", "90.0", "9.0")),
            ("u", 2, ("o1", "1", "p1", "s1", "2018-01-05 00:00:00", "100.0", "10.0")),
            ("r", 1, ("o1", "2", "p2", "s1", "2018-01-05 00:00:00", "50.0", "5.0")),
            ("r", 1, ("o2", "1", "p1", "s1", "2018-02-05 00:00:00", "20.0", "2.0")),
        ],
    ),
    "olist_order_payments": (
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
        [
            ("c", 1, ("o1", "1", "boleto", "1", "150.0")),
            ("u", 2, ("o1", "1", "credit_card", "3", "160.0")),
            ("r", 1, ("o2", "1", "boleto", "1", "22.0")),
        ],
    ),
    "olist_order_reviews": (
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        [
            ("r", 1, ("r1", "o1", "5", "great", "loved it", "2018-01-11 00:00:00", "2018-01-12 12:00:00")),
            ("r", 1, ("r2", "o2", "2", None, None, "2018-02-12 00:00:00", None)),
        ],
    ),
    # ---- clickstream -------------------------------------------------------------------
    "click_customers": (
        ["customer_id", "name", "email", "country", "age", "signup_date", "marketing_opt_in"],
        [
            ("r", 1, ("1", " Ann ", "ann@example.com", "us", "30", "1/15/2020", "true")),
            ("r", 1, ("2", "Bob", "bob@example.com", "vn", "41", "2021-03-02", "false")),
        ],
    ),
    "click_products": (
        ["product_id", "category", "name", "price_usd", "cost_usd", "margin_usd"],
        [
            ("r", 1, ("1", "shoes", "Runner", "50.0", "30.0", "20.0")),
            ("r", 1, ("2", "hats", "Cap", "15.0", "5.0", "10.0")),
        ],
    ),
    "click_sessions": (
        ["session_id", "customer_id", "start_time", "device", "source", "country"],
        [
            ("r", 1, ("1", "1", "2024-01-01 10:00:00", "mobile", "google", "us")),
            ("r", 1, ("2", "2", "2024-01-02 15:30:00", "desktop", "direct", "vn")),
        ],
    ),
    "click_orders": (
        [
            "order_id",
            "customer_id",
            "order_time",
            "payment_method",
            "discount_pct",
            "subtotal_usd",
            "total_usd",
            "country",
            "device",
            "source",
        ],
        [("r", 1, ("1", "1", "2024-01-01 11:00:00", "card", "0.0", "50.0", "50.0", "us", "mobile", "google"))],
    ),
    "click_order_items": (
        ["order_id", "product_id", "unit_price_usd", "quantity", "line_total_usd"],
        [
            ("r", 1, ("1", "1", "50.0", "1", "50.0")),
            ("r", 1, ("1", "2", "15.0", "0", "0.0")),  # quantity 0 -> dropped
        ],
    ),
    "click_events": (
        ["event_id", "session_id", "timestamp", "event_type", "product_id", "qty", "payment"],
        [
            ("r", 1, ("1", "1", "2024-01-01 10:05:00", "page_view", "1", None, None)),
            ("r", 1, ("2", "1", "2024-01-01 11:00:00", "purchase", "1", "1", "card")),
        ],
    ),
    "click_reviews": (
        ["review_id", "order_id", "product_id", "rating", "review_text", "review_time"],
        [("r", 1, ("1", "1", "1", "5", " nice ", "2024-01-02 09:00:00"))],
    ),
}
