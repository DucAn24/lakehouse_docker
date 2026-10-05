from __future__ import annotations

import argparse
import sys

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

from common.config import create_spark_session, get_logger, KAFKA_BOOTSTRAP, BRONZE_BUCKET

logger = get_logger("kafka_to_bronze")

# ---------------------------------------------------------------------------
# Debezium CDC table schemas
# ---------------------------------------------------------------------------
SCHEMAS: dict[str, StructType] = {
    "olist.public.olist_orders": StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_status", StringType()),
            StructField("order_purchase_timestamp", StringType()),
            StructField("order_approved_at", StringType()),
            StructField("order_delivered_carrier_date", StringType()),
            StructField("order_delivered_customer_date", StringType()),
            StructField("order_estimated_delivery_date", StringType()),
        ]
    ),
    "olist.public.olist_customers": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_unique_id", StringType()),
            StructField("customer_zip_code_prefix", StringType()),
            StructField("customer_city", StringType()),
            StructField("customer_state", StringType()),
        ]
    ),
    "olist.public.olist_geolocation": StructType(
        [
            StructField("geolocation_zip_code_prefix", StringType()),
            StructField("geolocation_lat", StringType()),
            StructField("geolocation_lng", StringType()),
            StructField("geolocation_city", StringType()),
            StructField("geolocation_state", StringType()),
        ]
    ),
    "olist.public.olist_order_items": StructType(
        [
            StructField("order_id", StringType()),
            StructField("order_item_id", StringType()),
            StructField("product_id", StringType()),
            StructField("seller_id", StringType()),
            StructField("shipping_limit_date", StringType()),
            StructField("price", StringType()),
            StructField("freight_value", StringType()),
        ]
    ),
    "olist.public.olist_order_payments": StructType(
        [
            StructField("order_id", StringType()),
            StructField("payment_sequential", StringType()),
            StructField("payment_type", StringType()),
            StructField("payment_installments", StringType()),
            StructField("payment_value", StringType()),
        ]
    ),
    "olist.public.olist_order_reviews": StructType(
        [
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("review_score", StringType()),
            StructField("review_comment_title", StringType()),
            StructField("review_comment_message", StringType()),
            StructField("review_creation_date", StringType()),
            StructField("review_answer_timestamp", StringType()),
        ]
    ),
    "olist.public.olist_products": StructType(
        [
            StructField("product_id", StringType()),
            StructField("product_category_name", StringType()),
            StructField("product_name_length", StringType()),
            StructField("product_description_length", StringType()),
            StructField("product_photos_qty", StringType()),
            StructField("product_weight_g", StringType()),
            StructField("product_length_cm", StringType()),
            StructField("product_height_cm", StringType()),
            StructField("product_width_cm", StringType()),
        ]
    ),
    "olist.public.olist_sellers": StructType(
        [
            StructField("seller_id", StringType()),
            StructField("seller_zip_code_prefix", StringType()),
            StructField("seller_city", StringType()),
            StructField("seller_state", StringType()),
        ]
    ),
    "olist.public.product_category_translation": StructType(
        [
            StructField("product_category_name", StringType()),
            StructField("product_category_name_english", StringType()),
        ]
    ),
    # Clickstream tables
    "clickstream.public.click_customers": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("country", StringType()),
            StructField("age", StringType()),
            StructField("signup_date", StringType()),
            StructField("marketing_opt_in", StringType()),
        ]
    ),
    "clickstream.public.click_products": StructType(
        [
            StructField("product_id", StringType()),
            StructField("category", StringType()),
            StructField("name", StringType()),
            StructField("price_usd", StringType()),
            StructField("cost_usd", StringType()),
            StructField("margin_usd", StringType()),
        ]
    ),
    "clickstream.public.click_sessions": StructType(
        [
            StructField("session_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("start_time", StringType()),
            StructField("device", StringType()),
            StructField("source", StringType()),
            StructField("country", StringType()),
        ]
    ),
    "clickstream.public.click_orders": StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_time", StringType()),
            StructField("payment_method", StringType()),
            StructField("discount_pct", StringType()),
            StructField("subtotal_usd", StringType()),
            StructField("total_usd", StringType()),
            StructField("country", StringType()),
            StructField("device", StringType()),
            StructField("source", StringType()),
        ]
    ),
    "clickstream.public.click_order_items": StructType(
        [
            StructField("order_id", StringType()),
            StructField("product_id", StringType()),
            StructField("unit_price_usd", StringType()),
            StructField("quantity", StringType()),
            StructField("line_total_usd", StringType()),
        ]
    ),
    "clickstream.public.click_events": StructType(
        [
            StructField("event_id", StringType()),
            StructField("session_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("event_type", StringType()),
            StructField("product_id", StringType()),
            StructField("qty", StringType()),
            StructField("cart_size", StringType()),
            StructField("payment", StringType()),
            StructField("discount_pct", StringType()),
            StructField("amount_usd", StringType()),
        ]
    ),
    "clickstream.public.click_reviews": StructType(
        [
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("product_id", StringType()),
            StructField("rating", StringType()),
            StructField("review_text", StringType()),
            StructField("review_time", StringType()),
        ]
    ),
}



def process_topic_batch(spark, topic_name: str, table_schema: StructType):
    """Read a single Kafka topic in batch mode and write to Bronze as Delta."""
    logger.info("Processing topic: %s", topic_name)

    cdc_payload_schema = StructType(
        [
            StructField("before", table_schema),
            StructField("after", table_schema),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
        ]
    )
    envelope_schema = StructType(
        [
            StructField("payload", cdc_payload_schema),
        ]
    )

    df_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Check if empty efficiently using Spark's built-in isEmpty (no JSON parsing on driver needed)
    if df_kafka.isEmpty():
        logger.warning("No records found for %s -- skipping", topic_name)
        return

    # Repartition to split the single large Kafka partition into 10 smaller partitions.
    # This prevents Out of Memory (OOM) and executor thread lockups when parsing large datasets (e.g. geolocation with 1M rows).
    df_kafka = df_kafka.repartition(10)

    df_parsed = (
        df_kafka.selectExpr("topic", "CAST(value AS STRING) as json_value")
        .select(
            col("topic"),
            from_json(col("json_value"), envelope_schema).alias("data"),
        )
        .select(
            col("topic"),
            col("data.payload.before").alias("before"),
            col("data.payload.after").alias("after"),
            col("data.payload.op").alias("op"),
            col("data.payload.ts_ms").alias("ts_ms"),
        )
    )

    bronze_path = f"{BRONZE_BUCKET}/{topic_name}/"

    try:
        df_parsed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
        logger.info("Overwrote records in %s", topic_name)
    except Exception:
        logger.exception("Failed to write to %s", topic_name)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to Bronze batch ingestion")
    parser.add_argument("--table", help="Single topic name to process (e.g. olist.public.olist_orders)")
    args = parser.parse_args()

    spark = create_spark_session("OlistKafkaToBronzeBatch", f"{BRONZE_BUCKET}/")

    try:
        if args.table:
            if args.table not in SCHEMAS:
                logger.error("Unknown topic: %s", args.table)
                sys.exit(1)
            process_topic_batch(spark, args.table, SCHEMAS[args.table])
            logger.info("Single topic processing completed: %s", args.table)

        else:
            failed_topics: list[str] = []
            for topic, schema in SCHEMAS.items():
                try:
                    process_topic_batch(spark, topic, schema)
                except Exception:
                    failed_topics.append(topic)
                    logger.exception("Failed to process topic: %s", topic)

            logger.info(
                "Batch summary: %d/%d topics succeeded",
                len(SCHEMAS) - len(failed_topics),
                len(SCHEMAS),
            )
            if failed_topics:
                logger.error("Failed topics: %s", ", ".join(failed_topics))
                sys.exit(1)

            logger.info("Kafka to Bronze batch processing completed successfully")

    except Exception:
        logger.exception("Kafka to Bronze processing failed")
        sys.exit(1)
    finally:
        spark.stop()
