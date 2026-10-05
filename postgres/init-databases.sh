#!/bin/bash
set -e

# Create additional databases for services that share this PostgreSQL instance
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE metabase;
EOSQL

echo "Additional databases created: airflow, metabase"

# Create Olist tables in the orders database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    -- Olist tables
    CREATE TABLE IF NOT EXISTS olist_customers (
        customer_id TEXT PRIMARY KEY,
        customer_unique_id TEXT,
        customer_zip_code_prefix TEXT,
        customer_city TEXT,
        customer_state TEXT
    );
    CREATE TABLE IF NOT EXISTS olist_sellers (
        seller_id TEXT PRIMARY KEY,
        seller_zip_code_prefix TEXT,
        seller_city TEXT,
        seller_state TEXT
    );
    CREATE TABLE IF NOT EXISTS olist_products (
        product_id TEXT PRIMARY KEY,
        product_category_name TEXT,
        product_name_length INT,
        product_description_length INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    );
    CREATE TABLE IF NOT EXISTS product_category_translation (
        product_category_name TEXT,
        product_category_name_english TEXT
    );
    CREATE TABLE IF NOT EXISTS olist_orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_status TEXT,
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES olist_customers(customer_id)
    );
    CREATE TABLE IF NOT EXISTS olist_order_items (
        order_id TEXT,
        order_item_id INT,
        product_id TEXT,
        seller_id TEXT,
        shipping_limit_date TIMESTAMP,
        price NUMERIC(10,2),
        freight_value NUMERIC(10,2),
        PRIMARY KEY (order_id, order_item_id),
        FOREIGN KEY (order_id) REFERENCES olist_orders(order_id),
        FOREIGN KEY (product_id) REFERENCES olist_products(product_id),
        FOREIGN KEY (seller_id) REFERENCES olist_sellers(seller_id)
    );
    CREATE TABLE IF NOT EXISTS olist_geolocation (
        geolocation_zip_code_prefix TEXT,
        geolocation_lat NUMERIC(9,6),
        geolocation_lng NUMERIC(9,6),
        geolocation_city TEXT,
        geolocation_state TEXT
    );
    CREATE TABLE IF NOT EXISTS olist_order_payments (
        order_id TEXT,
        payment_sequential INT,
        payment_type TEXT,
        payment_installments INT,
        payment_value NUMERIC(10,2),
        PRIMARY KEY (order_id, payment_sequential),
        FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
    );
    CREATE TABLE IF NOT EXISTS olist_order_reviews (
        review_id TEXT PRIMARY KEY,
        order_id TEXT,
        review_score INT,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
    );

    -- Clickstream tables
    CREATE TABLE IF NOT EXISTS click_customers (
        customer_id INT PRIMARY KEY,
        name TEXT, email TEXT, country TEXT,
        age INT, signup_date DATE, marketing_opt_in BOOLEAN
    );
    CREATE TABLE IF NOT EXISTS click_products (
        product_id INT PRIMARY KEY,
        category TEXT, name TEXT,
        price_usd NUMERIC(10,2), cost_usd NUMERIC(10,2), margin_usd NUMERIC(10,2)
    );
    CREATE TABLE IF NOT EXISTS click_sessions (
        session_id INT PRIMARY KEY,
        customer_id INT REFERENCES click_customers(customer_id),
        start_time TIMESTAMP, device TEXT, source TEXT, country TEXT
    );
    CREATE TABLE IF NOT EXISTS click_orders (
        order_id INT PRIMARY KEY,
        customer_id INT REFERENCES click_customers(customer_id),
        order_time TIMESTAMP, payment_method TEXT,
        discount_pct NUMERIC(5,2), subtotal_usd NUMERIC(10,2),
        total_usd NUMERIC(10,2), country TEXT, device TEXT, source TEXT
    );
    CREATE TABLE IF NOT EXISTS click_order_items (
        order_id INT REFERENCES click_orders(order_id),
        product_id INT REFERENCES click_products(product_id),
        unit_price_usd NUMERIC(10,2), quantity INT,
        line_total_usd NUMERIC(10,2),
        PRIMARY KEY (order_id, product_id)
    );
    CREATE TABLE IF NOT EXISTS click_events (
        event_id INT PRIMARY KEY,
        session_id INT REFERENCES click_sessions(session_id),
        timestamp TIMESTAMP, event_type TEXT,
        product_id INT, qty INT, cart_size INT,
        payment TEXT, discount_pct NUMERIC(5,2), amount_usd NUMERIC(10,2)
    );
    CREATE TABLE IF NOT EXISTS click_reviews (
        review_id INT PRIMARY KEY,
        order_id INT REFERENCES click_orders(order_id),
        product_id INT REFERENCES click_products(product_id),
        rating INT, review_text TEXT, review_time TIMESTAMP
    );
EOSQL

echo "All Olist + Clickstream tables created in database: $POSTGRES_DB"
