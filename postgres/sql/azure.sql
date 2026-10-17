-- Drop existing tables (in reverse order due to foreign key constraints)
DROP TABLE IF EXISTS dbo.olist_order_reviews CASCADE;
DROP TABLE IF EXISTS dbo.olist_order_payments CASCADE;
DROP TABLE IF EXISTS dbo.olist_geolocation CASCADE;
DROP TABLE IF EXISTS dbo.olist_order_items CASCADE;
DROP TABLE IF EXISTS dbo.olist_orders CASCADE;
DROP TABLE IF EXISTS dbo.product_category_translation CASCADE;
DROP TABLE IF EXISTS dbo.olist_products CASCADE;
DROP TABLE IF EXISTS dbo.olist_sellers CASCADE;
DROP TABLE IF EXISTS dbo.olist_customers CASCADE;

-- Create tables
CREATE TABLE dbo.olist_customers (
    customer_id VARCHAR(256) PRIMARY KEY,
    customer_unique_id VARCHAR(256),
    customer_zip_code_prefix VARCHAR(256),
    customer_city VARCHAR(256),
    customer_state VARCHAR(256)
);

CREATE TABLE dbo.olist_sellers (
    seller_id VARCHAR(256) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(256),
    seller_city VARCHAR(256),
    seller_state VARCHAR(256)
);

CREATE TABLE dbo.olist_products (
    product_id VARCHAR(256) PRIMARY KEY,
    product_category_name VARCHAR(256),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE dbo.product_category_translation (
    product_category_name VARCHAR(256),
    product_category_name_english VARCHAR(256)
);

CREATE TABLE dbo.olist_orders (
    order_id VARCHAR(256) PRIMARY KEY,
    customer_id VARCHAR(256),
    order_status VARCHAR(256),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE dbo.olist_order_items (
    order_id VARCHAR(256),
    order_item_id INT,
    product_id VARCHAR(256),
    seller_id VARCHAR(256),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2)
);

CREATE TABLE dbo.olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(256),
    geolocation_lat NUMERIC(9,6),
    geolocation_lng NUMERIC(9,6),
    geolocation_city VARCHAR(256),
    geolocation_state VARCHAR(256)
);

CREATE TABLE dbo.olist_order_payments (
    order_id VARCHAR(256),
    payment_sequential INT,
    payment_type VARCHAR(256),
    payment_installments INT,
    payment_value NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE dbo.olist_order_reviews (
    review_id VARCHAR(256) PRIMARY KEY,
    order_id VARCHAR(256),
    review_score INT,
    review_comment_title VARCHAR(256),
    review_comment_message VARCHAR(256),
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);
