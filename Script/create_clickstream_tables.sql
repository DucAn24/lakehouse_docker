-- ============================================================
-- Clickstream Dataset Tables (E-Commerce Transactions + Clickstream)
-- Source: Kaggle - wafaaelhusseini/e-commerce-transactions-clickstream
-- ============================================================

DROP TABLE IF EXISTS click_reviews CASCADE;
DROP TABLE IF EXISTS click_events CASCADE;
DROP TABLE IF EXISTS click_order_items CASCADE;
DROP TABLE IF EXISTS click_orders CASCADE;
DROP TABLE IF EXISTS click_sessions CASCADE;
DROP TABLE IF EXISTS click_products CASCADE;
DROP TABLE IF EXISTS click_customers CASCADE;

CREATE TABLE click_customers (
    customer_id INT PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    age INT,
    signup_date DATE,
    marketing_opt_in BOOLEAN
);

CREATE TABLE click_products (
    product_id INT PRIMARY KEY,
    category TEXT,
    name TEXT,
    price_usd NUMERIC(10,2),
    cost_usd NUMERIC(10,2),
    margin_usd NUMERIC(10,2)
);

CREATE TABLE click_sessions (
    session_id INT PRIMARY KEY,
    customer_id INT REFERENCES click_customers(customer_id),
    start_time TIMESTAMP,
    device TEXT,
    source TEXT,
    country TEXT
);

CREATE TABLE click_orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES click_customers(customer_id),
    order_time TIMESTAMP,
    payment_method TEXT,
    discount_pct NUMERIC(5,2),
    subtotal_usd NUMERIC(10,2),
    total_usd NUMERIC(10,2),
    country TEXT,
    device TEXT,
    source TEXT
);

CREATE TABLE click_order_items (
    order_id INT REFERENCES click_orders(order_id),
    product_id INT REFERENCES click_products(product_id),
    unit_price_usd NUMERIC(10,2),
    quantity INT,
    line_total_usd NUMERIC(10,2)
);

CREATE TABLE click_events (
    event_id INT PRIMARY KEY,
    session_id INT REFERENCES click_sessions(session_id),
    timestamp TIMESTAMP,
    event_type TEXT,
    product_id NUMERIC,
    qty NUMERIC,
    cart_size NUMERIC,
    payment TEXT,
    discount_pct NUMERIC(5,2),
    amount_usd NUMERIC(10,2)
);

CREATE TABLE click_reviews (
    review_id INT PRIMARY KEY,
    order_id INT REFERENCES click_orders(order_id),
    product_id INT REFERENCES click_products(product_id),
    rating INT,
    review_text TEXT,
    review_time TIMESTAMP
);
