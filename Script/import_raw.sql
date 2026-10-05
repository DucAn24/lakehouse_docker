-- Import Olist CSVs (order matters: parents before children)
TRUNCATE TABLE public.olist_customers RESTART IDENTITY CASCADE;
\copy public.olist_customers FROM '/tmp/olist/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_sellers RESTART IDENTITY CASCADE;
\copy public.olist_sellers FROM '/tmp/olist/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_products RESTART IDENTITY CASCADE;
\copy public.olist_products FROM '/tmp/olist/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_orders RESTART IDENTITY CASCADE;
\copy public.olist_orders FROM '/tmp/olist/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_items RESTART IDENTITY CASCADE;
\copy public.olist_order_items FROM '/tmp/olist/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_geolocation RESTART IDENTITY CASCADE;
\copy public.olist_geolocation FROM '/tmp/olist/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_payments RESTART IDENTITY CASCADE;
\copy public.olist_order_payments FROM '/tmp/olist/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_reviews RESTART IDENTITY CASCADE;
CREATE TEMP TABLE tmp_reviews AS TABLE olist_order_reviews WITH NO DATA;
\copy tmp_reviews FROM '/tmp/olist/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;
INSERT INTO olist_order_reviews
SELECT * FROM tmp_reviews
ON CONFLICT (review_id) DO NOTHING;
DROP TABLE tmp_reviews;

TRUNCATE TABLE public.product_category_translation RESTART IDENTITY CASCADE;
\copy public.product_category_translation FROM '/tmp/olist/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;
