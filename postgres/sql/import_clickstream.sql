-- Import Clickstream CSVs (order matters: parents before children)
TRUNCATE TABLE public.click_customers RESTART IDENTITY CASCADE;
\copy public.click_customers FROM '/tmp/clickstream/customers.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_products RESTART IDENTITY CASCADE;
\copy public.click_products FROM '/tmp/clickstream/products.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_sessions RESTART IDENTITY CASCADE;
\copy public.click_sessions FROM '/tmp/clickstream/sessions.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_orders RESTART IDENTITY CASCADE;
\copy public.click_orders FROM '/tmp/clickstream/orders.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_order_items RESTART IDENTITY CASCADE;
\copy public.click_order_items FROM '/tmp/clickstream/order_items.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_events RESTART IDENTITY CASCADE;
\copy public.click_events FROM '/tmp/clickstream/events.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.click_reviews RESTART IDENTITY CASCADE;
\copy public.click_reviews FROM '/tmp/clickstream/reviews.csv' DELIMITER ',' CSV HEADER;
