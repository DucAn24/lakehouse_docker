-- Doanh thu theo vung dia ly
SELECT 
    c.customer_region AS region,
    c.customer_state AS state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_item_value) AS total_revenue,
    AVG(f.total_item_value) AS avg_order_value,
    SUM(f.freight_value) AS total_shipping_cost
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
JOIN lakehouse.gold.dim_date d ON f.date_sk = d.date_sk
GROUP BY c.customer_region, c.customer_state
ORDER BY total_revenue DESC
LIMIT 5
