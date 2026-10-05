-- Doanh thu Clickstream theo quốc gia và hành vi chiết khấu
SELECT 
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.total_usd), 2) AS total_revenue_usd,
    ROUND(AVG(o.total_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(o.discount_pct), 2) AS avg_discount_pct
FROM delta.gold.fact_click_orders o
JOIN delta.gold.dim_click_customer c ON o.customer_sk = c.customer_sk
GROUP BY c.country
ORDER BY total_revenue_usd DESC
LIMIT 5
