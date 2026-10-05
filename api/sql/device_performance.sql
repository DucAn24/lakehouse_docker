-- Hiệu năng bán hàng theo loại thiết bị (Device Performance)
SELECT 
    d.device,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.total_usd), 2) AS total_revenue_usd,
    ROUND(AVG(o.total_usd), 2) AS avg_order_value_usd
FROM delta.gold.fact_click_orders o
JOIN delta.gold.dim_device d ON o.device_sk = d.device_sk
GROUP BY d.device
ORDER BY total_revenue_usd DESC
