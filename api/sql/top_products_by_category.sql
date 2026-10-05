-- Top san pham ban chay theo danh muc
SELECT 
    p.product_category_name_english AS category,
    COUNT(*) AS items_sold,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.unit_price), 2) AS total_sales,
    ROUND(AVG(f.unit_price), 2) AS avg_price
FROM delta.gold.fact_orders f
JOIN delta.gold.dim_product p ON f.product_sk = p.product_sk
JOIN delta.gold.dim_date d ON f.date_sk = d.date_sk
GROUP BY p.product_category_name_english
ORDER BY total_sales DESC
LIMIT 5
