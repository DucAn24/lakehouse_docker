-- Phan tich danh gia khach hang theo danh muc san pham
SELECT 
    p.product_category_name_english AS category,
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.review_score), 2) AS avg_score,
    SUM(r.is_positive) AS positive_reviews,
    SUM(r.is_negative) AS negative_reviews,
    SUM(r.is_neutral) AS neutral_reviews,
    ROUND(SUM(r.is_positive) * 100.0 / COUNT(*), 2) AS positive_rate_pct
FROM delta.gold.fact_reviews r
JOIN delta.gold.fact_orders f ON r.order_id = f.order_id
JOIN delta.gold.dim_product p ON f.product_sk = p.product_sk
JOIN delta.gold.dim_date d ON r.date_sk = d.date_sk
GROUP BY p.product_category_name_english
ORDER BY total_reviews DESC
LIMIT 5
