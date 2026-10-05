-- Phân tích phễu chuyển đổi Clickstream (Funnel Analysis)
SELECT 
    event_type,
    COUNT(DISTINCT session_sk) AS unique_sessions,
    COUNT(event_sk) AS total_events,
    ROUND(CAST(COUNT(DISTINCT session_sk) AS DOUBLE) / (
        SELECT COUNT(DISTINCT session_sk) 
        FROM delta.gold.fact_clickstream_events 
        WHERE event_type = 'PAGE_VIEW'
    ) * 100.0, 2) AS conversion_rate_pct
FROM delta.gold.fact_clickstream_events
GROUP BY event_type
ORDER BY total_events DESC
