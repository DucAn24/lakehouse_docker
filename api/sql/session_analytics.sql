-- Phân tích phiên hoạt động theo thiết bị và nguồn truy cập
SELECT 
    d.device,
    ts.traffic_source,
    COUNT(DISTINCT s.session_sk) AS total_sessions,
    COUNT(e.event_sk) AS total_events,
    ROUND(CAST(COUNT(e.event_sk) AS DOUBLE) / COUNT(DISTINCT s.session_sk), 2) AS avg_actions_per_session
FROM delta.gold.fact_sessions s
JOIN delta.gold.dim_device d ON s.device_sk = d.device_sk
JOIN delta.gold.dim_traffic_source ts ON s.source_sk = ts.source_sk
LEFT JOIN delta.gold.fact_clickstream_events e ON s.session_sk = e.session_sk
GROUP BY d.device, ts.traffic_source
ORDER BY total_sessions DESC
LIMIT 10
