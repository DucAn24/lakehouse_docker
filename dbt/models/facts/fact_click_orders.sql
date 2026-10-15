{{ config(properties=gold_properties()) }}

-- Grain: one row per clickstream order.
select
    {{ hash_key(['o.order_id']) }} as order_sk,
    o.order_id,
    coalesce(c.customer_sk, -1) as customer_sk,
    {{ date_key('o.order_time') }} as date_sk,
    coalesce(d.device_sk, -1) as device_sk,
    coalesce(s.source_sk, -1) as source_sk,
    o.payment_method,
    o.discount_pct,
    o.subtotal_usd,
    o.total_usd,
    o.country,
    o.order_time as order_timestamp,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_orders') }} as o
left join {{ ref('dim_click_customer') }} as c on o.customer_id = c.customer_id
left join {{ ref('dim_device') }} as d on o.device = d.device
left join {{ ref('dim_traffic_source') }} as s on o.source = s.traffic_source
