{{ config(properties=gold_properties()) }}

-- Grain: one row per clickstream event; session and customer come from the parent session.
select
    {{ hash_key(['e.event_id']) }} as event_sk,
    e.event_id,
    coalesce(s.session_sk, -1) as session_sk,
    coalesce(s.customer_sk, -1) as customer_sk,
    {{ date_key('e.event_timestamp') }} as date_sk,
    coalesce(p.product_sk, -1) as product_sk,
    e.event_type,
    e.quantity,
    e.payment_method,
    e.event_timestamp,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_events') }} as e
left join {{ ref('fact_sessions') }} as s on e.session_id = s.session_id
left join {{ ref('dim_click_product') }} as p on e.product_id = p.product_id
