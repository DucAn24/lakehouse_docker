{{ config(properties=gold_properties()) }}

-- Grain: one row per web session.
select
    {{ hash_key(['ss.session_id']) }} as session_sk,
    ss.session_id,
    coalesce(c.customer_sk, -1) as customer_sk,
    {{ date_key('ss.start_time') }} as date_sk,
    coalesce(d.device_sk, -1) as device_sk,
    coalesce(s.source_sk, -1) as source_sk,
    ss.country,
    ss.start_time as session_start_timestamp,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_sessions') }} as ss
left join {{ ref('dim_click_customer') }} as c on ss.customer_id = c.customer_id
left join {{ ref('dim_device') }} as d on ss.device = d.device
left join {{ ref('dim_traffic_source') }} as s on ss.source = s.traffic_source
