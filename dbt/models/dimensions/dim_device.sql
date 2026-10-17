{{ config(properties=gold_properties()) }}

-- Devices seen in sessions or orders; a literal 'UNKNOWN' device resolves to the -1 member.
with devices as (
    select device from {{ source('silver', 'click_sessions') }}
    union
    select device from {{ source('silver', 'click_orders') }}
)

select
    {{ hash_key(['device']) }} as device_sk,
    device,
    current_timestamp as etl_loaded_at
from devices
where device is not null and device <> 'UNKNOWN'

union all

select -1, 'UNKNOWN', current_timestamp
