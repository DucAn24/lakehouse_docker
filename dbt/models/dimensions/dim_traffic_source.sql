{{ config(properties=gold_properties()) }}

-- Traffic sources seen in sessions or orders; a literal 'UNKNOWN' source resolves to the -1 member.
with sources as (
    select source from {{ source('silver', 'click_sessions') }}
    union
    select source from {{ source('silver', 'click_orders') }}
)

select
    {{ hash_key(['source']) }} as source_sk,
    source as traffic_source,
    current_timestamp as etl_loaded_at
from sources
where source is not null and source <> 'UNKNOWN'

union all

select -1, 'UNKNOWN', current_timestamp
