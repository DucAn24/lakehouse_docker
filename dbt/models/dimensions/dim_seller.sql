{{ config(properties=gold_properties()) }}

select
    {{ hash_key(['seller_id']) }} as seller_sk,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_region,
    seller_latitude,
    seller_longitude,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'olist_sellers') }}

union all

select -1, 'UNKNOWN', '00000', 'UNKNOWN', 'UNKNOWN', 'OUTROS', null, null, current_timestamp
