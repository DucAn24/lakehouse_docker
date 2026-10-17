{{ config(properties=gold_properties()) }}

-- product_id -1 is the UNKNOWN member; silver click_events uses it for events without a product.
select
    {{ hash_key(['product_id']) }} as product_sk,
    product_id,
    product_category,
    product_name,
    price_usd,
    cost_usd,
    margin_usd,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_products') }}

union all

select -1, -1, 'UNKNOWN', 'UNKNOWN', null, null, null, current_timestamp
