{{ config(properties=gold_properties()) }}

select
    {{ hash_key(['product_id']) }} as product_sk,
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    case
        when product_volume_cm3 < 1000 then 'SMALL'
        when product_volume_cm3 < 10000 then 'MEDIUM'
        when product_volume_cm3 < 50000 then 'LARGE'
        when product_volume_cm3 >= 50000 then 'EXTRA_LARGE'
        else 'UNKNOWN'
    end as size_category,
    case
        when product_weight_g < 500 then 'LIGHT'
        when product_weight_g < 2000 then 'MEDIUM'
        when product_weight_g < 10000 then 'HEAVY'
        when product_weight_g >= 10000 then 'VERY_HEAVY'
        else 'UNKNOWN'
    end as weight_category,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'olist_products') }}

union all

select
    -1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN',
    null, null, null, null, null, null, null, null,
    'UNKNOWN', 'UNKNOWN', current_timestamp
