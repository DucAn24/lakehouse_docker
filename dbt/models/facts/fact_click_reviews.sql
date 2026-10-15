{{ config(properties=gold_properties()) }}

-- Grain: one row per clickstream product review.
select
    {{ hash_key(['r.review_id']) }} as review_sk,
    r.review_id,
    coalesce(o.order_sk, -1) as order_sk,
    coalesce(p.product_sk, -1) as product_sk,
    {{ date_key('r.review_time') }} as date_sk,
    r.rating,
    r.review_text,
    r.sentiment,
    r.review_time as review_timestamp,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_reviews') }} as r
left join {{ ref('fact_click_orders') }} as o on r.order_id = o.order_id
left join {{ ref('dim_click_product') }} as p on r.product_id = p.product_id
