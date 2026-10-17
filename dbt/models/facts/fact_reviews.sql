{{ config(properties=gold_properties()) }}

-- Grain: one row per (review_id, order_id), the silver key.
select
    {{ hash_key(['r.review_id', 'r.order_id']) }} as review_sk,
    r.review_id,
    r.order_id,
    {{ date_key('r.review_creation_date') }} as date_sk,
    coalesce(dc.customer_sk, -1) as customer_sk,
    r.review_score,
    r.review_rating,
    r.review_comment_title,
    r.review_comment_message,
    r.has_comment as review_has_comment,
    r.review_response_time_hours,
    case when r.review_score >= 4 then 1 else 0 end as is_positive,
    case when r.review_score = 3 then 1 else 0 end as is_neutral,
    case when r.review_score <= 2 then 1 else 0 end as is_negative,
    r.review_creation_date,
    r.review_answer_timestamp,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'olist_order_reviews') }} as r
inner join {{ source('silver', 'olist_orders') }} as o on r.order_id = o.order_id
left join {{ source('silver', 'olist_customers') }} as c on o.customer_id = c.customer_id
left join {{ ref('dim_customer') }} as dc on c.customer_unique_id = dc.customer_unique_id
