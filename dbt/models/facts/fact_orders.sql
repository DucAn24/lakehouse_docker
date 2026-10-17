{{ config(properties=gold_properties()) }}

-- Grain: one row per order item.
with payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        sum(payment_installments) as payment_installments,
        min_by(payment_type, payment_sequential) as primary_payment_type
    from {{ source('silver', 'olist_order_payments') }}
    group by order_id
),

items as (
    select
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,
        i.price,
        i.freight_value,
        i.total_item_value,
        i.shipping_limit_date,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        o.is_delivered,
        o.is_delivered_late,
        o.actual_delivery_days,
        o.delivery_delay_days,
        c.customer_unique_id
    from {{ source('silver', 'olist_order_items') }} as i
    inner join {{ source('silver', 'olist_orders') }} as o on i.order_id = o.order_id
    -- an order's customer_id -> the person (customer_unique_id) dim_customer is keyed on
    left join {{ source('silver', 'olist_customers') }} as c on o.customer_id = c.customer_id
)

select
    {{ hash_key(['items.order_id', 'items.order_item_id']) }} as order_item_sk,
    items.order_id,
    items.order_item_id,
    {{ date_key('items.order_purchase_timestamp') }} as date_sk,
    coalesce(dc.customer_sk, -1) as customer_sk,
    coalesce(dp.product_sk, -1) as product_sk,
    coalesce(ds.seller_sk, -1) as seller_sk,
    items.order_status,
    case
        when items.order_status = 'DELIVERED' then 'COMPLETED'
        when items.order_status in ('SHIPPED', 'INVOICED', 'PROCESSING', 'APPROVED') then 'IN_PROGRESS'
        when items.order_status = 'CREATED' then 'PENDING'
        when items.order_status in ('CANCELED', 'UNAVAILABLE') then 'FAILED'
        else 'OTHER'
    end as status_category,
    items.is_delivered,
    items.is_delivered_late,
    items.price as unit_price,
    items.freight_value,
    items.total_item_value,
    items.actual_delivery_days,
    items.delivery_delay_days,
    coalesce(p.total_payment_value, 0) as total_payment_value,
    cast(coalesce(p.payment_installments, 0) as integer) as payment_installments,
    coalesce(p.primary_payment_type, 'UNKNOWN') as primary_payment_type,
    items.order_purchase_timestamp,
    items.order_approved_at,
    items.order_delivered_customer_date,
    items.shipping_limit_date,
    current_timestamp as etl_loaded_at
from items
left join payments as p on items.order_id = p.order_id
left join {{ ref('dim_customer') }} as dc on items.customer_unique_id = dc.customer_unique_id
left join {{ ref('dim_product') }} as dp on items.product_id = dp.product_id
left join {{ ref('dim_seller') }} as ds on items.seller_id = ds.seller_id
