{{ config(properties=gold_properties()) }}

-- One row per person: a customer_unique_id gets a new customer_id for every order.
with customers as (
    select
        *,
        row_number() over (partition by customer_unique_id order by customer_id) as row_num
    from {{ source('silver', 'olist_customers') }}
)

select
    {{ hash_key(['customer_unique_id']) }} as customer_sk,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_region,
    customer_latitude,
    customer_longitude,
    current_timestamp as etl_loaded_at
from customers
where row_num = 1

union all

select -1, 'UNKNOWN', '00000', 'UNKNOWN', 'UNKNOWN', 'OUTROS', null, null, current_timestamp
