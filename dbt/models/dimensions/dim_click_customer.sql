{{ config(properties=gold_properties()) }}

select
    {{ hash_key(['customer_id']) }} as customer_sk,
    customer_id,
    customer_name,
    email,
    country,
    age,
    signup_date,
    marketing_opt_in,
    current_timestamp as etl_loaded_at
from {{ source('silver', 'click_customers') }}

union all

select -1, -1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', null, null, false, current_timestamp
