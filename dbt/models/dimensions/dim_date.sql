{{ config(properties=gold_properties()) }}

-- One row per day over whole calendar years spanning every date a fact table keys on.
with fact_dates as (
    select order_purchase_timestamp as ts from {{ source('silver', 'olist_orders') }}
    union all
    select review_creation_date from {{ source('silver', 'olist_order_reviews') }}
    union all
    select start_time from {{ source('silver', 'click_sessions') }}
    union all
    select order_time from {{ source('silver', 'click_orders') }}
    union all
    select event_timestamp from {{ source('silver', 'click_events') }}
    union all
    select review_time from {{ source('silver', 'click_reviews') }}
),

years as (
    -- 2016-2020 until any fact has a date
    select year_number
    from (
        select
            coalesce(min(year(ts)), 2016) as first_year,
            coalesce(max(year(ts)), 2020) as last_year
        from fact_dates
    )
    cross join unnest(sequence(first_year, last_year)) as t (year_number)
),

days as (
    -- one sequence per year keeps each under Trino's 10,000-element sequence limit
    select full_date
    from years
    cross join unnest(sequence(
        date_add('year', year_number - 1970, date '1970-01-01'),
        date_add('day', -1, date_add('year', year_number - 1969, date '1970-01-01')),
        interval '1' day
    )) as t (full_date)
)

select
    {{ date_key('full_date') }} as date_sk,
    full_date,
    cast(day(full_date) as integer) as day_of_month,
    cast(day_of_week(full_date) % 7 + 1 as integer) as day_of_week,  -- 1 = Sunday ... 7 = Saturday
    date_format(full_date, '%W') as day_name,
    cast(week_of_year(full_date) as integer) as week_of_year,  -- ISO week
    cast(month(full_date) as integer) as month_number,
    date_format(full_date, '%M') as month_name,
    cast(quarter(full_date) as integer) as quarter,
    cast(year(full_date) as integer) as year,
    day_of_week(full_date) in (6, 7) as is_weekend,
    date_format(full_date, '%Y-%m') as year_month,
    cast(year(full_date) as varchar) || '-Q' || cast(quarter(full_date) as varchar) as year_quarter,
    current_timestamp as etl_loaded_at
from days
