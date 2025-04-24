with source as (
    select distinct 
        product_name,
        manufacturing_price,
        transaction_date
    from {{ ref('stg_raw_financials') }}
    where product_name is not null
),

-- Get the earliest date for each product-price combination
product_price_periods as (
    select
        product_name,
        manufacturing_price,
        min(transaction_date) as effective_date
    from source
    group by product_name, manufacturing_price
),

-- Create records with effective dates and end dates for SCD Type 2
products_with_dates as (
    select
        product_name,
        manufacturing_price,
        effective_date,
        lead(effective_date, 1, '9999-12-31'::date) over (
            partition by product_name
            order by effective_date
        ) - interval '1 day' as end_date
    from product_price_periods
),

-- Add surrogate keys and current flag
final as (
    select
        row_number() over (order by product_name, effective_date) as product_key,
        product_name,
        manufacturing_price,
        effective_date,
        end_date,
        case 
            when end_date = '9999-12-30'::date then true
            else false
        end as is_current,
        current_timestamp as created_at
    from products_with_dates
)

select * from final