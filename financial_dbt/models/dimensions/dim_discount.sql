with source as (
    select distinct
        discount_band
    from {{ ref('stg_raw_financials') }}
    where discount_band is not null
),

-- Calculate approximate discount ranges based on the band names
-- gives every discount band a number 
-- and also assigns a percentage to it
discount_with_ranges as (
    select
        discount_band,
        case-- rannge minimum
            when discount_band = 'None' then 0
            when discount_band = 'Low' then 1
            when discount_band = 'Medium' then 15
            when discount_band = 'High' then 30
            else 0
        end as discount_range_min,
        case-- range maximum
            when discount_band = 'None' then 0
            when discount_band = 'Low' then 15
            when discount_band = 'Medium' then 30
            when discount_band = 'High' then 50
            else 0
        end as discount_range_max,
        case-- giving percentages and adds some description
            when discount_band = 'None' then 'No discount applied'
            when discount_band = 'Low' then 'Small discount between 1-15%'
            when discount_band = 'Medium' then 'Medium discount between 15-30%'
            when discount_band = 'High' then 'High discount between 30-50%'
            else 'Unknown discount type'
        end as discount_description
    from source
),

final as (
    select
        row_number() over (order by discount_band) as discount_key,
        discount_band,
        discount_range_min,
        discount_range_max,
        discount_description,
        current_timestamp as created_at
    from discount_with_ranges
)

select * from final