with source as (
    select distinct
        country_name
    from {{ ref('stg_raw_financials') }}
    where country_name is not null
),

-- For demo purposes, assigning simple regions
countries_with_regions as (
    select
        country_name,
        case
            when country_name in ('United States', 'Canada', 'Mexico') then 'North America'
            when country_name in ('France', 'Germany', 'UK', 'Italy', 'Spain') then 'Europe'
            when country_name in ('Japan', 'China', 'India', 'South Korea') then 'Asia'
            when country_name in ('Brazil', 'Argentina') then 'South America'
            when country_name in ('Australia', 'New Zealand') then 'Oceania'
            else 'Other'
        end as region,
        case
            when country_name in ('United States', 'Canada') then 'Northern America'
            when country_name = 'Mexico' then 'Central America'
            when country_name in ('France', 'Germany', 'UK', 'Italy', 'Spain') then 'Western Europe'
            when country_name in ('Japan', 'China', 'South Korea') then 'East Asia'
            when country_name = 'India' then 'South Asia'
            when country_name in ('Brazil', 'Argentina') then 'Southern South America'
            when country_name in ('Australia', 'New Zealand') then 'Australia and New Zealand'
            else 'Other'
        end as sub_region
    from source
),

final as (
    select
        row_number() over (order by country_name) as geography_key,
        country_name,
        region,
        sub_region,
        current_timestamp as created_at
    from countries_with_regions
)

select * from final