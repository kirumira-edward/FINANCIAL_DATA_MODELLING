

with source as (
    select distinct segment_name -- here we are taking unique segment values and then also handles SCD type 1
    from {{ ref ('stg_raw_financials')}}
    where segment_name is not null
),
segments as (
    select
    row_number() over(order by segment_name) as segment_key, --surrogate key created using the rownumber() window function since it assign unique numbers to every record
    segment_name,
    'Business segment for ' || segment_name as segment_description,
    current_timestamp as created_at,
    current_timestamp as updated_at

    from source
)

select
 segment_key,segment_name,segment_description,created_at,updated_at 
 from segments