{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

with stg_financials as (
    select * from {{ ref('stg_raw_financials') }}
    {% if is_incremental() %}
    -- Only process new/changed records when running incrementally
    -- Using a static date condition for the first run to avoid circular reference
    where cast(load_datetime as date) > '{{ var("incremental_start_date", "2000-01-01") }}'::date
    {% endif %}
),

-- Get dimension keys by joining with dimension tables
with_keys as (
    select
        -- Surrogate key generation for transaction
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_date', 'stg.product_name', 'stg.segment_name', 'stg.country_name', 'stg.discount_band', 'stg.units_sold']) }} as transaction_id,
        
        -- Join with dimension tables to get keys
        dd.date_key,
        dp.product_key,
        ds.segment_key,
        dg.geography_key,
        disc.discount_key,
        
        -- Measures
        stg.units_sold,
        stg.sale_price,
        stg.gross_sales,
        stg.discounts,
        stg.net_sales,
        stg.cogs,
        -- Fix for NULL profits - recalculate if NULL
        coalesce(stg.profit, stg.net_sales - stg.cogs) as profit,
        
        -- Source timestamps and metadata
        stg.transaction_date,
        stg.load_datetime,
        stg.record_source
        
    from stg_financials stg
    -- Join with date dimension
    left join {{ ref('dim_date') }} dd
        on stg.transaction_date = dd.date_day
    
    -- Join with product dimension (using current products only)
    left join {{ ref('dim_product') }} dp
        on stg.product_name = dp.product_name
        and dp.is_current = true
    
    -- Join with segment dimension
    left join {{ ref('dim_segment') }} ds
        on stg.segment_name = ds.segment_name
    
    -- Join with geography dimension
    left join {{ ref('dim_geography') }} dg
        on stg.country_name = dg.country_name
    
    -- Join with discount dimension
    left join {{ ref('dim_discount') }} disc
        on stg.discount_band = disc.discount_band
),

-- Add a check for any missing dimension keys
validate_keys as (
    select
        transaction_id,
        date_key,
        product_key,
        segment_key,
        geography_key,
        discount_key,
        units_sold,
        sale_price,
        gross_sales,
        discounts,
        net_sales,
        cogs,
        profit,
        transaction_date,
        load_datetime as load_date,
        record_source,
        -- Flag any records with missing dimension keys for monitoring
        case when date_key is null or product_key is null or segment_key is null 
             or geography_key is null or discount_key is null
             then true else false end as has_missing_keys
    from with_keys
)

select
    transaction_id,
    date_key,
    product_key,
    segment_key,
    geography_key,
    discount_key,
    units_sold,
    sale_price,
    gross_sales,
    discounts,
    net_sales,
    cogs,
    profit,
    transaction_date,
    load_date,
    record_source,
    has_missing_keys
from validate_keys