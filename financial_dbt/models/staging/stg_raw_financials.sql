with source as (
    select * from {{ source('raw', 'raw_financials') }}
),

renamed as (
    select
        -- Dimension columns
        "Segment" as segment_name,
        "Country" as country_name,
        "Product" as product_name,
        "Discount Band" as discount_band,
        
        -- Measure columns
        "Units Sold" as units_sold,
        "Manufacturing Price" as manufacturing_price,
        "Sale Price" as sale_price,
        "Gross Sales" as gross_sales,
        "Discounts" as discounts,
        "Sales" as net_sales,
        "COGS" as cogs,
        "Profit" as profit,
        
        -- Date columns
        "Date" as transaction_date,
        "Month Number" as month_number,
        "Month Name" as month_name,
        "Year" as year,
        
        -- Metadata
        current_timestamp as load_datetime,
        'raw_financials' as record_source
    from source
),

cleaned as (
    select
        -- Clean and standardize dimensions
        trim(segment_name) as segment_name,
        trim(country_name) as country_name,
        trim(product_name) as product_name,
        trim(discount_band) as discount_band,
        
        -- Ensure measures are numeric
        cast(units_sold as numeric) as units_sold,
        cast(manufacturing_price as numeric) as manufacturing_price,
        cast(sale_price as numeric) as sale_price,
        cast(gross_sales as numeric) as gross_sales,
        cast(discounts as numeric) as discounts,
        cast(net_sales as numeric) as net_sales,
        cast(cogs as numeric) as cogs,
        cast(profit as numeric) as profit,
        
        -- Standardize dates
        cast(transaction_date as date) as transaction_date,
        cast(month_number as integer) as month_number,
        trim(month_name) as month_name,
        cast(year as integer) as year,
        
        -- Pass through metadata
        load_datetime,
        record_source
    from renamed
)

select * from cleaned