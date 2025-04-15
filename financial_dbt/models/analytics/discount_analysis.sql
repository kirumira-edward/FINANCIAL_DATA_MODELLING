{{
  config(
    materialized = 'table'
  )
}}

with discount_metrics as (
    select
        disc.discount_key,
        disc.discount_band,
        p.product_key,
        p.product_name,
        s.segment_key,
        s.segment_name,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        count(distinct f.transaction_id) as transaction_count
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_discount') }} disc on f.discount_key = disc.discount_key
    join {{ ref('dim_product') }} p on f.product_key = p.product_key
    join {{ ref('dim_segment') }} s on f.segment_key = s.segment_key
    where f.has_missing_keys = false  -- Exclude records with data quality issues
    group by 
        disc.discount_key,
        disc.discount_band,
        p.product_key,
        p.product_name,
        s.segment_key,
        s.segment_name
),

-- Product metrics without discounts for comparison
product_overall as (
    select
        p.product_key,
        sum(f.net_sales) as total_product_sales,
        sum(f.profit) as total_product_profit,
        sum(f.units_sold) as total_product_units
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_product') }} p on f.product_key = p.product_key
    where f.has_missing_keys = false
    group by p.product_key
)

select
    dm.discount_key,
    dm.discount_band,
    dm.product_key,
    dm.product_name,
    dm.segment_key,
    dm.segment_name,
    dm.gross_sales,
    dm.total_discounts,
    dm.net_sales,
    dm.total_cogs,
    dm.total_profit,
    dm.units_sold,
    dm.transaction_count,
    -- Calculated metrics
    round(dm.total_discounts / nullif(dm.gross_sales, 0) * 100, 2) as discount_pct,
    round(dm.total_profit / nullif(dm.net_sales, 0) * 100, 2) as profit_margin_pct,
    -- Product contribution with this discount
    round(dm.net_sales / nullif(po.total_product_sales, 0) * 100, 2) as pct_of_product_sales,
    round(dm.total_profit / nullif(po.total_product_profit, 0) * 100, 2) as pct_of_product_profit,
    round(dm.units_sold / nullif(po.total_product_units, 0) * 100, 2) as pct_of_product_units,
    -- Discount effectiveness
    round(dm.total_discounts / nullif(dm.units_sold, 0), 2) as discount_per_unit,
    round(dm.total_profit / nullif(dm.total_discounts, 0), 2) as profit_per_discount_dollar,
    -- Categorization
    case 
        when dm.total_profit > 0 and dm.total_discounts > 0 then
            round(dm.total_profit / nullif(dm.total_discounts, 0), 2)
        else 0
    end as roi_ratio,
    case
        when dm.total_profit <= 0 then 'Loss Making'
        when dm.total_profit / nullif(dm.total_discounts, 0) >= 3 then 'Highly Effective'
        when dm.total_profit / nullif(dm.total_discounts, 0) >= 1 then 'Effective'
        else 'Ineffective'
    end as discount_effectiveness
from discount_metrics dm
join product_overall po on dm.product_key = po.product_key
order by dm.discount_band, dm.total_profit desc