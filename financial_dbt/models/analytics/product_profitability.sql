{{
  config(
    materialized = 'table'
  )
}}

with product_metrics as (
    select
        p.product_key,
        p.product_name,
        p.manufacturing_price,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        count(distinct f.transaction_id) as transaction_count
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_product') }} p on f.product_key = p.product_key
    where f.has_missing_keys = false  -- Exclude records with data quality issues
    group by p.product_key, p.product_name, p.manufacturing_price
)

select
    product_key,
    product_name,
    manufacturing_price,
    gross_sales,
    total_discounts,
    net_sales,
    total_cogs,
    total_profit,
    units_sold,
    transaction_count,
    -- Calculated metrics
    round(total_profit / nullif(units_sold, 0), 2) as profit_per_unit,
    round(100.0 * total_profit / nullif(net_sales, 0), 2) as profit_margin_pct,
    round(net_sales / nullif(units_sold, 0), 2) as avg_sale_price,
    round(total_discounts / nullif(gross_sales, 0) * 100, 2) as discount_pct,
    -- Ranking metrics
    row_number() over (order by total_profit desc) as profit_rank,
    row_number() over (order by units_sold desc) as volume_rank,
    row_number() over (order by net_sales desc) as revenue_rank,
    -- Profit margin category
    case
        when total_profit / nullif(net_sales, 0) >= 0.2 then 'High Margin'
        when total_profit / nullif(net_sales, 0) >= 0.1 then 'Medium Margin'
        when total_profit / nullif(net_sales, 0) >= 0 then 'Low Margin'
        else 'Loss Making'
    end as profit_category
from product_metrics
order by total_profit desc