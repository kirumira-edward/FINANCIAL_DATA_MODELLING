{{
  config(
    materialized = 'table'
  )
}}

with monthly_sales as (
    select
        d.year,
        d.month_number,
        d.month_name,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        count(distinct f.transaction_id) as transaction_count,
        sum(f.units_sold) as units_sold
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where f.has_missing_keys = false  -- Exclude records with data quality issues
    group by d.year, d.month_number, d.month_name
    order by d.year, d.month_number
),

with_previous_month as (
    select
        year,
        month_number,
        month_name,
        gross_sales,
        total_discounts,
        net_sales,
        total_cogs,
        total_profit,
        transaction_count,
        units_sold,
        lag(net_sales) over (order by year, month_number) as prev_month_net_sales,
        lag(total_profit) over (order by year, month_number) as prev_month_profit,
        lag(units_sold) over (order by year, month_number) as prev_month_units
    from monthly_sales
)

select
    year,
    month_number,
    month_name,
    gross_sales,
    total_discounts,
    net_sales,
    total_cogs,
    total_profit,
    transaction_count,
    units_sold,
    prev_month_net_sales,
    prev_month_profit,
    prev_month_units,
    -- Month-over-month changes
    net_sales - coalesce(prev_month_net_sales, 0) as net_sales_mom_change,
    case 
        when coalesce(prev_month_net_sales, 0) = 0 then null
        else round(100.0 * (net_sales - prev_month_net_sales) / prev_month_net_sales, 2)
    end as net_sales_mom_pct_change,
    
    total_profit - coalesce(prev_month_profit, 0) as profit_mom_change,
    case 
        when coalesce(prev_month_profit, 0) = 0 then null
        else round(100.0 * (total_profit - prev_month_profit) / prev_month_profit, 2)
    end as profit_mom_pct_change,
    
    units_sold - coalesce(prev_month_units, 0) as units_mom_change,
    case 
        when coalesce(prev_month_units, 0) = 0 then null
        else round(100.0 * (units_sold - prev_month_units) / prev_month_units, 2)
    end as units_mom_pct_change
from with_previous_month
order by year, month_number