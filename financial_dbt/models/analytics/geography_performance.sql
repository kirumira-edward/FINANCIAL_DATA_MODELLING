{{
  config(
    materialized = 'table'
  )
}}

with geo_metrics as (
    select
        g.geography_key,
        g.country_name,
        g.region,
        d.year,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        count(distinct f.transaction_id) as transaction_count
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_geography') }} g on f.geography_key = g.geography_key
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where f.has_missing_keys = false  -- Exclude records with data quality issues
    group by g.geography_key, g.country_name, g.region, d.year
),

-- Get region totals for calculating country contribution within region
region_totals as (
    select
        region,
        year,
        sum(net_sales) as total_region_sales,
        sum(total_profit) as total_region_profit
    from geo_metrics
    group by region, year
),

-- Previous year metrics for calculating YoY growth
prev_year_metrics as (
    select
        geography_key,
        country_name,
        year + 1 as next_year,
        net_sales as prev_year_sales,
        total_profit as prev_year_profit,
        units_sold as prev_year_units
    from geo_metrics
)

select
    gm.geography_key,
    gm.country_name,
    gm.region,
    gm.year,
    gm.gross_sales,
    gm.total_discounts,
    gm.net_sales,
    gm.total_cogs,
    gm.total_profit,
    gm.units_sold,
    gm.transaction_count,
    -- Calculated metrics
    round(gm.total_profit / nullif(gm.net_sales, 0) * 100, 2) as profit_margin_pct,
    round(gm.total_discounts / nullif(gm.gross_sales, 0) * 100, 2) as discount_pct,
    -- Region contribution
    round(gm.net_sales / nullif(rt.total_region_sales, 0) * 100, 2) as region_sales_contribution_pct,
    round(gm.total_profit / nullif(rt.total_region_profit, 0) * 100, 2) as region_profit_contribution_pct,
    -- Year-over-year growth
    py.prev_year_sales,
    py.prev_year_profit,
    case
        when py.prev_year_sales is not null then
            round(100.0 * (gm.net_sales - py.prev_year_sales) / nullif(py.prev_year_sales, 0), 2)
        else null
    end as sales_yoy_growth_pct,
    case
        when py.prev_year_profit is not null then
            round(100.0 * (gm.total_profit - py.prev_year_profit) / nullif(py.prev_year_profit, 0), 2)
        else null
    end as profit_yoy_growth_pct,
    -- Ranking
    row_number() over (partition by gm.year order by gm.total_profit desc) as overall_profit_rank,
    row_number() over (partition by gm.region, gm.year order by gm.total_profit desc) as region_profit_rank
from geo_metrics gm
join region_totals rt on gm.region = rt.region and gm.year = rt.year
left join prev_year_metrics py on gm.geography_key = py.geography_key and gm.year = py.next_year
order by gm.year, gm.total_profit desc