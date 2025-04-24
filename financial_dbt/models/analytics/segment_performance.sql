{{
  config(
    materialized = 'table'
  )
}}

with segment_metrics as (
    select
        s.segment_key,
        s.segment_name,
        d.year,
        d.quarter,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        count(distinct f.transaction_id) as transaction_count
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_segment') }} s on f.segment_key = s.segment_key
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where f.has_missing_keys = false  -- Exclude records with data quality issues
    group by s.segment_key, s.segment_name, d.year, d.quarter
),

-- Get yearly totals for calculating segment contribution
yearly_totals as (
    select
        year,
        sum(net_sales) as total_year_sales,
        sum(total_profit) as total_year_profit,
        sum(units_sold) as total_year_units
    from segment_metrics
    group by year
)

select
    sm.segment_key,
    sm.segment_name,
    sm.year,
    sm.quarter,
    sm.gross_sales,
    sm.total_discounts,
    sm.net_sales,
    sm.total_cogs,
    sm.total_profit,
    sm.units_sold,
    sm.transaction_count,
    -- Calculated metrics
    round(sm.total_profit / nullif(sm.net_sales, 0) * 100, 2) as profit_margin_pct,
    round(sm.total_discounts / nullif(sm.gross_sales, 0) * 100, 2) as discount_pct,
    -- Segment contribution metrics
    round(sm.net_sales / nullif(yt.total_year_sales, 0) * 100, 2) as sales_contribution_pct,
    round(sm.total_profit / nullif(yt.total_year_profit, 0) * 100, 2) as profit_contribution_pct,
    -- Quarter-over-quarter growth
    lag(sm.net_sales) over (partition by sm.segment_key, sm.year order by sm.quarter) as prev_quarter_sales,
    case
        when lag(sm.net_sales) over (partition by sm.segment_key, sm.year order by sm.quarter) is not null then
            round(100.0 * (sm.net_sales - lag(sm.net_sales) over (partition by sm.segment_key, sm.year order by sm.quarter)) 
            / nullif(lag(sm.net_sales) over (partition by sm.segment_key, sm.year order by sm.quarter), 0), 2)
        else null
    end as sales_qoq_growth_pct
from segment_metrics sm
join yearly_totals yt on sm.year = yt.year
order by sm.year, sm.quarter, sm.total_profit desc