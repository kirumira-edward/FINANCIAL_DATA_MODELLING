{{
  config(
    materialized = 'table'
  )
}}

-- Overall company performance by year and quarter
with company_performance as (
    select
        d.year,
        d.quarter,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.cogs) as total_cogs,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        count(distinct f.transaction_id) as transaction_count,
        round(100.0 * sum(f.discounts) / nullif(sum(f.gross_sales), 0), 2) as discount_pct,
        round(100.0 * sum(f.profit) / nullif(sum(f.net_sales), 0), 2) as profit_margin_pct
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where f.has_missing_keys = false
    group by d.year, d.quarter
    order by d.year, d.quarter
),

-- Previous period metrics for calculating period-over-period changes
with_previous_period as (
    select
        year,
        quarter,
        gross_sales,
        total_discounts,
        net_sales,
        total_cogs,
        total_profit,
        units_sold,
        transaction_count,
        discount_pct,
        profit_margin_pct,
        lag(net_sales) over (order by year, quarter) as prev_period_sales,
        lag(total_profit) over (order by year, quarter) as prev_period_profit,
        lag(profit_margin_pct) over (order by year, quarter) as prev_period_margin
    from company_performance
),

-- Top 5 products by profit
top_products as (
    select
        p.product_name,
        sum(f.net_sales) as net_sales,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        round(100.0 * sum(f.profit) / nullif(sum(f.net_sales), 0), 2) as profit_margin_pct
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_product') }} p on f.product_key = p.product_key
    where f.has_missing_keys = false
    group by p.product_name
    order by total_profit desc
    limit 5
),

-- Top 5 segments by profit
top_segments as (
    select
        s.segment_name,
        sum(f.net_sales) as net_sales,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        round(100.0 * sum(f.profit) / nullif(sum(f.net_sales), 0), 2) as profit_margin_pct
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_segment') }} s on f.segment_key = s.segment_key
    where f.has_missing_keys = false
    group by s.segment_name
    order by total_profit desc
    limit 5
),

-- Top 5 countries by profit
top_countries as (
    select
        g.country_name,
        g.region,
        sum(f.net_sales) as net_sales,
        sum(f.profit) as total_profit,
        sum(f.units_sold) as units_sold,
        round(100.0 * sum(f.profit) / nullif(sum(f.net_sales), 0), 2) as profit_margin_pct
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_geography') }} g on f.geography_key = g.geography_key
    where f.has_missing_keys = false
    group by g.country_name, g.region
    order by total_profit desc
    limit 5
),

-- Discount effectiveness summary
discount_effectiveness as (
    select
        disc.discount_band,
        sum(f.gross_sales) as gross_sales,
        sum(f.discounts) as total_discounts,
        sum(f.net_sales) as net_sales,
        sum(f.profit) as total_profit,
        round(100.0 * sum(f.discounts) / nullif(sum(f.gross_sales), 0), 2) as discount_pct,
        round(100.0 * sum(f.profit) / nullif(sum(f.net_sales), 0), 2) as profit_margin_pct,
        case
            when sum(f.profit) <= 0 then 'Loss Making'
            when sum(f.profit) / nullif(sum(f.discounts), 0) >= 3 then 'Highly Effective'
            when sum(f.profit) / nullif(sum(f.discounts), 0) >= 1 then 'Effective'
            else 'Ineffective'
        end as effectiveness
    from {{ ref('fact_financial_transactions') }} f
    join {{ ref('dim_discount') }} disc on f.discount_key = disc.discount_key
    where f.has_missing_keys = false
    group by disc.discount_band
    order by total_profit desc
),

-- Main dashboard with all metrics
combined_metrics as (
    -- Overall Performance
    select
        'Overall Performance' as metric_category,
        cp.year::text || '-Q' || cp.quarter::text as time_period,
        'Company' as dimension_value,
        cp.net_sales as revenue,
        cp.total_profit as profit,
        cp.profit_margin_pct as margin_pct,
        cp.units_sold as volume,
        -- Period-over-period changes
        case 
            when cp.prev_period_sales is not null 
            then round(100.0 * (cp.net_sales - cp.prev_period_sales) / nullif(cp.prev_period_sales, 0), 2)
            else null
        end as revenue_pop_pct,
        case 
            when cp.prev_period_profit is not null 
            then round(100.0 * (cp.total_profit - cp.prev_period_profit) / nullif(cp.prev_period_profit, 0), 2)
            else null
        end as profit_pop_pct,
        cp.profit_margin_pct - coalesce(cp.prev_period_margin, 0) as margin_pop_delta,
        1 as category_sort,  -- Sort key for Overall Performance
        cp.year * 10 + cp.quarter as time_sort,  -- Sort key for chronological order
        cp.total_profit as value_sort  -- Sort key for values
    from with_previous_period cp

    union all

    -- Top Products
    select
        'Top Products' as metric_category,
        'All Time' as time_period,
        p.product_name as dimension_value,
        p.net_sales as revenue,
        p.total_profit as profit,
        p.profit_margin_pct as margin_pct,
        p.units_sold as volume,
        null as revenue_pop_pct,
        null as profit_pop_pct,
        null as margin_pop_delta,
        2 as category_sort,  -- Sort key for Top Products
        0 as time_sort,  -- Not applicable for All Time
        p.total_profit as value_sort  -- Sort by profit
    from top_products p

    union all

    -- Top Segments
    select
        'Top Segments' as metric_category,
        'All Time' as time_period,
        s.segment_name as dimension_value,
        s.net_sales as revenue,
        s.total_profit as profit,
        s.profit_margin_pct as margin_pct,
        s.units_sold as volume,
        null as revenue_pop_pct,
        null as profit_pop_pct,
        null as margin_pop_delta,
        3 as category_sort,  -- Sort key for Top Segments
        0 as time_sort,  -- Not applicable for All Time
        s.total_profit as value_sort  -- Sort by profit
    from top_segments s

    union all

    -- Top Countries
    select
        'Top Countries' as metric_category,
        c.region as time_period,
        c.country_name as dimension_value,
        c.net_sales as revenue,
        c.total_profit as profit,
        c.profit_margin_pct as margin_pct,
        c.units_sold as volume,
        null as revenue_pop_pct,
        null as profit_pop_pct,
        null as margin_pop_delta,
        4 as category_sort,  -- Sort key for Top Countries
        0 as time_sort,  -- Not applicable for countries
        c.total_profit as value_sort  -- Sort by profit
    from top_countries c

    union all

    -- Discount Effectiveness
    select
        'Discount Effectiveness' as metric_category,
        d.effectiveness as time_period,
        d.discount_band as dimension_value,
        d.net_sales as revenue,
        d.total_profit as profit,
        d.profit_margin_pct as margin_pct,
        null as volume,
        null as revenue_pop_pct,
        null as profit_pop_pct,
        null as margin_pop_delta,
        5 as category_sort,  -- Sort key for Discount Effectiveness
        0 as time_sort,  -- Not applicable for discounts
        d.total_profit as value_sort  -- Sort by profit
    from discount_effectiveness d
)

-- Final query with proper ordering
select
    metric_category,
    time_period,
    dimension_value,
    revenue,
    profit,
    margin_pct,
    volume,
    revenue_pop_pct,
    profit_pop_pct,
    margin_pop_delta
from combined_metrics
order by 
    category_sort,
    time_sort,
    value_sort desc