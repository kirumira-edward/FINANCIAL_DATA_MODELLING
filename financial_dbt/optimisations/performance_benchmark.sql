-- Performance Benchmark Script

-- Record benchmark results in a temporary table
CREATE TEMPORARY TABLE benchmark_results (
    query_id TEXT,
    query_description TEXT,
    execution_time_ms NUMERIC,
    planning_time_ms NUMERIC,
    execution_time_with_cache_ms NUMERIC,
    planning_time_with_cache_ms NUMERIC
);

-- Benchmark Query 1: Simple aggregation on fact table
EXPLAIN ANALYZE 
WITH benchmark AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(net_sales) AS total_sales,
        SUM(profit) AS total_profit,
        AVG(profit) AS avg_profit
    FROM staging.fact_financial_transactions
)
SELECT * FROM benchmark;

-- Benchmark Query 2: Common join pattern with date dimension
EXPLAIN ANALYZE
WITH benchmark AS (
    SELECT 
        d.year,
        d.month_name,
        COUNT(*) AS transaction_count,
        SUM(f.net_sales) AS total_sales,
        SUM(f.profit) AS total_profit
    FROM staging.fact_financial_transactions f
    JOIN staging.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month_name
    ORDER BY d.year, d.month_name
)
SELECT * FROM benchmark;

-- Benchmark Query 3: Multiple dimension joins
EXPLAIN ANALYZE
WITH benchmark AS (
    SELECT 
        d.year,
        s.segment_name,
        p.product_name,
        SUM(f.net_sales) AS total_sales,
        SUM(f.profit) AS total_profit
    FROM staging.fact_financial_transactions f
    JOIN staging.dim_date d ON f.date_key = d.date_key
    JOIN staging.dim_segment s ON f.segment_key = s.segment_key
    JOIN staging.dim_product p ON f.product_key = p.product_key
    WHERE f.has_missing_keys = false
    GROUP BY d.year, s.segment_name, p.product_name
    ORDER BY d.year, total_profit DESC
    LIMIT 20
)
SELECT * FROM benchmark;

-- Benchmark Query 4: Filtering and aggregation
EXPLAIN ANALYZE
WITH benchmark AS (
    SELECT 
        g.region,
        g.country_name,
        SUM(f.net_sales) AS total_sales,
        SUM(f.profit) AS total_profit,
        COUNT(DISTINCT f.transaction_id) AS transaction_count
    FROM staging.fact_financial_transactions f
    JOIN staging.dim_geography g ON f.geography_key = g.geography_key
    JOIN staging.dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2017 AND f.has_missing_keys = false
    GROUP BY g.region, g.country_name
    ORDER BY total_profit DESC
    LIMIT 10
)
SELECT * FROM benchmark;

-- Benchmark Query 5: Complex query with multiple joins and filtering
EXPLAIN ANALYZE
WITH benchmark AS (
    SELECT 
        d.year,
        d.quarter,
        p.product_name,
        s.segment_name,
        g.region,
        disc.discount_band,
        SUM(f.net_sales) AS total_sales,
        SUM(f.profit) AS total_profit,
        SUM(f.profit) / NULLIF(SUM(f.net_sales), 0) * 100 AS profit_margin,
        SUM(f.units_sold) AS total_units
    FROM staging.fact_financial_transactions f
    JOIN staging.dim_date d ON f.date_key = d.date_key
    JOIN staging.dim_product p ON f.product_key = p.product_key
    JOIN staging.dim_segment s ON f.segment_key = s.segment_key
    JOIN staging.dim_geography g ON f.geography_key = g.geography_key
    JOIN staging.dim_discount disc ON f.discount_key = disc.discount_key
    WHERE f.has_missing_keys = false
      AND d.year BETWEEN 2016 AND 2017
      AND g.region = 'Europe'
    GROUP BY d.year, d.quarter, p.product_name, s.segment_name, g.region, disc.discount_band
    ORDER BY d.year, d.quarter, total_profit DESC
    LIMIT 20
)
SELECT * FROM benchmark;

-- Benchmark Query 6: Query using the analytical models
EXPLAIN ANALYZE
WITH benchmark AS (
    SELECT 
        pp.product_name,
        pp.total_profit,
        pp.profit_margin_pct,
        pp.profit_category,
        da.discount_band,
        da.discount_effectiveness
    FROM staging.product_profitability pp
    JOIN staging.discount_analysis da ON pp.product_name = da.product_name
    WHERE pp.profit_category = 'High Margin'
      AND da.discount_effectiveness = 'Highly Effective'
    ORDER BY pp.total_profit DESC
    LIMIT 10
)
SELECT * FROM benchmark;

-- After running all benchmarks, you can look at the results with:
-- SELECT * FROM benchmark_results ORDER BY query_id;