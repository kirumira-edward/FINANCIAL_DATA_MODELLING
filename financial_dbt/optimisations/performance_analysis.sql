-- Performance Analysis Script

-- 1. Check table sizes
SELECT 
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_size,
    pg_relation_size(relid) AS table_size_bytes
FROM pg_catalog.pg_statio_user_tables
WHERE schemaname IN ('staging')  -- Replace with your schema name
ORDER BY pg_relation_size(relid) DESC;

-- 2. Check for missing indexes
SELECT
    schemaname || '.' || relname AS table_name,
    seq_scan AS sequential_scans,
    idx_scan AS index_scans,
    seq_scan - idx_scan AS difference,
    CASE 
        WHEN seq_scan - idx_scan > 0 THEN 'Consider adding index'
        ELSE 'OK' 
    END AS recommendation
FROM pg_stat_user_tables
WHERE schemaname IN ('staging')  -- Replace with your schema name
ORDER BY difference DESC;

-- 3. Analyze a typical query for the fact table - looking for full table scans
EXPLAIN ANALYZE
SELECT 
    d.year, 
    d.month_name, 
    SUM(f.net_sales) AS total_sales,
    SUM(f.profit) AS total_profit
FROM staging.fact_financial_transactions f
JOIN staging.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name
ORDER BY d.year, d.month_name;

-- 4. Analyze a typical query involving multiple dimensions
EXPLAIN ANALYZE
SELECT 
    p.product_name,
    s.segment_name,
    SUM(f.net_sales) AS total_sales,
    SUM(f.profit) AS total_profit
FROM staging.fact_financial_transactions f
JOIN staging.dim_product p ON f.product_key = p.product_key
JOIN staging.dim_segment s ON f.segment_key = s.segment_key
WHERE f.has_missing_keys = false
GROUP BY p.product_name, s.segment_name
ORDER BY total_profit DESC
LIMIT 10;

-- 5. Check for unused indexes
SELECT
    schemaname || '.' || relname AS table_name,
    indexrelname AS index_name,
    idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE schemaname IN ('staging')  -- Replace with your schema name
  AND idx_scan = 0
ORDER BY relname, indexrelname;