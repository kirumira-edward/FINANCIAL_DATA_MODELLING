-- Data Quality Dashboard
-- This script creates a data quality summary view

-- Drop the view if it exists
DROP VIEW IF EXISTS staging.data_quality_dashboard;

-- Create a view with data quality metrics
CREATE VIEW staging.data_quality_dashboard AS

WITH dimension_counts AS (
    SELECT 
        'dim_date' AS dimension_table,
        COUNT(*) AS row_count,
        0 AS null_key_count,
        0 AS duplicate_key_count
    FROM staging.dim_date
    
    UNION ALL
    
    SELECT 
        'dim_product' AS dimension_table,
        COUNT(*) AS row_count,
        SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_key_count,
        COUNT(*) - COUNT(DISTINCT product_key) AS duplicate_key_count
    FROM staging.dim_product
    
    UNION ALL
    
    SELECT 
        'dim_segment' AS dimension_table,
        COUNT(*) AS row_count,
        SUM(CASE WHEN segment_key IS NULL THEN 1 ELSE 0 END) AS null_key_count,
        COUNT(*) - COUNT(DISTINCT segment_key) AS duplicate_key_count
    FROM staging.dim_segment
    
    UNION ALL
    
    SELECT 
        'dim_geography' AS dimension_table,
        COUNT(*) AS row_count,
        SUM(CASE WHEN geography_key IS NULL THEN 1 ELSE 0 END) AS null_key_count,
        COUNT(*) - COUNT(DISTINCT geography_key) AS duplicate_key_count
    FROM staging.dim_geography
    
    UNION ALL
    
    SELECT 
        'dim_discount' AS dimension_table,
        COUNT(*) AS row_count,
        SUM(CASE WHEN discount_key IS NULL THEN 1 ELSE 0 END) AS null_key_count,
        COUNT(*) - COUNT(DISTINCT discount_key) AS duplicate_key_count
    FROM staging.dim_discount
),

fact_metrics AS (
    SELECT
        'fact_financial_transactions' AS fact_table,
        COUNT(*) AS row_count,
        SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_key_count,
        COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_key_count,
        SUM(CASE WHEN has_missing_keys = true THEN 1 ELSE 0 END) AS records_with_missing_keys,
        SUM(CASE WHEN net_sales < 0 THEN 1 ELSE 0 END) AS negative_sales_count,
        SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END) AS negative_profit_count,
        SUM(CASE WHEN units_sold <= 0 AND net_sales > 0 THEN 1 ELSE 0 END) AS inconsistent_units_count
    FROM staging.fact_financial_transactions
),

referential_integrity AS (
    SELECT
        'date_dimension' AS relationship,
        COUNT(*) AS fact_count,
        SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS orphaned_records
    FROM staging.fact_financial_transactions f
    LEFT JOIN staging.dim_date d ON f.date_key = d.date_key
    
    UNION ALL
    
    SELECT
        'product_dimension' AS relationship,
        COUNT(*) AS fact_count,
        SUM(CASE WHEN p.product_key IS NULL THEN 1 ELSE 0 END) AS orphaned_records
    FROM staging.fact_financial_transactions f
    LEFT JOIN staging.dim_product p ON f.product_key = p.product_key
    
    UNION ALL
    
    SELECT
        'segment_dimension' AS relationship,
        COUNT(*) AS fact_count,
        SUM(CASE WHEN s.segment_key IS NULL THEN 1 ELSE 0 END) AS orphaned_records
    FROM staging.fact_financial_transactions f
    LEFT JOIN staging.dim_segment s ON f.segment_key = s.segment_key
    
    UNION ALL
    
    SELECT
        'geography_dimension' AS relationship,
        COUNT(*) AS fact_count,
        SUM(CASE WHEN g.geography_key IS NULL THEN 1 ELSE 0 END) AS orphaned_records
    FROM staging.fact_financial_transactions f
    LEFT JOIN staging.dim_geography g ON f.geography_key = g.geography_key
    
    UNION ALL
    
    SELECT
        'discount_dimension' AS relationship,
        COUNT(*) AS fact_count,
        SUM(CASE WHEN d.discount_key IS NULL THEN 1 ELSE 0 END) AS orphaned_records
    FROM staging.fact_financial_transactions f
    LEFT JOIN staging.dim_discount d ON f.discount_key = d.discount_key
),

analytical_consistency AS (
    SELECT
        'monthly_sales_vs_fact' AS consistency_check,
        ABS(f.fact_sales - m.monthly_sales) AS difference,
        CASE 
            WHEN ABS(f.fact_sales - m.monthly_sales) < 1 THEN 'Consistent'
            ELSE 'Inconsistent'
        END AS status
    FROM (
        SELECT SUM(net_sales) AS fact_sales FROM staging.fact_financial_transactions
    ) f
    CROSS JOIN (
        SELECT SUM(net_sales) AS monthly_sales FROM staging.monthly_sales_analysis
    ) m
    
    UNION ALL
    
    SELECT
        'product_profit_vs_fact' AS consistency_check,
        ABS(f.fact_profit - p.product_profit) AS difference,
        CASE 
            WHEN ABS(f.fact_profit - p.product_profit) < 1 THEN 'Consistent'
            ELSE 'Inconsistent'
        END AS status
    FROM (
        SELECT SUM(profit) AS fact_profit FROM staging.fact_financial_transactions
    ) f
    CROSS JOIN (
        SELECT SUM(total_profit) AS product_profit FROM staging.product_profitability
    ) p
    
    UNION ALL
    
    SELECT
        'segment_sales_vs_fact' AS consistency_check,
        ABS(f.fact_sales - s.segment_sales) AS difference,
        CASE 
            WHEN ABS(f.fact_sales - s.segment_sales) < 1 THEN 'Consistent'
            ELSE 'Inconsistent'
        END AS status
    FROM (
        SELECT SUM(net_sales) AS fact_sales FROM staging.fact_financial_transactions
    ) f
    CROSS JOIN (
        SELECT SUM(net_sales) AS segment_sales FROM staging.segment_performance
    ) s
),

-- Combine all metrics into one dashboard
combined_results AS (
    -- Dimension table metrics
    SELECT
        'Dimension Tables' AS category,
        dimension_table AS metric_name,
        row_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN row_count > 0 THEN 'Good' ELSE 'Error' END AS status
    FROM dimension_counts
    
    UNION ALL
    
    SELECT
        'Dimension Tables' AS category,
        dimension_table || ' - Null Keys' AS metric_name,
        null_key_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN null_key_count = 0 THEN 'Good' ELSE 'Error' END AS status
    FROM dimension_counts
    
    UNION ALL
    
    SELECT
        'Dimension Tables' AS category,
        dimension_table || ' - Duplicate Keys' AS metric_name,
        duplicate_key_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN duplicate_key_count = 0 THEN 'Good' ELSE 'Error' END AS status
    FROM dimension_counts
    
    UNION ALL
    
    -- Fact table metrics
    SELECT
        'Fact Table' AS category,
        fact_table AS metric_name,
        row_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN row_count > 0 THEN 'Good' ELSE 'Error' END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Null Keys' AS metric_name,
        null_key_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN null_key_count = 0 THEN 'Good' ELSE 'Error' END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Duplicate Keys' AS metric_name,
        duplicate_key_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN duplicate_key_count = 0 THEN 'Good' ELSE 'Error' END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Records With Missing Keys' AS metric_name,
        records_with_missing_keys::text AS metric_value,
        'Count' AS metric_type,
        CASE 
            WHEN records_with_missing_keys = 0 THEN 'Good'
            WHEN records_with_missing_keys > 0 AND records_with_missing_keys < row_count * 0.01 THEN 'Warning'
            ELSE 'Error'
        END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Negative Sales' AS metric_name,
        negative_sales_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN negative_sales_count = 0 THEN 'Good' ELSE 'Warning' END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Negative Profit' AS metric_name,
        negative_profit_count::text AS metric_value,
        'Count' AS metric_type,
        CASE 
            WHEN negative_profit_count = 0 THEN 'Good'
            WHEN negative_profit_count > 0 AND negative_profit_count < row_count * 0.05 THEN 'Warning'
            ELSE 'Error'
        END AS status
    FROM fact_metrics
    
    UNION ALL
    
    SELECT
        'Fact Table' AS category,
        fact_table || ' - Inconsistent Units' AS metric_name,
        inconsistent_units_count::text AS metric_value,
        'Count' AS metric_type,
        CASE WHEN inconsistent_units_count = 0 THEN 'Good' ELSE 'Warning' END AS status
    FROM fact_metrics
    
    UNION ALL
    
    -- Referential integrity metrics
    SELECT
        'Referential Integrity' AS category,
        relationship AS metric_name,
        orphaned_records::text AS metric_value,
        'Orphaned Records' AS metric_type,
        CASE WHEN orphaned_records = 0 THEN 'Good' ELSE 'Error' END AS status
    FROM referential_integrity
    
    UNION ALL
    
    -- Analytical model consistency
    SELECT
        'Analytical Consistency' AS category,
        consistency_check AS metric_name,
        difference::text AS metric_value,
        'Difference' AS metric_type,
        status
    FROM analytical_consistency
)

-- Final dashboard output
SELECT
    category,
    metric_name,
    metric_value,
    metric_type,
    status,
    CASE
        WHEN status = 'Good' THEN '✅'
        WHEN status = 'Warning' THEN '⚠️'
        WHEN status = 'Error' THEN '❌'
        ELSE '❓'
    END AS status_icon,
    CURRENT_TIMESTAMP AS checked_at
FROM combined_results
ORDER BY 
    CASE 
        WHEN category = 'Dimension Tables' THEN 1
        WHEN category = 'Fact Table' THEN 2
        WHEN category = 'Referential Integrity' THEN 3
        WHEN category = 'Analytical Consistency' THEN 4
        ELSE 5
    END,
    metric_name;