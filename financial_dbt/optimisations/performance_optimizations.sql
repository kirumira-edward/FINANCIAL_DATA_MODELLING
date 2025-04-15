-- Performance Optimization Script

-- 1. Add indexes to dimension tables
-- Primary key indexes are automatically created, but we'll add indexes for frequently joined columns

-- Date dimension - add index on date_day which is used for joining
CREATE INDEX IF NOT EXISTS idx_dim_date_date_day ON staging.dim_date (date_day);

-- Product dimension - add index on product_name for joins and is_current for SCD Type 2 filtering
CREATE INDEX IF NOT EXISTS idx_dim_product_name ON staging.dim_product (product_name);
CREATE INDEX IF NOT EXISTS idx_dim_product_current ON staging.dim_product (is_current) WHERE is_current = true;

-- Segment dimension - add index on segment_name
CREATE INDEX IF NOT EXISTS idx_dim_segment_name ON staging.dim_segment (segment_name);

-- Geography dimension - add indexes on country_name and region for joins and filtering
CREATE INDEX IF NOT EXISTS idx_dim_geography_country ON staging.dim_geography (country_name);
CREATE INDEX IF NOT EXISTS idx_dim_geography_region ON staging.dim_geography (region);

-- Discount dimension - add index on discount_band
CREATE INDEX IF NOT EXISTS idx_dim_discount_band ON staging.dim_discount (discount_band);

-- 2. Add indexes to fact table
-- First, add indexes for foreign keys to improve join performance
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON staging.fact_financial_transactions (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_key ON staging.fact_financial_transactions (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_segment_key ON staging.fact_financial_transactions (segment_key);
CREATE INDEX IF NOT EXISTS idx_fact_geography_key ON staging.fact_financial_transactions (geography_key);
CREATE INDEX IF NOT EXISTS idx_fact_discount_key ON staging.fact_financial_transactions (discount_key);

-- Add index for data quality filtering
CREATE INDEX IF NOT EXISTS idx_fact_has_missing_keys ON staging.fact_financial_transactions (has_missing_keys) WHERE has_missing_keys = false;

-- Add index for incremental processing
CREATE INDEX IF NOT EXISTS idx_fact_load_date ON staging.fact_financial_transactions (load_date);

-- Add composite index for common filtering scenarios (date + product)
CREATE INDEX IF NOT EXISTS idx_fact_date_product ON staging.fact_financial_transactions (date_key, product_key);

-- 3. Implement table partitioning (commented out - implement if dataset is large enough)
/*
-- First, create the partitioned table structure
CREATE TABLE staging.fact_financial_transactions_partitioned (
    transaction_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    segment_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    discount_key INTEGER NOT NULL,
    units_sold NUMERIC,
    sale_price NUMERIC,
    gross_sales NUMERIC,
    discounts NUMERIC,
    net_sales NUMERIC,
    cogs NUMERIC,
    profit NUMERIC,
    transaction_date DATE,
    load_date TIMESTAMP,
    record_source TEXT,
    has_missing_keys BOOLEAN
) PARTITION BY RANGE (date_key);

-- Create yearly partitions (adjust years as needed)
CREATE TABLE staging.fact_financial_transactions_y2014 
    PARTITION OF staging.fact_financial_transactions_partitioned 
    FOR VALUES FROM (20140101) TO (20150101);

CREATE TABLE staging.fact_financial_transactions_y2015 
    PARTITION OF staging.fact_financial_transactions_partitioned 
    FOR VALUES FROM (20150101) TO (20160101);

CREATE TABLE staging.fact_financial_transactions_y2016 
    PARTITION OF staging.fact_financial_transactions_partitioned 
    FOR VALUES FROM (20160101) TO (20170101);

CREATE TABLE staging.fact_financial_transactions_y2017 
    PARTITION OF staging.fact_financial_transactions_partitioned 
    FOR VALUES FROM (20170101) TO (20180101);

CREATE TABLE staging.fact_financial_transactions_y2018 
    PARTITION OF staging.fact_financial_transactions_partitioned 
    FOR VALUES FROM (20180101) TO (20190101);

-- Move data to the partitioned table
INSERT INTO staging.fact_financial_transactions_partitioned
SELECT * FROM staging.fact_financial_transactions;

-- Create indexes on partitions
CREATE INDEX ON staging.fact_financial_transactions_y2014 (product_key);
CREATE INDEX ON staging.fact_financial_transactions_y2015 (product_key);
CREATE INDEX ON staging.fact_financial_transactions_y2016 (product_key);
CREATE INDEX ON staging.fact_financial_transactions_y2017 (product_key);
CREATE INDEX ON staging.fact_financial_transactions_y2018 (product_key);
*/

-- 4. Update statistics to help the query planner
ANALYZE staging.fact_financial_transactions;
ANALYZE staging.dim_date;
ANALYZE staging.dim_product;
ANALYZE staging.dim_segment;
ANALYZE staging.dim_geography;
ANALYZE staging.dim_discount;

-- 5. Optimize analytical models for faster querying
-- Add indexes to the analytical models

-- Monthly Sales Analysis - index on year and month for time-series queries
CREATE INDEX IF NOT EXISTS idx_monthly_sales_year_month ON staging.monthly_sales_analysis (year, month_number);

-- Product Profitability - index on profitability metrics for ranking queries
CREATE INDEX IF NOT EXISTS idx_product_profit_margin ON staging.product_profitability (profit_margin_pct DESC);
CREATE INDEX IF NOT EXISTS idx_product_total_profit ON staging.product_profitability (total_profit DESC);

-- Segment Performance - index for filtering and grouping
CREATE INDEX IF NOT EXISTS idx_segment_performance_segment ON staging.segment_performance (segment_key, year, quarter);

-- Geography Performance - index for regional analysis
CREATE INDEX IF NOT EXISTS idx_geography_region_country ON staging.geography_performance (region, country_name);

-- Discount Analysis - index for filtering by effectiveness
CREATE INDEX IF NOT EXISTS idx_discount_effectiveness ON staging.discount_analysis (discount_effectiveness, total_profit DESC);

-- Executive Dashboard - index for filtering by category
CREATE INDEX IF NOT EXISTS idx_exec_dash_category ON staging.executive_dashboard (metric_category, profit DESC);

-- Update statistics on the analytical models
ANALYZE staging.monthly_sales_analysis;
ANALYZE staging.product_profitability;
ANALYZE staging.segment_performance;
ANALYZE staging.geography_performance;
ANALYZE staging.discount_analysis;
ANALYZE staging.executive_dashboard;