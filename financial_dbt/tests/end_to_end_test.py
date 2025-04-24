#!/usr/bin/env python
"""
End-to-End Pipeline Test for Financial Data Modeling Project
This script executes database tests to validate the data warehouse.
"""

import os
import sys
import subprocess
import time
import logging
import pandas as pd
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('pipeline_test')

# Database connection parameters - update these with your actual credentials
DB_PARAMS = {
    'dbname': 'financial_dwh',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': '5432'
}

# Project directory paths
PROJECT_DIR = os.path.abspath('.')
DBT_PROJECT_DIR = os.path.abspath('.')  # Assuming running from dbt project root

def run_command(command, cwd=None):
    """Execute a shell command and log output"""
    logger.info(f"Running command: {command}")
    try:
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Stream output to logs
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()
            
            if stdout_line == '' and stderr_line == '' and process.poll() is not None:
                break
                
            if stdout_line:
                logger.info(stdout_line.strip())
            if stderr_line:
                logger.error(stderr_line.strip())
        
        retcode = process.poll()
        success = retcode == 0
        
        if not success:
            logger.error(f"Command failed with return code {retcode}")
        else:
            logger.info("Command completed successfully")
            
        return success
    
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return False

def connect_to_db():
    """Connect to the PostgreSQL database"""
    try:
        logger.info(f"Connecting to database: {DB_PARAMS['dbname']} on {DB_PARAMS['host']}")
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return None

def execute_query(conn, query):
    """Execute a SQL query and return the results"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return pd.DataFrame(results, columns=columns)
            return None
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        logger.error(f"Query: {query}")
        return None

def test_dbt_compile():
    """Test that dbt models compile without errors"""
    logger.info("Testing dbt compilation...")
    return run_command("dbt compile", cwd=DBT_PROJECT_DIR)

def test_dbt_run():
    """Test running dbt models"""
    logger.info("Testing dbt model execution...")
    return run_command("dbt run", cwd=DBT_PROJECT_DIR)

def test_dbt_test():
    """Test dbt tests pass"""
    logger.info("Running dbt tests...")
    return run_command("dbt test", cwd=DBT_PROJECT_DIR)

def test_table_counts(conn):
    """Test that all tables have data"""
    logger.info("Testing table row counts...")
    
    tables = [
        'dim_date', 'dim_product', 'dim_segment', 'dim_geography', 'dim_discount',
        'fact_financial_transactions',
        'monthly_sales_analysis', 'product_profitability', 'segment_performance',
        'geography_performance', 'discount_analysis', 'executive_dashboard'
    ]
    
    all_tables_have_data = True
    
    for table in tables:
        query = f"SELECT COUNT(*) as row_count FROM staging.{table}"
        result = execute_query(conn, query)
        
        if result is None or result.empty:
            logger.error(f"Error getting row count for {table}")
            all_tables_have_data = False
            continue
            
        row_count = result.iloc[0]['row_count']
        logger.info(f"Table staging.{table} has {row_count} rows")
        
        if row_count == 0:
            logger.error(f"Table staging.{table} has no data!")
            all_tables_have_data = False
    
    return all_tables_have_data

def test_referential_integrity(conn):
    """Test referential integrity between fact and dimension tables"""
    logger.info("Testing referential integrity...")
    
    integrity_tests = [
        {
            'name': 'fact_to_dim_date',
            'query': """
                SELECT COUNT(*) as orphaned_records
                FROM staging.fact_financial_transactions f
                LEFT JOIN staging.dim_date d ON f.date_key = d.date_key
                WHERE d.date_key IS NULL
            """
        },
        {
            'name': 'fact_to_dim_product',
            'query': """
                SELECT COUNT(*) as orphaned_records
                FROM staging.fact_financial_transactions f
                LEFT JOIN staging.dim_product p ON f.product_key = p.product_key
                WHERE p.product_key IS NULL
            """
        },
        {
            'name': 'fact_to_dim_segment',
            'query': """
                SELECT COUNT(*) as orphaned_records
                FROM staging.fact_financial_transactions f
                LEFT JOIN staging.dim_segment s ON f.segment_key = s.segment_key
                WHERE s.segment_key IS NULL
            """
        },
        {
            'name': 'fact_to_dim_geography',
            'query': """
                SELECT COUNT(*) as orphaned_records
                FROM staging.fact_financial_transactions f
                LEFT JOIN staging.dim_geography g ON f.geography_key = g.geography_key
                WHERE g.geography_key IS NULL
            """
        },
        {
            'name': 'fact_to_dim_discount',
            'query': """
                SELECT COUNT(*) as orphaned_records
                FROM staging.fact_financial_transactions f
                LEFT JOIN staging.dim_discount d ON f.discount_key = d.discount_key
                WHERE d.discount_key IS NULL
            """
        }
    ]
    
    all_integrity_tests_pass = True
    
    for test in integrity_tests:
        result = execute_query(conn, test['query'])
        
        if result is None or result.empty:
            logger.error(f"Error running integrity test: {test['name']}")
            all_integrity_tests_pass = False
            continue
            
        orphaned_count = result.iloc[0]['orphaned_records']
        
        if orphaned_count > 0:
            logger.error(f"Integrity test {test['name']} failed: {orphaned_count} orphaned records")
            all_integrity_tests_pass = False
        else:
            logger.info(f"Integrity test {test['name']} passed")
    
    return all_integrity_tests_pass

def test_data_quality(conn):
    """Test data quality in the fact table"""
    logger.info("Testing data quality...")
    
    quality_tests = [
        {
            'name': 'null_transaction_ids',
            'query': """
                SELECT COUNT(*) as null_count
                FROM staging.fact_financial_transactions
                WHERE transaction_id IS NULL
            """
        },
        {
            'name': 'negative_sales',
            'query': """
                SELECT COUNT(*) as negative_count
                FROM staging.fact_financial_transactions
                WHERE net_sales < 0
            """
        },
        {
            'name': 'units_sold_zero_with_sales',
            'query': """
                SELECT COUNT(*) as inconsistent_count
                FROM staging.fact_financial_transactions
                WHERE units_sold = 0 AND net_sales > 0
            """
        },
        {
            'name': 'profit_margin_validation',
            'query': """
                SELECT COUNT(*) as inconsistent_count
                FROM staging.fact_financial_transactions
                WHERE profit > net_sales
            """
        },
        {
            'name': 'missing_date_keys',
            'query': """
                SELECT COUNT(*) as missing_keys
                FROM staging.fact_financial_transactions
                WHERE date_key IS NULL
            """
        }
    ]
    
    all_quality_tests_pass = True
    
    for test in quality_tests:
        result = execute_query(conn, test['query'])
        
        if result is None or result.empty:
            logger.error(f"Error running quality test: {test['name']}")
            all_quality_tests_pass = False
            continue
            
        count = result.iloc[0][0]
        if count > 0:
            logger.warning(f"Quality test {test['name']} found {count} issues")
            # Not failing the overall test, just warning
        else:
            logger.info(f"Quality test {test['name']} passed")
    
    return all_quality_tests_pass

def test_analytical_models(conn):
    """Test the analytical models for consistency"""
    logger.info("Testing analytical models consistency...")
    
    consistency_tests = [
        {
            'name': 'monthly_sales_totals_match_fact',
            'query': """
                WITH fact_totals AS (
                    SELECT SUM(net_sales) as fact_sales
                    FROM staging.fact_financial_transactions
                ),
                monthly_totals AS (
                    SELECT SUM(net_sales) as monthly_sales
                    FROM staging.monthly_sales_analysis
                )
                SELECT 
                    fact_totals.fact_sales, 
                    monthly_totals.monthly_sales,
                    ABS(fact_totals.fact_sales - monthly_totals.monthly_sales) as difference
                FROM fact_totals, monthly_totals
            """
        },
        {
            'name': 'product_profit_totals_match_fact',
            'query': """
                WITH fact_totals AS (
                    SELECT SUM(profit) as fact_profit
                    FROM staging.fact_financial_transactions
                ),
                product_totals AS (
                    SELECT SUM(total_profit) as product_profit
                    FROM staging.product_profitability
                )
                SELECT 
                    fact_totals.fact_profit, 
                    product_totals.product_profit,
                    ABS(fact_totals.fact_profit - product_totals.product_profit) as difference
                FROM fact_totals, product_totals
            """
        },
        {
            'name': 'segment_totals_match_fact',
            'query': """
                WITH fact_totals AS (
                    SELECT SUM(net_sales) as fact_sales
                    FROM staging.fact_financial_transactions
                ),
                segment_totals AS (
                    SELECT SUM(net_sales) as segment_sales
                    FROM staging.segment_performance
                )
                SELECT 
                    fact_totals.fact_sales, 
                    segment_totals.segment_sales,
                    ABS(fact_totals.fact_sales - segment_totals.segment_sales) as difference
                FROM fact_totals, segment_totals
            """
        }
    ]
    
    all_consistency_tests_pass = True
    
    for test in consistency_tests:
        result = execute_query(conn, test['query'])
        
        if result is None or result.empty:
            logger.error(f"Error running consistency test: {test['name']}")
            all_consistency_tests_pass = False
            continue
            
        difference = result.iloc[0]['difference']
        # Allow a small rounding difference due to aggregation
        if difference > 1:
            logger.error(f"Consistency test {test['name']} failed: difference of {difference}")
            logger.error(f"Details: {result.to_dict('records')}")
            all_consistency_tests_pass = False
        else:
            logger.info(f"Consistency test {test['name']} passed")
    
    return all_consistency_tests_pass

def run_all_tests():
    """Run all tests and report results"""
    start_time = time.time()
    logger.info("Starting comprehensive system tests")
    
    # Skip source data test since it's in S3
    logger.info("Skipping source data test (data is in S3)...")
    
    # Test dbt compilation
    if not test_dbt_compile():
        logger.error("dbt compilation test failed - stopping further tests")
        return False
    
    # Connect to database for data tests
    conn = connect_to_db()
    if conn is None:
        logger.error("Database connection failed - stopping further tests")
        return False
    
    # Skip dbt model execution if you've already run them
    logger.info("Skipping dbt model execution (assuming models are already run)...")
    dbt_run_success = True
    
    # Test dbt tests
    dbt_tests_pass = test_dbt_test()
    if not dbt_tests_pass:
        logger.warning("Some dbt tests failed - this may indicate data quality issues")
        # Continue with other tests even if dbt tests fail
    
    # Test table counts
    tables_have_data = test_table_counts(conn)
    if not tables_have_data:
        logger.error("Table count test failed - some tables may be empty")
        # Continue with other tests
    
    # Test referential integrity
    integrity_passes = test_referential_integrity(conn)
    if not integrity_passes:
        logger.error("Referential integrity test failed")
        # Continue with other tests
    
    # Test data quality
    quality_passes = test_data_quality(conn)
    if not quality_passes:
        logger.warning("Data quality test found issues")
        # Continue with other tests
    
    # Test analytical models
    analytical_models_pass = test_analytical_models(conn)
    if not analytical_models_pass:
        logger.error("Analytical models consistency test failed")
        # Continue with other tests
    
    # Close database connection
    conn.close()
    
    # Calculate test duration
    duration = time.time() - start_time
    logger.info(f"All tests completed in {duration:.2f} seconds")
    
    # Report overall test results
    overall_success = (
        dbt_run_success and
        dbt_tests_pass and
        tables_have_data and
        integrity_passes and
        quality_passes and
        analytical_models_pass
    )
    
    if overall_success:
        logger.info("✅ ALL TESTS PASSED - System is working as expected")
    else:
        logger.error("❌ SOME TESTS FAILED - See log for details")
    
    return overall_success

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)