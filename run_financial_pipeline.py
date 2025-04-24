#!/usr/bin/env python
"""
Financial Data Pipeline Orchestration Script
This script coordinates the execution of the financial data pipeline,
including data extraction, loading, and transformation with dbt.
"""

import os
import sys
import time
import logging
import subprocess
import datetime
import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('financial_pipeline')

# Pipeline configuration
DBT_PROJECT_DIR = os.path.abspath('financial_dbt')
DATA_DIR = os.path.abspath('data')
INCREMENTAL_START_DATE = None  # Will be set based on last successful run


def send_notification(subject, message, recipients=None):
    """
    Send email notification about pipeline success/failure
    For demonstration purposes - in practice, configure with your email settings
    """
    if not recipients:
        recipients = ['edwardkirumira87@gmail.com']  # Default recipient
    
    logger.info(f"Would send notification: {subject}")
    logger.info(f"Message: {message}")
    logger.info(f"Recipients: {recipients}")
    
    # Comment out email sending for now to avoid connection errors
    """
    sender = "pipeline_alerts@yourcompany.com"
    
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(message, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.yourcompany.com', 587)
        server.starttls()
        server.login(sender, "your_password")
        server.send_message(msg)
        server.quit()
        logger.info("Email notification sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
    """


def run_command(command, cwd=None):
    """
    Execute a shell command and log output
    Returns True if successful, False otherwise
    """
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
        if retcode != 0:
            logger.error(f"Command failed with return code {retcode}")
            return False
        
        logger.info(f"Command completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return False


def extract_load_data():
    """
    Extract data from source system and load into staging area
    This would typically involve API calls, file downloads, etc.
    """
    logger.info("Starting data extraction and loading")
    
    # Example: In practice, this would be your actual data extraction code
    # For demonstration, we'll just check if a raw data file exists
    raw_financial_path = os.path.join(DATA_DIR, 'raw_financials.csv')
    
    if not os.path.exists(raw_financial_path):
        logger.error(f"Raw data file not found: {raw_financial_path}")
        return False
    
    # Example: Copy raw data to PostgreSQL using psql COPY command
    # Note: In a real scenario, you might use pandas, SQLAlchemy, etc.
    load_command = f"psql -h localhost -U postgres -d financial_dw -c \"\\COPY raw.raw_financials FROM '{raw_financial_path}' CSV HEADER\""
    success = run_command(load_command)
    
    if not success:
        logger.error("Data loading failed")
        return False
    
    logger.info("Data extraction and loading completed successfully")
    return True


def determine_incremental_date():
    """
    Determine the incremental start date for fact table loading
    """
    global INCREMENTAL_START_DATE
    
    # In a real implementation, this would query the database or a metadata table
    # to find the last successful load date
    try:
        # Example: Get the max load_date from fact table
        # This is simplified - in practice, use proper SQL client
        query_command = "psql -h localhost -U postgres -d financial_dw -t -c \"SELECT max(load_date) FROM financial_dm.fact_financial_transactions\""
        process = subprocess.Popen(
            query_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode == 0 and stdout.strip():
            INCREMENTAL_START_DATE = stdout.strip()
            logger.info(f"Using incremental start date: {INCREMENTAL_START_DATE}")
        else:
            # Default to yesterday if no date found
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            INCREMENTAL_START_DATE = yesterday.strftime('%Y-%m-%d')
            logger.info(f"No previous load date found, defaulting to: {INCREMENTAL_START_DATE}")
    
    except Exception as e:
        logger.error(f"Error determining incremental date: {e}")
        # Default to yesterday
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        INCREMENTAL_START_DATE = yesterday.strftime('%Y-%m-%d')
        logger.info(f"Using default incremental start date: {INCREMENTAL_START_DATE}")


def run_dbt_models(models=None, full_refresh=False):
    """
    Run dbt models with proper error handling
    """
    logger.info(f"Running dbt models: {models if models else 'all'}")
    
    # Build the dbt command
    dbt_cmd = "dbt run"
    
    if models:
        dbt_cmd += f" --models {models}"
    
    if full_refresh:
        dbt_cmd += " --full-refresh"
    
    # Simpler approach for vars that works on Windows
    if INCREMENTAL_START_DATE:
        dbt_cmd += f" --vars \"{{incremental_start_date: '{INCREMENTAL_START_DATE}'}}\"" 
    
    # Run the command
    success = run_command(dbt_cmd, cwd=DBT_PROJECT_DIR)
    
    if not success:
        logger.error("dbt model run failed")
        return False
    
    logger.info("dbt models completed successfully")
    return True
  


def run_dbt_tests(models=None):
    """
    Run dbt tests with proper error handling
    """
    logger.info(f"Running dbt tests: {models if models else 'all'}")
    
    # Build the dbt command
    dbt_cmd = "dbt test"
    
    if models:
        dbt_cmd += f" --models {models}"
    
    # Run the command
    success = run_command(dbt_cmd, cwd=DBT_PROJECT_DIR)
    
    if not success:
        logger.error("dbt tests failed")
        return False
    
    logger.info("dbt tests completed successfully")
    return True
  
def run_pipeline(args):
    """
    Run the complete data pipeline
    """
    start_time = time.time()
    pipeline_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Starting financial data pipeline at {pipeline_date}")
    
    try:
        # Step 1: Extract and load data (unless skipped)
        if not args.skip_extract_load:
            if not extract_load_data():
                raise Exception("Data extraction and loading failed")
        
        # Step 2: Determine incremental start date (if needed)
        if not args.full_refresh:
            determine_incremental_date()
        
        # Step 3: Run dbt models
        # First run dimensions, then facts
        if not run_dbt_models("dim_*", args.full_refresh):
            raise Exception("Dimension models failed")
        
        if not run_dbt_models("fact_*", args.full_refresh):
            raise Exception("Fact models failed")
        
        # Step 4: Run tests
        # In the run_pipeline function
        if not run_dbt_models("staging.dim_*", args.full_refresh):
         raise Exception("Dimension models failed")

        if not run_dbt_models("staging.fact_*", args.full_refresh):
         raise Exception("Fact models failed")
        
        # Step 5: Generate documentation (optional)
        if args.generate_docs:
            if not run_command("dbt docs generate", cwd=DBT_PROJECT_DIR):
                logger.warning("Documentation generation failed, but continuing pipeline")
        
        # Calculate duration
        duration = time.time() - start_time
        logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
        
        # Send success notification
        send_notification(
            "Financial Pipeline Completed Successfully",
            f"The financial data pipeline ran successfully at {pipeline_date}.\nDuration: {duration:.2f} seconds."
        )
        
        return True
    
    except Exception as e:
        # Calculate duration
        duration = time.time() - start_time
        error_msg = str(e)
        logger.error(f"Pipeline failed after {duration:.2f} seconds: {error_msg}")
        
        # Send failure notification
        send_notification(
            "Financial Pipeline Failed",
            f"The financial data pipeline failed at {pipeline_date}.\nDuration: {duration:.2f} seconds.\nError: {error_msg}",
            recipients=["your_email@example.com", "data_team@example.com"]  # Add multiple recipients for critical errors
        )
        
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Data Pipeline Orchestration")
    parser.add_argument("--full-refresh", action="store_true", help="Perform full refresh instead of incremental")
    parser.add_argument("--skip-extract-load", action="store_true", help="Skip data extraction and loading step")
    parser.add_argument("--skip-tests", action="store_true", help="Skip running dbt tests")
    parser.add_argument("--generate-docs", action="store_true", help="Generate dbt documentation")
    
    args = parser.parse_args()
    
    success = run_pipeline(args)
    sys.exit(0 if success else 1)