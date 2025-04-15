# Financial Data Pipeline Orchestration

This document explains how to set up and use the automated pipeline for the financial data warehousing project.

## Overview

The pipeline orchestration system automates the following steps:

1. Data extraction from source systems
2. Loading data into the staging area
3. Running dbt models to transform the data
   - First dimension tables
   - Then fact tables
4. Running data quality tests
5. Generating documentation (optional)
6. Sending notifications on success/failure

## Requirements

- Python 3.8+
- PostgreSQL database
- dbt Core installed and configured
- Email server (for notifications, optional)

## Pipeline Script Usage

The main script `run_financial_pipeline.py` can be run with various options:

```
python run_financial_pipeline.py [OPTIONS]
```

### Options

- `--full-refresh`: Perform a full refresh of all models instead of incremental loading
- `--skip-extract-load`: Skip the data extraction and loading step (useful for re-running transformations)
- `--skip-tests`: Skip running dbt tests (useful for development iterations)
- `--generate-docs`: Generate dbt documentation after running the pipeline

### Examples

Run the complete pipeline with incremental loading:
```
python run_financial_pipeline.py
```

Run a full refresh of all models:
```
python run_financial_pipeline.py --full-refresh
```

Skip data extraction and only run transformations:
```
python run_financial_pipeline.py --skip-extract-load
```

## Scheduling the Pipeline

### On Windows

1. Place the `schedule_pipeline.bat` file in your project directory
2. Open Task Scheduler
3. Create a Basic Task
4. Set trigger to Daily (e.g., 2:00 AM)
5. Set action to Start a Program
6. Program/script: Path to the batch file
7. Finish the wizard

### On Linux/Mac

1. Place the `schedule_pipeline.sh` file in your project directory
2. Make it executable: `chmod +x schedule_pipeline.sh`
3. Add a cron job by running `crontab -e` and adding:
   ```
   0 2 * * * /path/to/financial_data_modelling/schedule_pipeline.sh >> /path/to/financial_data_modelling/cron.log 2>&1
   ```
   This runs the pipeline daily at 2 AM

## Email Notifications

To enable email notifications:

1. Edit the `send_notification` function in `run_financial_pipeline.py`
2. Uncomment the email sending code
3. Configure your SMTP server settings
4. Update the recipient email addresses

## Monitoring and Logs

The pipeline creates detailed logs in:
- `pipeline.log`: Overall pipeline execution logs
- Standard dbt logs in the dbt project directory

## Troubleshooting

If the pipeline fails:

1. Check the logs for error messages
2. Verify database connectivity
3. Ensure the raw data files are in the expected location
4. Try running dbt commands manually to isolate issues

## Further Customization

The pipeline can be extended by:
- Adding more data sources
- Implementing more sophisticated error handling
- Setting up Slack/Teams notifications instead of email
- Adding data quality monitoring dashboards
- Implementing data lineage tracking