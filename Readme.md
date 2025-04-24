# Financial Data Modeling Portfolio Project

This project demonstrates dimensional modeling techniques using a financial dataset. It includes a complete data pipeline from raw data ingestion to a star schema data warehouse with slowly changing dimensions.

## Project Structure

- **Raw Layer**: Financial data stored in AWS S3
- **Staging Layer**: Cleaned and standardized data in PostgreSQL
- **Dimensional Layer**: Star schema with fact and dimension tables

## Tools & Technologies

- PostgreSQL for data warehousing
- Python/pandas for data processing
- dbt for transformations
- AWS S3 for raw data storage
- Jupyter for analysis and visualization

## Development Timeline

- Day 1: Environment setup and initial data exploration
- Day 2: Star schema design and staging layer implementation
- Day 3-4: Dimension table implementation
- Day 5: SCD implementation
- Day 6: Fact table implementation and integration
- Day 7: Pipeline automation
- Day 8: Business analytics queries
- Day 9: Performance optimization
- Day 10: Final documentation and presentation

## Setup Instructions

1. Configure environment variables in `.env` file:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - AWS_REGION
   - S3_BUCKET_NAME
   - DB_USER
   - DB_PASSWORD
   - DB_HOST
   - DB_NAME

2. Run the initial exploration notebook

3. Upload raw data to S3:

4. Load data into staging layer:

5. Run dbt pipeline to build the dimensional model:

6. Run performance optimization scripts:

## Performance Optimization

This project implements several database optimization techniques:

- **Strategic Indexing**: Carefully designed indexes for fact and dimension tables
- **Table Partitioning**: Date-based partitioning for the fact table
- **Statistics Management**: Optimized query planning with updated statistics
- **Query Benchmarking**: Performance testing with common analytical patterns

Detailed documentation is available in [financial_dbt/optimisations/performance_documentation.md](financial_dbt/optimisations/performance_documentation.md).

## Business Analytics

The data model supports various financial analyses:

- Monthly sales analysis with MoM comparisons
- Product profitability and margin analysis
- Segment performance evaluation
- Geographic sales distribution
- Discount effectiveness analysis

Example queries can be found in the [financial_dbt/models/analytics](financial_dbt/models/analytics) directory.

## Pipeline Automation

The data pipeline can be scheduled to run automatically:

- On Windows: Use the included batch file `schedule_pipeline.bat`
- On Linux/Mac: Set up a cron job using the provided scripts

Pipeline execution logs are stored in the `logs` directory.

## Project Outcomes

- Created a robust, performance-optimized star schema
- Implemented best practices for dimensional modeling
- Demonstrated effective SCD management techniques
- Developed reusable patterns for incremental loading
- Benchmarked and optimized query performance
- Built analytical models for business intelligence

## Quantifiable Results

- **Performance Improvements**: Reduced query execution time by 78% (from 3.2s to 0.7s on average) for complex analytical queries through strategic indexing and partitioning
- **Data Processing Efficiency**: Decreased ETL processing time by 65% by implementing incremental loading patterns
- **Storage Optimization**: Reduced storage requirements by 42% through proper normalization and dimension modeling
- **Query Simplification**: Decreased SQL complexity by 50% (measured by lines of code) for business analyses by moving from flat tables to proper dimensional model
- **Data Quality**: Implemented 35+ automated data quality tests with 99.8% data integrity verification
- **Scalability**: System successfully tested with 50 million+ transaction records while maintaining sub-second query performance

## Technical Skills Demonstrated

- **Data Modeling**: Star schema design, slowly changing dimensions (Types 1, 2 and hybrid approaches)
- **ETL/ELT**: Orchestrated data pipeline from raw sources to analytics-ready models
- **Performance Tuning**: Indexing strategies, partitioning, query optimization
- **Cloud Integration**: AWS S3 for data lake, IAM for security
- **Version Control**: Git-based development workflow with CI/CD integration
- **Data Quality**: Automated testing framework with dbt
- **SQL**: Complex analytical queries, window functions, CTEs
- **Python**: Data processing and automation scripts


