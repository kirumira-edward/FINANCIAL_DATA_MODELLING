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

2. Run the initial exploration notebook: