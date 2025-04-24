import pandas as pd
from sqlalchemy import create_engine

# Load the cleaned data
df = pd.read_csv('financials_cleaned.csv')

# Connect to PostgreSQL
db_user = 'postgres'  # Update with your username
db_password = '12345'  # Update with your password
db_host = 'localhost'
db_name = 'financial_dwh'

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')

# Create raw table in PostgreSQL
df.to_sql('raw_financials', engine, schema='raw', if_exists='replace', index=False)
print("Data loaded to PostgreSQL raw.raw_financials table")

