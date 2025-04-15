import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure AWS credentials
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region
)

# Upload file to S3
def upload_file(file_path, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    try:
        s3_client.upload_file(file_path, bucket_name, f"raw/financials/{object_name}")
        print(f"Successfully uploaded {file_path} to {bucket_name}/raw/financials/{object_name}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

if __name__ == "__main__":
    file_path = r"C:\Users\OPTIMUS\financial_data_modelling\Financials.csv" 
    
    # Verify file exists before attempting upload
    if os.path.exists(file_path):
        upload_file(file_path)
    else:
        print(f"File not found at: {file_path}")
        print("Please check the file path and try again.")