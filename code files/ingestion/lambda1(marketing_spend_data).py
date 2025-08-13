import boto3
import json
from datetime import datetime, timedelta

s3 = boto3.client('s3')

PUBLIC_BUCKET = 'placeholder' # company's public s3 bucket name
DEST_BUCKET = 'calendly-marketing-data'
DEST_PREFIX = 'raw/calendly/marketing/'

def lambda_handler(event, context):
    # Get yesterday's date (Day-1)
    target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    file_name = f"spend_data_{target_date}.json"
    source_key = f"calendly_spend_data/{file_name}"
    dest_key = f"{DEST_PREFIX}{file_name}"

    # Copy file from public to your bucket
    try:
        copy_source = {'Bucket': PUBLIC_BUCKET, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=DEST_BUCKET, Key=dest_key)
        return {'status': 'success', 'copied_file': dest_key}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
