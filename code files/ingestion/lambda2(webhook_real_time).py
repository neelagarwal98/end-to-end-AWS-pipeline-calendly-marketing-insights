import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'calendly-marketing-data'

def lambda_handler(event, context):
    # print("Incoming event:", json.dumps(event, indent=2))
    method = event.get("requestContext", {}).get("http", {}).get("method", "POST")

    if method == "GET":
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Webhook endpoint is alive"})
        }

    elif method == "POST":
        try:
            body = event.get('body')
            if isinstance(body, str):
                body = json.loads(body)

            today = datetime.utcnow().strftime('%Y/%m/%d')
            file_key = f"raw/calendly/real-time/{today}/{uuid.uuid4()}.json"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_key,
                Body=json.dumps(body, indent=2)
            )

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Webhook event stored"})
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(e)})
            }

    return {
        "statusCode": 405,
        "body": json.dumps({"error": "Method not allowed"})
    }
