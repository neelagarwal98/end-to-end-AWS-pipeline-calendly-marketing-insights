import json
import boto3
import urllib3
import time
from datetime import datetime, timezone
from dateutil import parser as dateparser

# AWS Clients
s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Constants
BUCKET = "calendly-marketing-data"
TRACKER_KEY = "metadata/calendly_ingestion_tracker.json"
BASE_PATH = "raw/calendly/historical"
ORG_URL = "https://api.calendly.com/organizations/8910725e-6409-44dc-ba57-ead6dd48b052"
SCHEDULED_EVENTS_ENDPOINT = 'https://api.calendly.com/scheduled_events'

# Secret retrieval
def get_token(secret_name='calendly-api-token'):
    resp = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])['CalendlyAPIToken']

# Tracker management
def get_last_ingested():
    try:
        response = s3.get_object(Bucket=BUCKET, Key=TRACKER_KEY)
        content = json.loads(response['Body'].read().decode('utf-8'))
        return content.get('last_ingested')
    except s3.exceptions.NoSuchKey:
        return None

def update_tracker(timestamp):
    content = {"last_ingested": timestamp}
    s3.put_object(Bucket=BUCKET, Key=TRACKER_KEY, Body=json.dumps(content))

# Retry helper
def safe_get(url, headers, retries=5):
    for i in range(retries):
        try:
            res = http.request("GET", url, headers=headers, timeout=urllib3.Timeout(connect=5.0, read=60.0))
            if res.status == 200:
                return json.loads(res.data.decode("utf-8"))
            else:
                print(f"! HTTP {res.status} for {url}")
        except Exception as e:
            print(f"! Network error: {e}")
        time.sleep(2 ** i)  # exponential backoff
    print(f"!!! Failed to fetch after {retries} retries: {url}")
    return None

# Main logic
def run_ingestion():
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    last_ts = get_last_ingested()
    last_ingested_dt = dateparser.isoparse(last_ts) if last_ts else None
    print(f"> Last ingested: {last_ingested_dt}")

    url = f"{SCHEDULED_EVENTS_ENDPOINT}?organization={ORG_URL}&count=100"
    page = 1
    max_event_dt = last_ingested_dt
    new_events_found = False

    while url:
        print(f">>> Fetching page {page}: {url}")
        payload = safe_get(url, headers)
        if not payload:
            print(f"!!! Skipping page {page} due to fetch failure.")
            break

        events = payload.get("collection", [])
        enriched = []

        for ev in events:
            try:
                ev_dt = dateparser.isoparse(ev["created_at"])
            except Exception:
                continue

            if last_ingested_dt and ev_dt <= last_ingested_dt:
                continue

            detail_url = ev.get("uri")
            detail_payload = safe_get(detail_url, headers)
            if detail_payload:
                enriched_event = detail_payload.get("resource")
                if enriched_event:
                    enriched.append(enriched_event)
                    max_event_dt = max(max_event_dt or ev_dt, ev_dt)
                    new_events_found = True

        if enriched:
            dt = datetime.now(timezone.utc)
            prefix = f"{BASE_PATH}/{dt.strftime('%Y/%m/%d')}/page_{page}.json"
            s3.put_object(
                Bucket=BUCKET,
                Key=prefix,
                Body=json.dumps({
                    "collection": enriched,
                    "pagination": payload.get("pagination")
                }, indent=2)
            )
            print(f"✓ Wrote {len(enriched)} events to {prefix}")

        next_token = payload.get("pagination", {}).get("next_page_token")
        print(f"    -> next_page_token = {next_token}")
        if next_token:
            url = f"{SCHEDULED_EVENTS_ENDPOINT}?organization={ORG_URL}&count=100&page_token={next_token}"
            page += 1
        else:
            url = None

    if new_events_found and max_event_dt:
        update_tracker(max_event_dt.isoformat())
        print(f"*** Tracker updated to {max_event_dt.isoformat()}")
    else:
        print("*** No new events. Tracker not updated.")

if __name__ == "__main__":
    run_ingestion()
