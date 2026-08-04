from datetime import datetime
import json
import os
import boto3
from dotenv import load_dotenv
from src.generator.event_generator import load_config, simulate_user_session

load_dotenv()

def upload_event_to_s3(s3_client, bucket_name: str, event: dict):
    """
    Uploads a single JSON telemetry record to S3 with key partitioning
    """
    event_dt = datetime.fromisoformat(event["timestamp"])
    s3_key = (
        f"raw_events/"
        f"year={event_dt.strftime('%Y')}/"
        f"month={event_dt.strftime('%m')}/"
        f"day={event_dt.strftime('%d')}/"
        f"event_{event['event_id']}.json"
    )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(event, indent=2),
        ContentType="application/json"
    )

def stream_to_s3(bucket_name: str, aws_region: str = "us-east-2", num_sessions: int = 50000):
    """
    Generates sessions and streams events directly to AWS S3.
    """
    config = load_config()

    # Initialize AWS S3 Client using local AWS CLI configuration
    s3_client = boto3.client("s3", region_name = aws_region)

    print(f"Starting stream for {num_sessions} user sessions to S3 bucket: '{bucket_name}'...")
    total_events = 0

    for idx in range(num_sessions):
        user_id = f"usr_{2000 + idx}"
        session_events = simulate_user_session(user_id, config)

        for event in session_events:
            upload_event_to_s3(s3_client, bucket_name, event)
            total_events += 1

    print(f"Successfully streamed {total_events} events to s3://{bucket_name}/raw_events/")

if __name__ == "__main__":
    S3_BUCKET = os.getenv("S3_BUCKET_NAME", "YOUR_S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

    stream_to_s3(bucket_name=S3_BUCKET, 
                 aws_region=AWS_REGION, 
                 num_sessions=50000
                )