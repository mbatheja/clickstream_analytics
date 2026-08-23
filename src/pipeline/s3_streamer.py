from src.utils.sample_size import calculate_sample_size
n_per_group, _ = calculate_sample_size()
TOTAL_SESSIONS = n_per_group * 2
from datetime import datetime
import json
import os
import sys
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from src.generator.event_generator import load_config, simulate_user_session

load_dotenv()

def upload_event_to_s3(s3_client, bucket_name: str, event: dict):
    """
    Uploads a single JSON telemetry record to S3 with key partitioning
    """
    try:
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
        return True
    except (KeyError, ValueError) as e:
        print(f"ERROR: Malformed event {event.get('event_id', '<unknown>')}: {e}", file=sys.stderr)
        return False
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: Failed to upload event {event.get('event_id', '<unknown>')} to S3: {e}", file=sys.stderr)
        return False

def stream_to_s3(bucket_name: str, aws_region: str = "us-east-2", num_sessions: int = TOTAL_SESSIONS):
    """
    Generates sessions and streams events directly to AWS S3.
    """
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"ERROR: Could not load configuration: {e}", file=sys.stderr)
        raise

    # Initialize AWS S3 Client using local AWS CLI configuration
    try:
        s3_client = boto3.client("s3", region_name=aws_region)
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: Could not initialize S3 client: {e}", file=sys.stderr)
        raise

    print(f"Starting stream for {num_sessions} user sessions to S3 bucket: '{bucket_name}'...")
    total_events = 0
    failed_events = 0

    for idx in range(num_sessions):
        user_id = f"usr_{2000 + idx}"
        try:
            session_events, _ = simulate_user_session(user_id, config)
        except Exception as e:
            print(f"ERROR: Failed to simulate session for {user_id}: {e}", file=sys.stderr)
            continue

        for event in session_events:
            if upload_event_to_s3(s3_client, bucket_name, event):
                total_events += 1
            else:
                failed_events += 1

    print(f"Successfully streamed {total_events} events to s3://{bucket_name}/raw_events/")
    if failed_events:
        print(f"WARNING: {failed_events} events failed to upload. See errors above.", file=sys.stderr)

    return total_events, failed_events

if __name__ == "__main__":
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "YOUR_S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

    try:
        _, failed = stream_to_s3(bucket_name=S3_BUCKET_NAME,
                                  aws_region=AWS_REGION,
                                  num_sessions=TOTAL_SESSIONS
                                 )
    except Exception as e:
        print(f"FATAL: s3_streamer aborted: {e}", file=sys.stderr)
        sys.exit(1)

    if failed:
        sys.exit(1)
