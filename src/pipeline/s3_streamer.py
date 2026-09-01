from src.utils.sample_size import get_required_sample_size
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import argparse
import asyncio
import json
import os
import random
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from src.generator.event_generator import load_config, simulate_user_session

load_dotenv()

UPLOAD_WORKERS = 32
SIGNUP_WINDOW_DAYS = 14

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

async def _sleep_scaled(seconds: float, time_scale: float):
    """Sleep for `seconds` of simulated time, compressed by time_scale
    (e.g. time_scale=3600 means 1 simulated hour of waiting becomes 1 real second)."""
    if seconds > 0:
        await asyncio.sleep(seconds / time_scale)

async def _process_user(idx, config, s3_client, bucket_name, time_scale, upload_pool, stats, progress):
    loop = asyncio.get_running_loop()
    user_id = f"usr_{2000 + idx}"

    # Establish immutable attributes per user
    variant = random.choice(["control", "new_checkout_ui"])
    device = random.choice(["mobile", "desktop", "tablet"])
    channel = random.choice(["Organic", "Google_Ads", "Email", "Direct"])

    # User signs up "now" (i.e. whenever their staggered arrival slot comes up)
    signup_dt = datetime.now() + timedelta(hours=random.randint(0, 12))
    signup_date = signup_dt.strftime("%Y-%m-%d")

    # Determine repeat visit frequency (1 to 5 sessions per user)
    num_user_sessions = random.choices([1, 2, 3, 4, 5], weights=[0.45, 0.25, 0.15, 0.10, 0.05])[0]
    current_session_time = signup_dt

    for _ in range(num_user_sessions):
        # Wait until this session's (scaled) real-time moment actually arrives
        wait_seconds = (current_session_time - datetime.now()).total_seconds()
        await _sleep_scaled(max(0, wait_seconds), time_scale)

        try:
            session_events, session_end_time = simulate_user_session(
                user_id=user_id,
                config=config,
                variant=variant,
                device=device,
                channel=channel,
                signup_date=signup_date,
                session_start_time=current_session_time
            )
        except Exception as e:
            print(f"ERROR: Failed to simulate session for {user_id}: {e}", file=sys.stderr)
            break

        # Stream events out one at a time, paced to the gaps between their own timestamps
        prev_time = current_session_time
        for event in session_events:
            event_time = datetime.fromisoformat(event["timestamp"])
            await _sleep_scaled((event_time - prev_time).total_seconds(), time_scale)
            prev_time = event_time

            success = await loop.run_in_executor(upload_pool, upload_event_to_s3, s3_client, bucket_name, event)
            if success:
                stats["uploaded"] += 1
            else:
                stats["failed"] += 1

        # Advance time by 1 to 7 days for subsequent return sessions
        days_until_next = random.randint(1, 7)
        hours_offset = random.randint(1, 8)
        current_session_time = session_end_time + timedelta(days=days_until_next, hours=hours_offset)

    progress["completed"] += 1
    completed, total = progress["completed"], progress["total"]
    if completed % progress["step"] == 0 or completed == total:
        pct = completed / total * 100
        print(f"  ...{completed:,}/{total:,} users ({pct:.0f}%), "
              f"{stats['uploaded']:,} events streamed so far")

async def _run_stream(config, bucket_name, aws_region, num_users, time_scale):
    try:
        s3_client = boto3.client(
            "s3", region_name=aws_region, config=Config(max_pool_connections=UPLOAD_WORKERS)
        )
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: Could not initialize S3 client: {e}", file=sys.stderr)
        raise

    signup_window_seconds = SIGNUP_WINDOW_DAYS * 24 * 3600
    arrival_interval = signup_window_seconds / num_users

    stats = {"uploaded": 0, "failed": 0}
    progress = {"completed": 0, "total": num_users, "step": max(1, num_users // 100)}

    print(f"Starting LIVE stream for {num_users:,} users arriving over a "
          f"{SIGNUP_WINDOW_DAYS}-day signup window (time compressed {time_scale}x) "
          f"to S3 bucket: '{bucket_name}'...")

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_pool:
        tasks = []
        for idx in range(num_users):
            tasks.append(asyncio.create_task(
                _process_user(idx, config, s3_client, bucket_name, time_scale, upload_pool, stats, progress)
            ))
            # Stagger arrivals across the signup window instead of launching everyone at once
            await _sleep_scaled(random.expovariate(1 / arrival_interval), time_scale)

        await asyncio.gather(*tasks)

    print(f"Successfully streamed {stats['uploaded']:,} events to s3://{bucket_name}/raw_events/")
    if stats["failed"]:
        print(f"WARNING: {stats['failed']:,} events failed to upload. See errors above.", file=sys.stderr)

    return stats["uploaded"], stats["failed"]

def stream_to_s3(bucket_name: str, aws_region: str = "us-east-2", num_users: int = None, time_scale: float = 3600.0):
    """
    Runs a live producer that streams a continuously-arriving population of users
    to S3 in (scaled) real time, instead of pre-computing and dumping an entire
    population's multi-week history at once. New users arrive gradually across a
    signup window, and each user's events are only uploaded once their own
    (scaled) real-time moment arrives -- so S3/Snowpipe see a genuine trickle of
    files over the run, not one instantaneous batch.

    time_scale compresses simulated time into real time (e.g. 3600 means 1
    simulated hour of waiting becomes 1 real second). Use 1.0 for true real-time
    streaming. If num_users is not given, it's derived from config.yaml's
    checkout_conversion control/variant rates via the same power analysis
    event_generator.py uses.
    """
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"ERROR: Could not load configuration: {e}", file=sys.stderr)
        raise

    if num_users is None:
        exp_config = config["experiments"]["checkout_conversion"]
        p_control, p_variant = exp_config["control"], exp_config["variant"]
        relative_mde = (p_variant - p_control) / p_control
        num_users = get_required_sample_size(p1=p_control, relative_mde=relative_mde)

    return asyncio.run(_run_stream(config, bucket_name, aws_region, num_users, time_scale))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--time-scale", type=float, default=3600.0,
        help="Compression factor for simulated time (e.g. 3600 = 1 simulated hour per real second). Use 1.0 for true real-time."
    )
    parser.add_argument(
        "--num-users", type=int, default=None,
        help="Override the statistically-derived user count"
    )
    args = parser.parse_args()

    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "YOUR_S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

    try:
        _, failed = stream_to_s3(bucket_name=S3_BUCKET_NAME,
                                  aws_region=AWS_REGION,
                                  num_users=args.num_users,
                                  time_scale=args.time_scale
                                 )
    except KeyboardInterrupt:
        print("\nStream interrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: s3_streamer aborted: {e}", file=sys.stderr)
        sys.exit(1)

    if failed:
        sys.exit(1)
