from src.utils.sample_size import get_required_sample_size
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime, timedelta
import argparse
import asyncio
import json
import os
import random
import sys
import uuid
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from src.generator.event_generator import (
    load_config,
    simulate_user_session,
    expected_checkout_reach_prob,
    expected_sessions_per_user,
    SESSION_COUNT_CHOICES,
    SESSION_COUNT_WEIGHTS,
)

load_dotenv()

UPLOAD_WORKERS = 32
SIGNUP_WINDOW_DAYS = 14
FLUSH_INTERVAL_SECONDS = 5  # real (wall-clock) seconds between batch uploads

def upload_batch_to_s3(s3_client, bucket_name: str, date_key: str, events: list):
    """
    Uploads a batch of events sharing the same simulated event date as one
    newline-delimited-JSON object (one JSON object per line), instead of one
    S3 object per event. Snowflake's native JSON parser splits NDJSON into one
    row per object automatically, so this needs no stage/file-format changes,
    and keeps Snowpipe's per-file load overhead bounded at high event volumes.
    """
    try:
        batch_id = uuid.uuid4().hex[:12]
        s3_key = f"raw_events/{date_key}/batch_{batch_id}.json"
        body = "\n".join(json.dumps(event) for event in events)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=body,
            ContentType="application/json"
        )
        return len(events), 0
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: Failed to upload batch of {len(events)} events to S3: {e}", file=sys.stderr)
        return 0, len(events)

def _date_key(event: dict) -> str:
    event_dt = datetime.fromisoformat(event["timestamp"])
    return f"year={event_dt.strftime('%Y')}/month={event_dt.strftime('%m')}/day={event_dt.strftime('%d')}"

async def _sleep_scaled(seconds: float, time_scale: float):
    """Sleep for `seconds` of simulated time, compressed by time_scale
    (e.g. time_scale=3600 means 1 simulated hour of waiting becomes 1 real second)."""
    if seconds > 0:
        await asyncio.sleep(seconds / time_scale)

async def _flush_buffer(buffer, buffer_lock, s3_client, bucket_name, upload_pool, stats):
    loop = asyncio.get_running_loop()
    async with buffer_lock:
        if not buffer:
            return
        batches = dict(buffer)
        buffer.clear()

    for date_key, events in batches.items():
        uploaded, failed = await loop.run_in_executor(
            upload_pool, upload_batch_to_s3, s3_client, bucket_name, date_key, events
        )
        stats["uploaded"] += uploaded
        stats["failed"] += failed

async def _flush_loop(buffer, buffer_lock, s3_client, bucket_name, upload_pool, stats, stop_event):
    while not stop_event.is_set():
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        await _flush_buffer(buffer, buffer_lock, s3_client, bucket_name, upload_pool, stats)
    # Final drain once all producer tasks have finished
    await _flush_buffer(buffer, buffer_lock, s3_client, bucket_name, upload_pool, stats)

async def _process_user(idx, config, buffer, buffer_lock, time_scale, progress):
    user_id = f"usr_{2000 + idx}"

    # Establish immutable attributes per user
    variant = random.choice(["control", "new_checkout_ui"])
    device = random.choice(["mobile", "desktop", "tablet"])
    channel = random.choice(["Organic", "Google_Ads", "Email", "Direct"])

    # User signed up sometime in the SIGNUP_WINDOW_DAYS leading up to now, not
    # exactly "now" -- gives a realistic spread of signup dates (some brand new,
    # some existing users returning) instead of everyone joining on the same day.
    signup_dt = datetime.now() - timedelta(
        days=random.randint(0, SIGNUP_WINDOW_DAYS), hours=random.randint(0, 12)
    )
    signup_date = signup_dt.strftime("%Y-%m-%d")

    # Determine repeat visit frequency (1 to 5 sessions per user)
    num_user_sessions = random.choices(SESSION_COUNT_CHOICES, weights=SESSION_COUNT_WEIGHTS)[0]
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

        # Queue events one at a time, paced to the gaps between their own timestamps
        prev_time = current_session_time
        for event in session_events:
            event_time = datetime.fromisoformat(event["timestamp"])
            await _sleep_scaled((event_time - prev_time).total_seconds(), time_scale)
            prev_time = event_time

            async with buffer_lock:
                buffer[_date_key(event)].append(event)

        # Advance time by 1 to 7 days for subsequent return sessions
        days_until_next = random.randint(1, 7)
        hours_offset = random.randint(1, 8)
        current_session_time = session_end_time + timedelta(days=days_until_next, hours=hours_offset)

    progress["completed"] += 1
    completed, total = progress["completed"], progress["total"]
    if completed % progress["step"] == 0 or completed == total:
        pct = completed / total * 100
        print(f"  ...{completed:,}/{total:,} users ({pct:.0f}%)")

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
    buffer = defaultdict(list)
    buffer_lock = asyncio.Lock()
    stop_event = asyncio.Event()

    print(f"Starting LIVE stream for {num_users:,} users arriving over a "
          f"{SIGNUP_WINDOW_DAYS}-day signup window (time compressed {time_scale}x), "
          f"batching uploads every {FLUSH_INTERVAL_SECONDS}s, to S3 bucket: '{bucket_name}'...")

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_pool:
        flush_task = asyncio.create_task(
            _flush_loop(buffer, buffer_lock, s3_client, bucket_name, upload_pool, stats, stop_event)
        )

        tasks = []
        for idx in range(num_users):
            tasks.append(asyncio.create_task(
                _process_user(idx, config, buffer, buffer_lock, time_scale, progress)
            ))
            # Stagger arrivals across the signup window instead of launching everyone at once
            await _sleep_scaled(random.expovariate(1 / arrival_interval), time_scale)

        await asyncio.gather(*tasks)

        stop_event.set()
        await flush_task  # runs the final drain of anything left in the buffer

    print(f"Successfully streamed {stats['uploaded']:,} events to s3://{bucket_name}/raw_events/")
    if stats["failed"]:
        print(f"WARNING: {stats['failed']:,} events failed to upload. See errors above.", file=sys.stderr)

    return stats["uploaded"], stats["failed"]

def stream_to_s3(bucket_name: str, aws_region: str = "us-east-2", num_users: int = None, time_scale: float = 3600.0):
    """
    Runs a live producer that streams a continuously-arriving population of users
    to S3 in (scaled) real time, batching events into periodic NDJSON files
    instead of writing one file per event. New users arrive gradually across a
    signup window, and each user's events are only queued once their own
    (scaled) real-time moment arrives -- so S3/Snowpipe see a genuine trickle
    of batched files over the run, not one instantaneous per-event dump.

    time_scale compresses simulated time into real time (e.g. 3600 means 1
    simulated hour of waiting becomes 1 real second). Use 1.0 for true real-time
    streaming. If num_users is not given, it's derived from config.yaml's
    checkout_conversion control/variant rates via the same power analysis
    event_generator.py uses (inflated to account for funnel drop-off before
    checkout -- see event_generator.expected_checkout_reach_prob).
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

        # Required checkout-reaching sessions per arm, inflated to a total user
        # count since the configured lift only applies to sessions that reach
        # checkout -- see event_generator.run_generator() for the same logic.
        n_per_arm = get_required_sample_size(p1=p_control, relative_mde=relative_mde) / 2
        checkout_reach_prob = expected_checkout_reach_prob(config)
        sessions_per_user = expected_sessions_per_user()
        num_users = int((n_per_arm * 2) / (sessions_per_user * checkout_reach_prob))
        print(f"Inflated required N: {num_users:,} total users "
              f"(checkout-reach rate {checkout_reach_prob * 100:.2f}%, "
              f"{sessions_per_user:.2f} sessions/user)")

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
