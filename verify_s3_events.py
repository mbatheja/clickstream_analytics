import json
import math
import os
import random
import time
from pathlib import Path

import boto3
import pandas as pd
import yaml
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from src.utils.sample_size import get_required_sample_size

# Sized so two disjoint samples take ~10-15 min total at ~90ms/object.
RECAPTURE_SAMPLE_SIZE = 4000

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

BUCKET_NAME = "clickstream-raw-data-1"

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-1"),
)

# 0. Statistically required sample size (same power analysis as event_generator.py)
with open(Path(__file__).resolve().parent / "config" / "config.yaml") as f:
    config = yaml.safe_load(f)

exp_config = config["experiments"]["checkout_conversion"]
p_control = exp_config["control"]
p_variant = exp_config["variant"]
relative_mde = (p_variant - p_control) / p_control
required_n = get_required_sample_size(p1=p_control, relative_mde=relative_mde)
print(f"Statistically required N (power analysis): {required_n:,} users")

# 1. Count all files using pagination
paginator = s3.get_paginator("list_objects_v2")
file_keys = []

for page in paginator.paginate(Bucket=BUCKET_NAME):
    contents = page.get("Contents", [])
    # Filters out subdirectories or non-json files
    file_keys.extend([obj["Key"] for obj in contents if obj["Key"].endswith(".json")])

print(f"Total S3 JSON files found: {len(file_keys)}")
print(f"Have enough raw event files vs. required N: {len(file_keys) >= required_n}")


def fetch_records(key, retries=3):
    """Fetch one object and return its list of event records, retrying transient network errors."""
    for attempt in range(retries):
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            payload = json.loads(response["Body"].read().decode("utf-8"))
            return payload if isinstance(payload, list) else [payload]
        except (ClientError, BotoCoreError):
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def user_ids_in_sample(keys):
    ids = set()
    records = []
    for key in keys:
        for record in fetch_records(key):
            records.append(record)
            if "user_id" in record:
                ids.add(record["user_id"])
    return ids, records


# 2. Estimate unique users covered so far via mark-recapture (Chapman estimator),
# instead of downloading all files just to dedupe user_id. Each user contributes
# multiple event files, so a naive "sample fraction * unique-in-sample" scale-up
# is biased; two disjoint samples plus their overlap correct for that.
sample_size = min(RECAPTURE_SAMPLE_SIZE, len(file_keys) // 2)
shuffled = file_keys[:]
random.shuffle(shuffled)
sample1_keys = shuffled[:sample_size]
sample2_keys = shuffled[sample_size:sample_size * 2]

print(f"Fetching two disjoint samples of {sample_size:,} files each for a mark-recapture estimate...")
sample1_ids, sample1_records = user_ids_in_sample(sample1_keys)
sample2_ids, _ = user_ids_in_sample(sample2_keys)

n1, n2 = len(sample1_ids), len(sample2_ids)
m = len(sample1_ids & sample2_ids)

estimated_users = (n1 + 1) * (n2 + 1) / (m + 1) - 1
variance = ((n1 + 1) * (n2 + 1) * (n1 - m) * (n2 - m)) / ((m + 1) ** 2 * (m + 2)) if m > 0 else float("nan")
se = math.sqrt(variance) if variance == variance else float("nan")
ci_low, ci_high = estimated_users - 1.96 * se, estimated_users + 1.96 * se

print(f"Unique users in sample 1: {n1:,} | sample 2: {n2:,} | overlap: {m:,}")
print(f"Estimated unique users currently in S3: {estimated_users:,.0f} (95% CI: {ci_low:,.0f} - {ci_high:,.0f})")
print(f"Estimate vs. statistically required N ({required_n:,}): "
      f"{'ENOUGH' if estimated_users >= required_n else 'NOT ENOUGH'}")

# 3. Preview a sample DataFrame for a quick schema/spot check
df = pd.DataFrame(sample1_records)
print(f"\nSample DataFrame preview ({len(df)} events from sample 1):")
print(df.head())