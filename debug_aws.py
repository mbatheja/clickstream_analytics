import os
from pathlib import Path
import boto3
import yaml
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError

# Load .env file
project_root = Path(__file__).resolve().parent
load_dotenv(dotenv_path=project_root / ".env")

key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

print("🔍 Environment Check:")
print(f"  • AWS_ACCESS_KEY_ID:     {key_id[:4]}...{key_id[-4:] if key_id else 'NOT FOUND'}")
print(f"  • AWS_SECRET_ACCESS_KEY: {'[SET]' if secret else 'NOT FOUND'}")
print(f"  • AWS_DEFAULT_REGION:    {region}")

# Load Config Bucket
config_path = project_root / "config" / "config.yaml"
bucket_name = "my-clickstream-bucket"
if config_path.exists():
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        bucket_name = config.get("aws", {}).get("s3_bucket", bucket_name)

print(f"  • Configured Bucket:     {bucket_name}\n")

# Test S3 Connection
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name=region
    )
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"✅ Success! Connected to bucket '{bucket_name}'.")

except NoCredentialsError:
    print("❌ Error: Boto3 cannot find AWS credentials. Check variable names in .env.")
except ClientError as e:
    code = e.response["Error"]["Code"]
    msg = e.response["Error"]["Message"]
    print(f"❌ AWS S3 Error [{code}]: {msg}")
    if code == "404":
        print(f"👉 Fix: Bucket '{bucket_name}' does not exist in AWS S3.")
    elif code == "403":
        print(f"👉 Fix: Access Denied. Check IAM user permissions for '{bucket_name}'.")