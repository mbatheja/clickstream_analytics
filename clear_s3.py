import os
import boto3
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-2")

if not aws_access_key or not aws_secret_key:
    print("❌ Error: AWS credentials missing!")
    print("Ensure your .env file in the project root contains:")
    print("  AWS_ACCESS_KEY_ID=your_key")
    print("  AWS_SECRET_ACCESS_KEY=your_secret")
    exit(1)

s3 = boto3.resource(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

bucket = s3.Bucket('clickstream-raw-data-1')
bucket.objects.filter(Prefix='raw_events/').delete()
print("✅ S3 raw_events folder successfully cleared!")
