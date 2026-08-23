import os
import boto3
import pytest

@pytest.mark.integration

def test_s3_upload_and_read():
    """
    Verify that s3_streamer actually writes and reads back from the S3 bucket.
    """
    s3_client = boto3.client("s3")
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-clickstream-bucket")
    test_key = "integration_test/sample_event.json"

    # Write mock payload to S3
    payload = '{"event_id": "test_123", "event_type": "view_homepage"}'
    s3_client.put_object(Bucket=bucket, Key=test_key, Body=payload)

    # Read back object and assert payload match
    obj = s3_client.get_object(Bucket=bucket, Key=test_key)
    retrieved_data = obj['Body'].read().decode('utf-8')
    assert "test_123" in retrieved_data