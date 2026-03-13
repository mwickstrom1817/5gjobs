"""
object_store.py — Streamlit-free version for FastAPI backend.
All st.secrets / st.cache calls replaced with os.environ.
"""
import os
import datetime

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

_r2_client = None

def get_r2_client():
    global _r2_client
    if _r2_client:
        return _r2_client
    if not HAS_BOTO3:
        print("❌ boto3 not installed.")
        return None
    endpoint_url  = os.environ.get("R2_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("S3_ENDPOINT_URL")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("S3_ACCESS_KEY_ID")
    secret_key    = os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("S3_SECRET_ACCESS_KEY")
    if not (access_key_id and secret_key):
        return None
    config = Config(signature_version='s3v4')
    region_name = None
    if endpoint_url:
        endpoint_url = endpoint_url.strip().rstrip('/')
        if "r2.cloudflarestorage.com" in endpoint_url:
            region_name = "auto"
    if region_name is None:
        region_name = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    _r2_client = boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key_id,
                               aws_secret_access_key=secret_key, config=config, region_name=region_name)
    return _r2_client

def get_bucket_name():
    return os.environ.get("R2_BUCKET_NAME") or os.environ.get("AWS_BUCKET_NAME") or os.environ.get("S3_BUCKET")

def upload_bytes(data: bytes, key: str, content_type: str):
    s3 = get_r2_client()
    bucket = get_bucket_name()
    if not s3 or not bucket:
        return None
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        return key
    except ClientError as e:
        print(f"Upload failed: {e}")
        return None

def get_view_url(key: str, expires_seconds: int = 3600):
    if not key:
        return None
    s3 = get_r2_client()
    bucket = get_bucket_name()
    if not s3 or not bucket:
        return None
    try:
        return s3.generate_presigned_url('get_object',
                                          Params={'Bucket': bucket, 'Key': key},
                                          ExpiresIn=expires_seconds)
    except ClientError:
        return None
