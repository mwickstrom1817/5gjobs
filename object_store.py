import os
import streamlit as st
import mimetypes

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

def get_r2_client():
    if not HAS_BOTO3:
        st.error("❌ `boto3` library not found. Please add it to `requirements.txt`.")
        return None
        
    # Try R2 config first
    endpoint_url = os.environ.get("R2_ENDPOINT_URL") or st.secrets.get("R2_ENDPOINT_URL")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID") or st.secrets.get("R2_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY") or st.secrets.get("R2_SECRET_ACCESS_KEY")
    
    # Fallback to standard AWS config if R2 not set
    if not access_key_id:
        access_key_id = os.environ.get("AWS_ACCESS_KEY_ID") or st.secrets.get("AWS_ACCESS_KEY_ID")
        secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or st.secrets.get("AWS_SECRET_ACCESS_KEY")
        # Endpoint might be None for standard AWS
        if not endpoint_url:
            endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or st.secrets.get("AWS_ENDPOINT_URL")

    # Fallback to S3_ prefix (User specific config)
    if not access_key_id:
        access_key_id = os.environ.get("S3_ACCESS_KEY_ID") or st.secrets.get("S3_ACCESS_KEY_ID")
        secret_access_key = os.environ.get("S3_SECRET_ACCESS_KEY") or st.secrets.get("S3_SECRET_ACCESS_KEY")
        if not endpoint_url:
            endpoint_url = os.environ.get("S3_ENDPOINT_URL") or st.secrets.get("S3_ENDPOINT_URL")

    if not (access_key_id and secret_access_key):
        return None
    
    # R2 requires s3v4 signature and often region='auto'
    config = Config(signature_version='s3v4')
    region_name = None
    
    if endpoint_url:
        endpoint_url = endpoint_url.strip().rstrip('/')
        if "r2.cloudflarestorage.com" in endpoint_url:
            region_name = "auto"
    
    # If region is still None, and no env var is set, default to us-east-1 to prevent "NoRegionError"
    if region_name is None and not os.environ.get("AWS_DEFAULT_REGION") and not os.environ.get("AWS_REGION"):
        region_name = "us-east-1"
        
    return boto3.client(
        's3',
        endpoint_url=endpoint_url, # Can be None for standard AWS
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=config,
        region_name=region_name
    )

def get_bucket_name():
    return os.environ.get("R2_BUCKET_NAME") or st.secrets.get("R2_BUCKET_NAME") or \
           os.environ.get("AWS_BUCKET_NAME") or st.secrets.get("AWS_BUCKET_NAME") or \
           os.environ.get("S3_BUCKET") or st.secrets.get("S3_BUCKET")

def upload_bytes(data, key, content_type):
    """Uploads bytes to R2/S3."""
    s3 = get_r2_client()
    bucket = get_bucket_name()
    
    if not s3:
        st.error("⚠️ Storage Client not available. Check credentials.")
        return None
    if not bucket:
        st.error("⚠️ Bucket name not configured.")
        return None
        
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type
        )
        return key
    except ClientError as e:
        st.error(f"❌ Upload Failed: {e}")
        return None

def upload_streamlit_file(uploaded_file, folder="photos"):
    """Uploads a Streamlit UploadedFile to R2/S3."""
    if uploaded_file is None:
        return None
        
    s3 = get_r2_client()
    bucket = get_bucket_name()
    
    if not s3:
        st.error("⚠️ Storage Client not available. Check credentials.")
        return None
    if not bucket:
        st.error("⚠️ Bucket name not configured.")
        return None
        
    # Generate key
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = uploaded_file.name
    key = f"{folder}/{timestamp}_{filename}"
    
    try:
        s3.upload_fileobj(
            uploaded_file,
            bucket,
            key,
            ExtraArgs={'ContentType': uploaded_file.type}
        )
        return key
    except ClientError as e:
        st.error(f"❌ Upload Failed: {e}")
        return None

def get_view_url(key, expires_seconds=3600):
    """Generates a presigned URL for viewing the object."""
    if not key:
        return None
        
    s3 = get_r2_client()
    bucket = get_bucket_name()
    
    if not s3 or not bucket:
        return None
        
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expires_seconds
        )
        return url
    except ClientError as e:
        return None
