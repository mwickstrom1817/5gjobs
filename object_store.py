import os
import streamlit as st
import mimetypes

try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

def get_r2_client():
    if not HAS_BOTO3:
        return None
        
    endpoint_url = os.environ.get("R2_ENDPOINT_URL") or st.secrets.get("R2_ENDPOINT_URL")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID") or st.secrets.get("R2_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY") or st.secrets.get("R2_SECRET_ACCESS_KEY")
    
    if not (endpoint_url and access_key_id and secret_access_key):
        return None
        
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )

def get_bucket_name():
    return os.environ.get("R2_BUCKET_NAME") or st.secrets.get("R2_BUCKET_NAME")

def upload_bytes(data, key, content_type):
    """Uploads bytes to R2."""
    s3 = get_r2_client()
    bucket = get_bucket_name()
    
    if not s3 or not bucket:
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
        return None

def upload_streamlit_file(uploaded_file, folder="photos"):
    """Uploads a Streamlit UploadedFile to R2."""
    if uploaded_file is None:
        return None
        
    s3 = get_r2_client()
    bucket = get_bucket_name()
    
    if not s3 or not bucket:
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
