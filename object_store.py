import os
import uuid
import datetime
from typing import Optional

import streamlit as st
import boto3
from botocore.config import Config


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=st.secrets["S3_ENDPOINT_URL"],
        aws_access_key_id=st.secrets["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["S3_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def upload_streamlit_file(uploaded_file, folder: str) -> Optional[str]:
    """
    Uploads a Streamlit UploadedFile (st.file_uploader or st.camera_input).
    Returns the object key (NOT a URL).
    """
    if uploaded_file is None:
        return None

    bucket = st.secrets["S3_BUCKET"]

    name = getattr(uploaded_file, "name", "") or ""
    _, ext = os.path.splitext(name)
    if not ext:
        ext = ".jpg"

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{folder}/{ts}_{uuid.uuid4().hex}{ext}"

    content_type = getattr(uploaded_file, "type", None) or "application/octet-stream"

    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=uploaded_file.getbuffer(),
        ContentType=content_type,
    )
    return key


def upload_bytes(data: bytes, key: str, content_type: str) -> str:
    bucket = st.secrets["S3_BUCKET"]
    s3 = _s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return key


def get_view_url(key: str, expires_seconds: int = 3600) -> str:
    """
    Returns either a public URL (if configured) or a signed URL for temporary viewing.
    """
    public_base = st.secrets.get("S3_PUBLIC_BASE_URL", "").strip()
    if public_base:
        return f"{public_base.rstrip('/')}/{key}"

    bucket = st.secrets["S3_BUCKET"]
    s3 = _s3_client()
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )
