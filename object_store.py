"""
object_store.py  —  S3 / Cloudflare R2 helper
Streamlit-free: works standalone with FastAPI or any Python process.
Credentials are read exclusively from environment variables.
"""

import os
import logging
import datetime
import threading
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    logger.warning("boto3 not installed — object storage unavailable.")

# ── Singleton client (replaces @st.cache_resource) ────────────────────────────

_client = None
_client_lock = threading.Lock()


def _build_client():
    """Create and return a boto3 S3 client from environment variables."""
    if not HAS_BOTO3:
        raise RuntimeError("`boto3` is not installed. Add it to requirements.txt.")

    # Support R2_, AWS_, or S3_ prefixed env vars (checked in priority order)
    def _env(*keys):
        for k in keys:
            v = os.environ.get(k)
            if v:
                return v
        return None

    endpoint_url   = _env("R2_ENDPOINT_URL",    "AWS_ENDPOINT_URL",    "S3_ENDPOINT_URL")
    access_key_id  = _env("R2_ACCESS_KEY_ID",   "AWS_ACCESS_KEY_ID",   "S3_ACCESS_KEY_ID")
    secret_key     = _env("R2_SECRET_ACCESS_KEY","AWS_SECRET_ACCESS_KEY","S3_SECRET_ACCESS_KEY")

    if not access_key_id or not secret_key:
        raise RuntimeError(
            "Object storage credentials not found. "
            "Set R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY (or AWS_ / S3_ equivalents)."
        )

    region_name = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")

    if endpoint_url:
        endpoint_url = endpoint_url.strip().rstrip("/")
        if "r2.cloudflarestorage.com" in endpoint_url:
            region_name = "auto"

    if not region_name:
        region_name = "us-east-1"

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=region_name,
    )


def get_r2_client():
    """Return the shared S3/R2 client, creating it once on first call."""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:          # double-checked locking
                _client = _build_client()
    return _client


def get_bucket_name() -> Optional[str]:
    """Return the configured bucket name from environment variables."""
    return (
        os.environ.get("R2_BUCKET_NAME")
        or os.environ.get("AWS_BUCKET_NAME")
        or os.environ.get("S3_BUCKET")
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def upload_bytes(data: bytes, key: str, content_type: str) -> Optional[str]:
    """
    Upload raw bytes to the object store.

    Returns the object key on success, or None on failure.
    Raises RuntimeError if credentials / bucket are not configured.
    """
    s3     = get_r2_client()
    bucket = get_bucket_name()

    if not bucket:
        raise RuntimeError("Bucket name not configured. Set R2_BUCKET_NAME.")

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        logger.info("Uploaded %s (%d bytes)", key, len(data))
        return key
    except ClientError as e:
        logger.error("Upload failed for key=%s: %s", key, e)
        return None


def upload_file_object(file_obj, filename: str, content_type: str,
                       folder: str = "photos") -> Optional[str]:
    """
    Upload a file-like object (e.g. from FastAPI's UploadFile.file).

    Returns the object key on success, or None on failure.
    """
    s3     = get_r2_client()
    bucket = get_bucket_name()

    if not bucket:
        raise RuntimeError("Bucket name not configured. Set R2_BUCKET_NAME.")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{folder}/{timestamp}_{filename}"

    try:
        s3.upload_fileobj(file_obj, bucket, key,
                          ExtraArgs={"ContentType": content_type})
        logger.info("Uploaded %s", key)
        return key
    except ClientError as e:
        logger.error("Upload failed for key=%s: %s", key, e)
        return None


# Simple in-process URL cache (replaces @st.cache_data(ttl=1800))
_url_cache: dict[str, tuple[str, datetime.datetime]] = {}
_url_cache_lock = threading.Lock()
_URL_TTL_SECONDS = 1800


def get_view_url(key: str, expires_seconds: int = 3600) -> Optional[str]:
    """
    Generate a presigned GET URL for the given object key.

    Results are cached in-process for 30 minutes to avoid excessive API calls.
    Returns None if the key is empty or an error occurs.
    """
    if not key:
        return None

    # Check cache first
    with _url_cache_lock:
        cached = _url_cache.get(key)
        if cached:
            url, ts = cached
            age = (datetime.datetime.now() - ts).total_seconds()
            if age < _URL_TTL_SECONDS:
                return url
            del _url_cache[key]

    try:
        s3     = get_r2_client()
        bucket = get_bucket_name()
        if not s3 or not bucket:
            return None

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_seconds,
        )

        with _url_cache_lock:
            _url_cache[key] = (url, datetime.datetime.now())

        return url
    except ClientError as e:
        logger.error("Failed to generate presigned URL for key=%s: %s", key, e)
        return None
