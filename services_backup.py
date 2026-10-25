"""Automated backups of the app state to the object store (R2/S3).

This uploads a JSON snapshot of the active database state so a database failure
is recoverable from Admin -> Data & Backup -> Restore Backup. The file format
intentionally matches what the restore feature expects: the raw state dict as
JSON, with encrypted location systems preserved.
"""
import json

from core import now_local, get_logger
from crypto import encrypt_state_systems
from persistence_pg import load_state
from object_store import get_r2_client, get_bucket_name

BACKUP_PREFIX = "backups/app_state_"
BACKUP_RETENTION = 30  # keep the newest N snapshots


def backup_key(d=None, manual=False):
    """Return an idempotent daily key or a unique timestamped manual key."""
    now = now_local()
    if manual:
        timestamp = d or now
        return f"{BACKUP_PREFIX}{timestamp.strftime('%Y-%m-%dT%H-%M-%S-%f')}.json"
    d = d or now.date()
    return f"{BACKUP_PREFIX}{d.isoformat()}.json"


def backup_exists(d=None):
    """Return whether today's nightly recovery point already exists."""
    try:
        s3 = get_r2_client()
        bucket = get_bucket_name()
        if not (s3 and bucket):
            return False
        s3.head_object(Bucket=bucket, Key=backup_key(d=d))
        return True
    except Exception:
        return False


def expired_keys(keys, keep=BACKUP_RETENTION):
    """Which backup keys to prune: everything but the newest `keep`.
    Keys are date/timestamp-suffixed, so lexicographic order is chronological."""
    dated = sorted(k for k in keys if k.startswith(BACKUP_PREFIX))
    return dated[:-keep] if len(dated) > keep else []


def list_backup_keys(s3, bucket):
    """List every backup key, following S3/R2 continuation tokens."""
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": BACKUP_PREFIX}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            return keys
        token = resp.get("NextContinuationToken")
        if not token:
            return keys
        kwargs["ContinuationToken"] = token


def run_daily_backup(manual=False):
    """Snapshot the DB state to the object store and prune old snapshots.
    Returns (ok, message). Never raises - a backup problem must never be the
    reason a reminder or a save fails."""
    logger = get_logger()
    try:
        s3 = get_r2_client()
        bucket = get_bucket_name()
        if not (s3 and bucket):
            return False, "Object storage is not configured."

        state, _version = load_state()
        data = json.dumps(encrypt_state_systems(state)).encode("utf-8")
        key = backup_key(manual=manual)
        s3.put_object(Bucket=bucket, Key=key, Body=data,
                      ContentType="application/json")

        for old in expired_keys(list_backup_keys(s3, bucket)):
            s3.delete_object(Bucket=bucket, Key=old)

        kind = "Manual" if manual else "Daily"
        logger.log(f"{kind} backup written to {key} ({len(data) / 1024:.1f} KB)")
        return True, f"Backup saved to {key}"
    except Exception as e:
        logger.log(f"Backup failed: {e}")
        return False, str(e)
