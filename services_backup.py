"""Automated daily backups of the app state to the object store (R2/S3).

The whole database is a single JSONB row; this uploads a JSON snapshot once a
day so a database failure is recoverable from Admin -> Data & Backup ->
Restore Backup. The file format intentionally matches what the restore
feature expects: the raw state dict as JSON, one file per day.
"""
import json

from core import now_local, get_logger
from persistence_pg import load_state
from object_store import get_r2_client, get_bucket_name

BACKUP_PREFIX = "backups/app_state_"
BACKUP_RETENTION = 30  # keep the newest N daily snapshots


def backup_key(d=None):
    """Daily key - one snapshot per day; re-running the same day overwrites
    the same file, which makes every retry idempotent."""
    d = d or now_local().date()
    return f"{BACKUP_PREFIX}{d.isoformat()}.json"


def expired_keys(keys, keep=BACKUP_RETENTION):
    """Which backup keys to prune: everything but the newest `keep`.
    Keys are date-suffixed, so lexicographic order is chronological."""
    dated = sorted(k for k in keys if k.startswith(BACKUP_PREFIX))
    return dated[:-keep] if len(dated) > keep else []


def run_daily_backup():
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
        data = json.dumps(state).encode("utf-8")
        key = backup_key()
        s3.put_object(Bucket=bucket, Key=key, Body=data,
                      ContentType="application/json")

        resp = s3.list_objects_v2(Bucket=bucket, Prefix=BACKUP_PREFIX)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        for old in expired_keys(keys):
            s3.delete_object(Bucket=bucket, Key=old)

        logger.log(f"Daily backup written to {key} ({len(data) / 1024:.1f} KB)")
        return True, f"Backup saved to {key}"
    except Exception as e:
        logger.log(f"Daily backup failed: {e}")
        return False, str(e)
