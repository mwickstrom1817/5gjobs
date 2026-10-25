"""Emergency restore of the active Neon schema from an R2 backup.

Usage:
    .venv/Scripts/python restore_from_r2.py
    .venv/Scripts/python restore_from_r2.py 2026-10-03

Requires environment variables (or .env) for R2 and the database:
    DATABASE_URL, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME

The script prints the newest (or requested) backup, shows a summary of its
contents, and asks for confirmation before overwriting the DB.
"""
import os
import sys
import json
import argparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from core import validate_state_dict
from crypto import decrypt_state_systems
from persistence_pg import init_db, save_state_to_db

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BACKUP_PREFIX = "backups/app_state_"


def get_r2_client():
    endpoint = os.environ.get("R2_ENDPOINT_URL", "").strip().rstrip("/")
    key = os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not all([endpoint, key, secret]):
        raise ValueError("R2_ENDPOINT_URL, R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY are required")
    region = "auto" if "r2.cloudflarestorage.com" in endpoint else "us-east-1"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name=region,
    )


def get_bucket():
    return os.environ.get("R2_BUCKET_NAME") or os.environ.get("AWS_BUCKET_NAME") or os.environ.get("S3_BUCKET")


def list_backups(s3, bucket):
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": BACKUP_PREFIX}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break
        kwargs["ContinuationToken"] = token
    return sorted(keys)


def summary(data):
    return {
        "jobs": len(data.get("jobs", [])),
        "techs": len(data.get("techs", [])),
        "locations": len(data.get("locations", [])),
        "adminEmails": data.get("adminEmails", []),
        "agreements": len(data.get("agreements", [])),
    }


def restore_to_db(data):
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DB_URL")
    if not dsn:
        raise ValueError("DATABASE_URL or NEON_DB_URL is required")
    init_db()
    return save_state_to_db(data)


def main():
    parser = argparse.ArgumentParser(description="Restore active app state from R2 backup to Neon DB")
    parser.add_argument("date", nargs="?", help="Backup date to restore (YYYY-MM-DD). Defaults to newest.")
    args = parser.parse_args()

    s3 = get_r2_client()
    bucket = get_bucket()
    if not bucket:
        raise ValueError("R2_BUCKET_NAME (or AWS_BUCKET_NAME/S3_BUCKET) is required")

    backups = list_backups(s3, bucket)
    if not backups:
        print("No backups found in R2.")
        sys.exit(1)

    if args.date:
        target = f"{BACKUP_PREFIX}{args.date}.json"
        if target not in backups:
            print(f"Backup for {args.date} not found. Available backups:")
            for b in backups:
                print("  ", b)
            sys.exit(1)
    else:
        target = backups[-1]

    print(f"Selected backup: {target}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=target)
        data = json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        print(f"Failed to download backup: {e}")
        sys.exit(1)

    valid, message = validate_state_dict(data)
    if not valid:
        print(f"Backup validation failed: {message}")
        sys.exit(1)

    data = decrypt_state_systems(data)
    print("Backup summary:", json.dumps(summary(data), indent=2))

    ans = input("Overwrite the active Neon DB state with this backup? [yes/no]: ")
    if ans.strip().lower() != "yes":
        print("Aborted. DB was not changed.")
        sys.exit(0)

    version = restore_to_db(data)
    print(f"Restored. New DB version: {version}")


if __name__ == "__main__":
    main()
