"""Emergency restore of the Neon app_state row from the newest R2 backup.

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
from datetime import date

import psycopg2
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

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
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=BACKUP_PREFIX)
    keys = [o["Key"] for o in resp.get("Contents", [])]
    # Keys look like backups/app_state_2026-10-03.json
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
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES ('global_state', %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, version = app_state.version + 1, updated_at = CURRENT_TIMESTAMP
                RETURNING version;
                """,
                (json.dumps(data),),
            )
            new_version = cur.fetchone()[0]
        conn.commit()
        return new_version
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Restore app_state from R2 backup to Neon DB")
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

    required = ["jobs", "techs", "locations"]
    if not all(k in data for k in required):
        print("Backup file is missing required keys:", required)
        sys.exit(1)

    print("Backup summary:", json.dumps(summary(data), indent=2))

    ans = input("Overwrite the Neon DB global_state row with this backup? [yes/no]: ")
    if ans.strip().lower() != "yes":
        print("Aborted. DB was not changed.")
        sys.exit(0)

    version = restore_to_db(data)
    print(f"Restored. New DB version: {version}")


if __name__ == "__main__":
    main()
