#!/usr/bin/env python3
"""
Upload IMPACT JSON data files to Cloudflare R2 (S3-compatible API).

Syncs docs/data/ to the R2 bucket, skipping files that haven't changed
(checked by ETag/MD5). Run after compute_snapshots.py.

Requirements:
    pip install boto3

Credentials (in .env):
    R2_ACCOUNT_ID
    R2_BUCKET_NAME
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_PUBLIC_URL

Usage:
    python scripts/upload_to_r2.py            # sync changed files only
    python scripts/upload_to_r2.py --dry-run  # show what would change
    python scripts/upload_to_r2.py --force    # re-upload everything
"""

import sys
import os
import hashlib
import argparse
import logging
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 not installed. Run: pip install boto3")
    sys.exit(1)

from src.pipeline.config import (
    R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY, R2_PUBLIC_URL, WEBSITE_DATA_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("upload_r2")


def md5_hex(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_client():
    for var, val in [("R2_ACCOUNT_ID", R2_ACCOUNT_ID),
                     ("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID),
                     ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY)]:
        if not val:
            print(f"ERROR: {var} not set in .env"); sys.exit(1)
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def list_remote_etags(client) -> dict:
    etags = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME):
        for obj in page.get("Contents", []):
            etags[obj["Key"]] = obj["ETag"].strip('"').lower()
    return etags


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    data_dir = Path(WEBSITE_DATA_DIR)
    local_files = sorted(data_dir.rglob("*.json"))
    logger.info(f"Bucket:      {R2_BUCKET_NAME}")
    logger.info(f"Public URL:  {R2_PUBLIC_URL}")
    logger.info(f"Local files: {len(local_files):,} JSON files")

    client = get_client()

    remote_etags = {}
    if not args.force:
        logger.info("Fetching remote file list...")
        remote_etags = list_remote_etags(client)
        logger.info(f"  {len(remote_etags):,} files already on R2")

    to_upload = []
    for path in local_files:
        key = path.relative_to(data_dir).as_posix()
        if md5_hex(path) != remote_etags.get(key, ""):
            to_upload.append((path, key))

    logger.info(f"  {len(local_files) - len(to_upload):,} unchanged, "
                f"{len(to_upload):,} to upload")

    if args.dry_run:
        for _, key in to_upload:
            logger.info(f"  [dry-run] {key}")
        return

    if not to_upload:
        logger.info("Already up to date."); return

    uploaded = errors = 0
    for path, key in to_upload:
        try:
            client.upload_file(
                str(path), R2_BUCKET_NAME, key,
                ExtraArgs={
                    "ContentType": "application/json",
                    "CacheControl": "public, max-age=3600",
                },
            )
            uploaded += 1
            if uploaded % 50 == 0 or uploaded == len(to_upload):
                logger.info(f"  {uploaded}/{len(to_upload)} uploaded")
        except ClientError as e:
            logger.error(f"  FAILED: {key} — {e}")
            errors += 1

    logger.info(f"Done: {uploaded} uploaded, {errors} errors")
    if R2_PUBLIC_URL:
        logger.info(f"Live at: {R2_PUBLIC_URL}/index.json")


if __name__ == "__main__":
    main()
