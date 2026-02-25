#!/usr/bin/env python3
"""
Upload IMPACT JSON data files to Cloudflare R2.

Syncs docs/data/ to the R2 bucket, skipping files that haven't changed
(checked by ETag / MD5). Run after compute_snapshots.py.

Requirements:
    pip install boto3

Credentials (in .env or environment):
    R2_ACCOUNT_ID
    R2_BUCKET_NAME
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_PUBLIC_URL  (informational only)

Usage:
    python scripts/upload_to_r2.py            # sync all
    python scripts/upload_to_r2.py --dry-run  # show what would change
    python scripts/upload_to_r2.py --force    # re-upload everything
"""

import sys
import os
import hashlib
import argparse
import logging
import mimetypes
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


def md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_r2_client():
    if not R2_ACCESS_KEY_ID or R2_ACCESS_KEY_ID == "REPLACE_ME":
        print("ERROR: R2_ACCESS_KEY_ID not set in .env")
        sys.exit(1)
    if not R2_SECRET_ACCESS_KEY:
        print("ERROR: R2_SECRET_ACCESS_KEY not set in .env")
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def list_remote_etags(client, bucket: str) -> dict:
    """Return {key: etag_hex} for all objects in the bucket."""
    etags = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            # ETags from S3/R2 are quoted MD5 hex strings
            etags[obj["Key"]] = obj["ETag"].strip('"').lower()
    return etags


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded without doing it")
    parser.add_argument("--force", action="store_true",
                        help="Re-upload all files regardless of ETag")
    args = parser.parse_args()

    data_dir = Path(WEBSITE_DATA_DIR)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        sys.exit(1)

    client = get_r2_client()

    logger.info(f"Bucket: {R2_BUCKET_NAME}")
    logger.info(f"Public URL: {R2_PUBLIC_URL}")

    # Collect local files
    local_files = sorted(data_dir.rglob("*.json"))
    logger.info(f"Found {len(local_files):,} local JSON files")

    # Get remote ETags (skip if force)
    remote_etags = {} if args.force else list_remote_etags(client, R2_BUCKET_NAME)
    logger.info(f"  {len(remote_etags):,} files already on R2")

    # Determine what needs uploading
    to_upload = []
    for local_path in local_files:
        key = local_path.relative_to(data_dir).as_posix()  # e.g. journals/nejm.json
        local_md5 = md5_of_file(local_path)
        remote_md5 = remote_etags.get(key, "")
        if local_md5 != remote_md5:
            to_upload.append((local_path, key, local_md5))

    skipped = len(local_files) - len(to_upload)
    logger.info(f"  {skipped:,} unchanged (skipping), {len(to_upload):,} to upload")

    if args.dry_run:
        for _, key, _ in to_upload:
            logger.info(f"  [dry-run] would upload: {key}")
        return

    if not to_upload:
        logger.info("Nothing to upload — R2 is already up to date.")
        return

    # Upload
    uploaded = 0
    errors = 0
    for local_path, key, _ in to_upload:
        try:
            client.upload_file(
                str(local_path),
                R2_BUCKET_NAME,
                key,
                ExtraArgs={
                    "ContentType": "application/json",
                    "CacheControl": "public, max-age=3600",
                },
            )
            uploaded += 1
            if uploaded % 50 == 0 or uploaded == len(to_upload):
                logger.info(f"  {uploaded}/{len(to_upload)} uploaded")
        except ClientError as e:
            logger.error(f"  Failed to upload {key}: {e}")
            errors += 1

    logger.info(f"Done: {uploaded} uploaded, {errors} errors")
    if R2_PUBLIC_URL:
        logger.info(f"Data live at: {R2_PUBLIC_URL}/")


if __name__ == "__main__":
    main()
