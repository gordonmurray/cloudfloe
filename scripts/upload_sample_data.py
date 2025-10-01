#!/usr/bin/env python3
"""
Upload pre-generated sample data to MinIO.
"""

import os
import sys
import boto3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def upload_sample_data(sample_dir='sample_data', bucket_name='movies'):
    """Upload sample data directory to MinIO."""

    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'cloudfloe'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'cloudfloe123'),
        region_name='us-east-1'
    )

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"✓ Created bucket: {bucket_name}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"✓ Bucket {bucket_name} already exists")
    except Exception as e:
        logger.error(f"Failed to create bucket: {e}")
        raise

    # Upload all files from sample_data directory
    sample_path = Path(sample_dir)
    if not sample_path.exists():
        logger.error(f"Sample data directory not found: {sample_dir}")
        sys.exit(1)

    logger.info(f"📤 Uploading sample data from {sample_dir}...")

    file_count = 0
    total_size = 0

    for file_path in sample_path.rglob("*.parquet"):
        # Get relative path from sample_dir
        relative_path = file_path.relative_to(sample_path)
        s3_key = str(relative_path)

        # Upload file
        s3_client.upload_file(str(file_path), bucket_name, s3_key)

        file_size = file_path.stat().st_size
        total_size += file_size
        file_count += 1
        logger.info(f"  ✓ {s3_key} ({file_size / 1024:.1f} KB)")

    logger.info(f"\n🎉 Upload complete!")
    logger.info(f"  Files: {file_count}")
    logger.info(f"  Total size: {total_size / 1024 / 1024:.1f} MB")
    logger.info(f"  Bucket: s3://{bucket_name}/")
    logger.info(f"\n✨ Ready to query!")
    logger.info(f"  MinIO Console: http://localhost:9001")
    logger.info(f"  Credentials: cloudfloe / cloudfloe123")
    logger.info(f"  Sample query: SELECT * FROM read_parquet('s3://{bucket_name}/data/**/*.parquet')")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python upload_sample_data.py <sample_data_dir>")
        sys.exit(1)

    sample_dir = sys.argv[1]

    try:
        upload_sample_data(sample_dir)
    except Exception as e:
        logger.error(f"\n❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
