#!/usr/bin/env python3
"""
Seed MinIO with the demo dataset as an Apache Iceberg table.

Reads the local Hive-partitioned Parquet sample, combines it into a single
Arrow table, and writes it as an Iceberg v2 table to
`s3://<bucket>/warehouse/demo/movies` using pyiceberg. DuckDB's
`iceberg_scan()` can then read the table directly, without a catalog.
"""
import logging
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


BUCKET = os.getenv("DEMO_BUCKET", "movies")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"s3://{BUCKET}/warehouse")
NAMESPACE = os.getenv("DEMO_NAMESPACE", "demo")
TABLE_NAME = os.getenv("DEMO_TABLE", "movies")

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "cloudfloe")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "cloudfloe123")
S3_REGION = os.getenv("MINIO_REGION", "us-east-1")


def ensure_bucket(bucket_name: str) -> None:
    """Create the target bucket in MinIO if it doesn't already exist."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )
    try:
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"Created bucket: {bucket_name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"Bucket {bucket_name} already exists")


def load_arrow_table(sample_dir: Path):
    """Read the Hive-partitioned Parquet dataset into a single Arrow table."""
    data_dir = sample_dir / "data"
    if not data_dir.exists():
        logger.error(f"Sample data directory not found: {data_dir}")
        sys.exit(1)

    dataset = pq.ParquetDataset(str(data_dir))
    arrow_table = dataset.read()
    logger.info(
        f"Loaded {arrow_table.num_rows} rows, {arrow_table.num_columns} columns"
    )
    return arrow_table


def write_iceberg_table(arrow_table) -> str:
    """Create (or replace) the Iceberg demo table and append the rows."""
    catalog = SqlCatalog(
        "default",
        **{
            "uri": "sqlite:///:memory:",
            "warehouse": WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
        },
    )

    existing_namespaces = {ns[0] for ns in catalog.list_namespaces()}
    if NAMESPACE not in existing_namespaces:
        catalog.create_namespace(NAMESPACE)
        logger.info(f"Created namespace: {NAMESPACE}")

    identifier = f"{NAMESPACE}.{TABLE_NAME}"
    table_location = f"{WAREHOUSE}/{NAMESPACE}/{TABLE_NAME}"

    if catalog.table_exists(identifier):
        logger.info(f"Dropping existing table: {identifier}")
        catalog.drop_table(identifier)

    table = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
        location=table_location,
    )
    logger.info(f"Created Iceberg table: {identifier} at {table_location}")

    table.append(arrow_table)
    logger.info(f"Appended {arrow_table.num_rows} rows")

    return table_location


def main(sample_dir: str) -> None:
    ensure_bucket(BUCKET)
    arrow_table = load_arrow_table(Path(sample_dir))
    table_location = write_iceberg_table(arrow_table)

    logger.info("\nUpload complete!")
    logger.info(f"  Rows: {arrow_table.num_rows}")
    logger.info(f"  Table: {table_location}")
    logger.info("\nReady to query!")
    logger.info("  MinIO Console: http://localhost:9001")
    logger.info(f"  Credentials: {S3_ACCESS_KEY} / {S3_SECRET_KEY}")
    logger.info(
        f"  Sample query: SELECT * FROM iceberg_scan('{table_location}') LIMIT 10"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python upload_sample_data.py <sample_data_dir>")
        sys.exit(1)

    try:
        main(sys.argv[1])
    except Exception as e:
        logger.error(f"\nFailed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
