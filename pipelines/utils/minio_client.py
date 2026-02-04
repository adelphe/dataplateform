"""MinIO (S3-compatible) client utilities for the data platform."""

import os
import logging
from typing import List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

_client = None


def get_minio_client():
    """Create or return a cached boto3 S3 client configured for MinIO.

    Uses connection pooling via module-level caching.
    Credentials are read from environment variables.
    """
    global _client
    if _client is not None:
        return _client

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")

    config = Config(
        retries={"max_attempts": 3, "mode": "standard"},
        max_pool_connections=10,
    )

    _client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=config,
    )
    return _client


def upload_file(
    file_path: str,
    bucket_name: str,
    object_name: str,
    create_bucket: bool = True,
) -> str:
    """Upload a local file to a MinIO bucket.

    Args:
        file_path: Path to the local file.
        bucket_name: Target bucket name.
        object_name: Object key in the bucket.
        create_bucket: Create the bucket if it doesn't exist.

    Returns:
        The object name (key) of the uploaded file.
    """
    client = get_minio_client()

    if create_bucket:
        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError:
            log.info("Creating bucket: %s", bucket_name)
            client.create_bucket(Bucket=bucket_name)

    client.upload_file(Filename=file_path, Bucket=bucket_name, Key=object_name)
    log.info("Uploaded %s to %s/%s", file_path, bucket_name, object_name)
    return object_name


def download_file(
    bucket_name: str,
    object_name: str,
    file_path: str,
) -> str:
    """Download a file from a MinIO bucket.

    Args:
        bucket_name: Source bucket name.
        object_name: Object key in the bucket.
        file_path: Local path to save the downloaded file.

    Returns:
        The local file path.
    """
    client = get_minio_client()
    client.download_file(Bucket=bucket_name, Key=object_name, Filename=file_path)
    log.info("Downloaded %s/%s to %s", bucket_name, object_name, file_path)
    return file_path


def list_objects(
    bucket_name: str,
    prefix: Optional[str] = None,
    max_keys: int = 1000,
) -> List[dict]:
    """List objects in a MinIO bucket.

    Args:
        bucket_name: Bucket to list.
        prefix: Optional prefix filter.
        max_keys: Maximum number of keys to return.

    Returns:
        List of object metadata dicts with 'Key', 'Size', 'LastModified'.
    """
    client = get_minio_client()
    kwargs = {"Bucket": bucket_name, "MaxKeys": max_keys}
    if prefix:
        kwargs["Prefix"] = prefix

    objects = []
    while True:
        response = client.list_objects_v2(**kwargs)
        if "Contents" in response:
            objects.extend(response["Contents"])
        if not response.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = response["NextContinuationToken"]

    log.info("Listed %d objects in %s (prefix=%s)", len(objects), bucket_name, prefix)
    return objects


def delete_object(bucket_name: str, object_name: str) -> None:
    """Delete an object from a MinIO bucket.

    Args:
        bucket_name: Bucket containing the object.
        object_name: Object key to delete.
    """
    client = get_minio_client()
    client.delete_object(Bucket=bucket_name, Key=object_name)
    log.info("Deleted %s/%s", bucket_name, object_name)
