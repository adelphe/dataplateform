"""Custom operator for uploading files to MinIO (S3-compatible storage)."""

import logging
import os

import boto3
from botocore.exceptions import ClientError

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class MinIOUploadOperator(BaseOperator):
    """Upload a file to a MinIO bucket.

    Args:
        bucket_name: Target MinIO bucket name.
        object_name: Object key in the bucket.
        file_path: Local path of the file to upload.
        minio_conn_id: Airflow connection ID for MinIO credentials.
        replace: Whether to overwrite if object already exists.
    """

    template_fields = ("bucket_name", "object_name", "file_path")

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        minio_conn_id: str = "minio_default",
        replace: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.file_path = file_path
        self.minio_conn_id = minio_conn_id
        self.replace = replace

    def _get_client(self):
        """Create a boto3 S3 client configured for MinIO using the Airflow connection."""
        conn = BaseHook.get_connection(self.minio_conn_id)

        extra = conn.extra_dejson if conn.extra else {}
        endpoint_url = extra.get("endpoint_url") or extra.get("host", "http://minio:9000")
        access_key = conn.login
        secret_key = conn.password

        if not access_key or not secret_key:
            raise ValueError(
                f"Connection '{self.minio_conn_id}' is missing login (access key) "
                "or password (secret key)."
            )

        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

    def _ensure_bucket_exists(self, client):
        """Create the bucket if it does not already exist."""
        try:
            client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            log.info("Bucket %s does not exist, creating it", self.bucket_name)
            client.create_bucket(Bucket=self.bucket_name)

    def execute(self, context):
        """Upload the file to MinIO with retry logic."""
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        client = self._get_client()
        self._ensure_bucket_exists(client)

        if not self.replace:
            try:
                client.head_object(Bucket=self.bucket_name, Key=self.object_name)
                log.info(
                    "Object %s already exists in %s and replace=False, skipping upload",
                    self.object_name,
                    self.bucket_name,
                )
                return self.object_name
            except ClientError:
                pass

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                client.upload_file(
                    Filename=self.file_path,
                    Bucket=self.bucket_name,
                    Key=self.object_name,
                )
                log.info(
                    "Successfully uploaded %s to %s/%s",
                    self.file_path,
                    self.bucket_name,
                    self.object_name,
                )
                return self.object_name
            except ClientError as e:
                if attempt < max_retries:
                    log.warning(
                        "Upload attempt %d/%d failed: %s. Retrying...",
                        attempt,
                        max_retries,
                        str(e),
                    )
                else:
                    log.error("Upload failed after %d attempts", max_retries)
                    raise
