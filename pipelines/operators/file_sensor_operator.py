"""Custom sensor operator for detecting files in MinIO buckets."""

import fnmatch
import os
import logging

import boto3
from botocore.exceptions import ClientError

from airflow.sdk.bases.hook import BaseHook
from airflow.sensors.base import BaseSensorOperator

log = logging.getLogger(__name__)


class MinIOFileSensorOperator(BaseSensorOperator):
    """Sensor that waits for a file to appear in a MinIO bucket.

    Supports both exact prefix matching and wildcard patterns.

    Args:
        bucket_name: MinIO bucket to monitor.
        prefix: Object key prefix to search for.
        minio_conn_id: Airflow connection ID for MinIO.
        wildcard_match: If True, treat prefix as a wildcard pattern.
    """

    template_fields = ("bucket_name", "prefix")

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        minio_conn_id: str = "minio_default",
        wildcard_match: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.minio_conn_id = minio_conn_id
        self.wildcard_match = wildcard_match

    def _get_client(self):
        """Create a boto3 S3 client configured for MinIO using the Airflow connection."""
        try:
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
        except Exception:
            log.warning(
                "Could not retrieve Airflow connection '%s', "
                "falling back to environment variables.",
                self.minio_conn_id,
            )
            endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
            access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")

        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

    def poke(self, context):
        """Check if the file exists in the MinIO bucket."""
        client = self._get_client()

        try:
            if self.wildcard_match:
                # For wildcard matching, list objects and filter
                search_prefix = self.prefix.split("*")[0]
                response = client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=search_prefix,
                )

                if "Contents" not in response:
                    log.info(
                        "No objects found in %s with prefix %s",
                        self.bucket_name,
                        search_prefix,
                    )
                    return False

                for obj in response["Contents"]:
                    if fnmatch.fnmatch(obj["Key"], self.prefix):
                        log.info(
                            "Found matching object: %s in %s",
                            obj["Key"],
                            self.bucket_name,
                        )
                        return True

                log.info(
                    "No objects matching pattern %s in %s",
                    self.prefix,
                    self.bucket_name,
                )
                return False
            else:
                # Exact prefix match - check if any objects exist
                response = client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=self.prefix,
                    MaxKeys=1,
                )

                found = "Contents" in response and len(response["Contents"]) > 0
                if found:
                    log.info(
                        "Found object with prefix %s in %s",
                        self.prefix,
                        self.bucket_name,
                    )
                else:
                    log.info(
                        "No objects with prefix %s in %s",
                        self.prefix,
                        self.bucket_name,
                    )
                return found

        except ClientError as e:
            log.warning("Error checking MinIO bucket %s: %s", self.bucket_name, str(e))
            return False
