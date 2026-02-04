"""High-level storage abstraction implementing the medallion architecture.

Provides layer-aware utilities for uploading, downloading, listing,
and promoting data across Bronze (raw), Silver (staging), and Gold (curated) layers.

Usage:
    from utils.storage import StorageLayer, upload_to_layer, promote_data

    # Upload raw data to bronze layer
    uri = upload_to_layer("/tmp/data.csv", StorageLayer.BRONZE, "postgres/users.csv")

    # Promote cleaned data from bronze to silver
    promote_data("postgres/users.csv", StorageLayer.BRONZE, StorageLayer.SILVER)
"""

import os
import logging
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, List, Optional

from utils.minio_client import (
    get_minio_client,
    upload_file,
    download_file,
    list_objects,
    create_bucket,
    bucket_exists,
    copy_object,
    put_object_with_metadata,
)

log = logging.getLogger(__name__)

# Bucket name constants matching the medallion architecture
BRONZE_BUCKET = "raw"
SILVER_BUCKET = "staging"
GOLD_BUCKET = "curated"


class StorageLayer(Enum):
    """Medallion architecture storage layers."""

    BRONZE = BRONZE_BUCKET
    SILVER = SILVER_BUCKET
    GOLD = GOLD_BUCKET


# Mapping from layer to bucket name
_LAYER_BUCKETS: Dict[StorageLayer, str] = {
    StorageLayer.BRONZE: BRONZE_BUCKET,
    StorageLayer.SILVER: SILVER_BUCKET,
    StorageLayer.GOLD: GOLD_BUCKET,
}


def _bucket_for_layer(layer: StorageLayer) -> str:
    """Return the bucket name for a given storage layer."""
    return _LAYER_BUCKETS[layer]


def get_layer_path(layer: StorageLayer, object_key: str) -> str:
    """Construct a standardized S3 URI for a given layer and object key.

    Args:
        layer: Target storage layer.
        object_key: Object key within the bucket.

    Returns:
        Full S3 URI, e.g. ``s3://raw/postgres/users.csv``.
    """
    bucket = _bucket_for_layer(layer)
    return f"s3://{bucket}/{object_key}"


def upload_to_layer(
    file_path: str,
    layer: StorageLayer,
    object_key: str,
    metadata: Optional[Dict[str, str]] = None,
) -> str:
    """Upload a local file to the specified medallion layer.

    Automatically routes to the correct bucket based on *layer*, attaches
    metadata tags (upload timestamp, layer name, source), and ensures the
    target bucket exists.

    Args:
        file_path: Path to the local file.
        layer: Target storage layer (BRONZE, SILVER, GOLD).
        object_key: Object key within the layer bucket.
        metadata: Optional additional metadata dict.

    Returns:
        Full S3 URI of the uploaded object.
    """
    bucket = _bucket_for_layer(layer)
    combined_metadata = {
        "upload_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": layer.name.lower(),
        "source": os.path.basename(file_path),
    }
    if metadata:
        combined_metadata.update(metadata)

    put_object_with_metadata(file_path, bucket, object_key, combined_metadata)
    uri = get_layer_path(layer, object_key)
    log.info("Uploaded %s to %s", file_path, uri)
    return uri


def download_from_layer(
    layer: StorageLayer,
    object_key: str,
    file_path: str,
) -> str:
    """Download a file from the specified medallion layer.

    Args:
        layer: Source storage layer.
        object_key: Object key within the layer bucket.
        file_path: Local path to save the downloaded file.

    Returns:
        The local file path.
    """
    bucket = _bucket_for_layer(layer)
    download_file(bucket, object_key, file_path)
    log.info("Downloaded %s to %s", get_layer_path(layer, object_key), file_path)
    return file_path


def list_layer_objects(
    layer: StorageLayer,
    prefix: Optional[str] = None,
    max_keys: int = 1000,
) -> List[dict]:
    """List objects within a specific medallion layer.

    Args:
        layer: Storage layer to list.
        prefix: Optional prefix filter within the layer.
        max_keys: Maximum number of keys to return.

    Returns:
        List of object metadata dicts.
    """
    bucket = _bucket_for_layer(layer)
    return list_objects(bucket, prefix=prefix, max_keys=max_keys)


def promote_data(
    object_key: str,
    source_layer: StorageLayer,
    target_layer: StorageLayer,
    target_key: Optional[str] = None,
) -> str:
    """Promote (copy) data from one medallion layer to another.

    Copies the object from the source layer bucket to the target layer
    bucket. Commonly used for bronze -> silver or silver -> gold promotion.

    Args:
        object_key: Object key in the source bucket.
        source_layer: Layer to copy from.
        target_layer: Layer to copy to.
        target_key: Optional different key in target bucket.
            Defaults to the same key as the source.

    Returns:
        Full S3 URI of the promoted object in the target layer.
    """
    source_bucket = _bucket_for_layer(source_layer)
    target_bucket = _bucket_for_layer(target_layer)
    dest_key = target_key or object_key

    copy_object(source_bucket, object_key, target_bucket, dest_key)

    uri = get_layer_path(target_layer, dest_key)
    log.info(
        "Promoted %s -> %s",
        get_layer_path(source_layer, object_key),
        uri,
    )
    return uri
