#!/usr/bin/env python3
"""Verify MinIO bucket structure and connectivity.

Checks that the medallion architecture buckets (raw, staging, curated) exist,
lists their contents, and prints a tree-style summary.

Usage:
    python verify_buckets.py
"""

import logging
import os
import sys

# Allow imports from the pipelines package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipelines"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.minio_client import get_minio_client, bucket_exists, list_objects, list_buckets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

EXPECTED_BUCKETS = ["raw", "staging", "curated"]
BUCKET_DESCRIPTIONS = {
    "raw": "Bronze layer - raw ingested data",
    "staging": "Silver layer - cleaned and validated data",
    "curated": "Gold layer - business-ready aggregated data",
}


def check_connectivity():
    """Verify connectivity to MinIO."""
    print("=" * 50)
    print(" MinIO Bucket Verification")
    print("=" * 50)
    print()

    try:
        client = get_minio_client()
        client.list_buckets()
        print("[OK] MinIO is reachable")
    except Exception as e:
        print(f"[FAIL] Cannot connect to MinIO: {e}")
        sys.exit(1)


def verify_buckets():
    """Verify expected buckets exist and display their contents."""
    print()
    print("Checking expected buckets...")
    print("-" * 50)

    all_ok = True
    for bucket_name in EXPECTED_BUCKETS:
        exists = bucket_exists(bucket_name)
        desc = BUCKET_DESCRIPTIONS.get(bucket_name, "")
        if exists:
            print(f"  [OK]   {bucket_name:12s} - {desc}")
        else:
            print(f"  [MISS] {bucket_name:12s} - {desc}")
            all_ok = False

    return all_ok


def list_all_buckets():
    """List all buckets present in MinIO."""
    print()
    print("All buckets:")
    print("-" * 50)

    buckets = list_buckets()
    if not buckets:
        print("  (no buckets found)")
    for b in buckets:
        print(f"  - {b['Name']}  (created: {b['CreationDate']})")


def print_bucket_tree():
    """Print a tree-style listing of objects in each expected bucket."""
    print()
    print("Bucket contents:")
    print("-" * 50)

    for bucket_name in EXPECTED_BUCKETS:
        if not bucket_exists(bucket_name):
            print(f"  {bucket_name}/ (does not exist)")
            continue

        objects = list_objects(bucket_name)
        print(f"  {bucket_name}/ ({len(objects)} objects)")

        # Group by top-level prefix
        prefixes = {}
        for obj in objects:
            key = obj["Key"]
            parts = key.split("/", 1)
            prefix = parts[0] if len(parts) > 1 else ""
            if prefix not in prefixes:
                prefixes[prefix] = []
            prefixes[prefix].append(key)

        for prefix in sorted(prefixes.keys()):
            keys = prefixes[prefix]
            if prefix:
                print(f"    {prefix}/")
                for key in sorted(keys):
                    name = key.split("/", 1)[1] if "/" in key else key
                    print(f"      {name}")
            else:
                for key in sorted(keys):
                    print(f"    {key}")

    print()


def main():
    check_connectivity()
    all_ok = verify_buckets()
    list_all_buckets()
    print_bucket_tree()

    if all_ok:
        print("Verification PASSED: all expected buckets exist.")
    else:
        print("Verification WARNING: some expected buckets are missing.")
        sys.exit(1)


if __name__ == "__main__":
    main()
