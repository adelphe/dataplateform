#!/usr/bin/env python3
"""Upload sample datasets to the Bronze (raw) storage layer for testing.

Generates and uploads sample customer, transaction, and product data
to the MinIO raw bucket under the ``sample/`` prefix.

Usage:
    python upload_sample_data.py
    python upload_sample_data.py --count 500
    python upload_sample_data.py --layer bronze
"""

import argparse
import io
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd

# Allow imports from the pipelines package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipelines"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.storage import StorageLayer, upload_to_layer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LAYER_MAP = {
    "bronze": StorageLayer.BRONZE,
    "silver": StorageLayer.SILVER,
    "gold": StorageLayer.GOLD,
}


def generate_customers(count: int) -> pd.DataFrame:
    """Generate sample customer data."""
    now = datetime.now(timezone.utc)
    data = {
        "id": list(range(1, count + 1)),
        "name": [f"Customer_{i}" for i in range(1, count + 1)],
        "email": [f"customer_{i}@example.com" for i in range(1, count + 1)],
        "created_at": [
            (now - timedelta(days=count - i)).isoformat() for i in range(1, count + 1)
        ],
    }
    return pd.DataFrame(data)


def generate_transactions(count: int) -> list:
    """Generate sample transaction data as a list of dicts."""
    now = datetime.now(timezone.utc)
    transactions = []
    for i in range(1, count + 1):
        transactions.append(
            {
                "transaction_id": f"TXN-{i:06d}",
                "customer_id": (i % 100) + 1,
                "amount": round(10.0 + (i * 3.7 % 500), 2),
                "timestamp": (now - timedelta(hours=count - i)).isoformat(),
            }
        )
    return transactions


def generate_products(count: int) -> pd.DataFrame:
    """Generate sample product catalog data."""
    categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
    data = {
        "product_id": list(range(1, count + 1)),
        "name": [f"Product_{i}" for i in range(1, count + 1)],
        "category": [categories[i % len(categories)] for i in range(count)],
        "price": [round(5.0 + (i * 2.3 % 200), 2) for i in range(1, count + 1)],
    }
    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="Upload sample data to MinIO")
    parser.add_argument(
        "--count", type=int, default=100, help="Number of rows to generate (default: 100)"
    )
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        default="bronze",
        help="Target storage layer (default: bronze)",
    )
    args = parser.parse_args()

    layer = LAYER_MAP[args.layer]
    count = args.count

    log.info("Generating sample data (%d rows per dataset)...", count)

    with tempfile.TemporaryDirectory() as tmpdir:
        # --- Customers CSV ---
        customers = generate_customers(count)
        csv_path = os.path.join(tmpdir, "customers.csv")
        customers.to_csv(csv_path, index=False)
        uri = upload_to_layer(
            csv_path, layer, "sample/customers.csv", metadata={"format": "csv"}
        )
        log.info("Customers uploaded: %s (%d rows)", uri, len(customers))

        # --- Transactions JSON ---
        transactions = generate_transactions(count)
        json_path = os.path.join(tmpdir, "transactions.json")
        with open(json_path, "w") as f:
            json.dump(transactions, f, indent=2)
        uri = upload_to_layer(
            json_path, layer, "sample/transactions.json", metadata={"format": "json"}
        )
        log.info("Transactions uploaded: %s (%d rows)", uri, len(transactions))

        # --- Products Parquet ---
        products = generate_products(count)
        parquet_path = os.path.join(tmpdir, "products.parquet")
        products.to_parquet(parquet_path, index=False)
        uri = upload_to_layer(
            parquet_path, layer, "sample/products.parquet", metadata={"format": "parquet"}
        )
        log.info("Products uploaded: %s (%d rows)", uri, len(products))

    log.info("Sample data upload complete.")


if __name__ == "__main__":
    main()
