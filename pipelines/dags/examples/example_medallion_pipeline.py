"""Example DAG: Medallion Architecture Pipeline (Bronze -> Silver -> Gold).

Demonstrates a complete data pipeline flowing through the three medallion
layers using the storage utility library:

1. **Bronze**: Generate and upload raw sample data
2. **Silver**: Clean, validate, and deduplicate the data
3. **Gold**: Aggregate the cleaned data for analytics

This DAG is for demonstration purposes. Adapt the patterns for your
production pipelines.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task

from utils.storage import (
    StorageLayer,
    upload_to_layer,
    download_from_layer,
    list_layer_objects,
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="example_medallion_pipeline",
    default_args=default_args,
    description="Demonstrates the bronze -> silver -> gold medallion pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "medallion", "etl"],
)
def medallion_pipeline():

    @task
    def ingest_to_bronze(**context) -> str:
        """Generate raw sample data and upload to the Bronze layer.

        In a real pipeline, this task would extract data from an external
        source (database, API, file system) and upload it as-is.
        """
        ds = context["ds"]

        # Generate sample raw data (simulating a source extract)
        now = datetime.now(timezone.utc)
        records = []
        for i in range(1, 101):
            records.append(
                {
                    "id": i,
                    "name": f"User_{i}",
                    "email": f"user_{i}@example.com" if i % 10 != 0 else None,
                    "amount": round(10.0 + (i * 2.7 % 200), 2),
                    "created_at": (now - timedelta(days=100 - i)).isoformat(),
                }
            )
        # Add some duplicates to demonstrate deduplication
        records.append(records[0].copy())
        records.append(records[5].copy())

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
            json.dump(records, f, indent=2)
            tmp_path = f.name

        object_key = f"sample/users/{ds}/raw_users.json"
        uri = upload_to_layer(
            tmp_path,
            StorageLayer.BRONZE,
            object_key,
            metadata={"source": "example_generator", "execution_date": ds},
        )
        os.unlink(tmp_path)
        return object_key

    @task
    def clean_to_silver(bronze_key: str, **context) -> str:
        """Read from Bronze, clean the data, and write to the Silver layer.

        Cleaning steps:
        - Remove rows with null email
        - Deduplicate by id
        - Standardize column types
        """
        ds = context["ds"]

        # Download from bronze
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmp_input = f.name
        download_from_layer(StorageLayer.BRONZE, bronze_key, tmp_input)

        # Read and clean
        with open(tmp_input) as f:
            raw_data = json.load(f)
        os.unlink(tmp_input)

        df = pd.DataFrame(raw_data)
        rows_before = len(df)

        # Remove nulls in required fields
        df = df.dropna(subset=["email"])

        # Deduplicate
        df = df.drop_duplicates(subset=["id"], keep="first")

        # Standardize types
        df["amount"] = df["amount"].astype(float)
        df["id"] = df["id"].astype(int)

        rows_after = len(df)
        print(f"Cleaned: {rows_before} rows -> {rows_after} rows "
              f"({rows_before - rows_after} removed)")

        # Write to silver as parquet
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_output = f.name
        df.to_parquet(tmp_output, index=False)

        object_key = f"cleaned/users/{ds}/users_clean.parquet"
        upload_to_layer(
            tmp_output,
            StorageLayer.SILVER,
            object_key,
            metadata={"rows": str(rows_after), "execution_date": ds},
        )
        os.unlink(tmp_output)
        return object_key

    @task
    def aggregate_to_gold(silver_key: str, **context) -> str:
        """Read from Silver, aggregate, and write to the Gold layer.

        Produces a summary with total users and revenue by date bucket.
        """
        ds = context["ds"]

        # Download from silver
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_input = f.name
        download_from_layer(StorageLayer.SILVER, silver_key, tmp_input)

        df = pd.read_parquet(tmp_input)
        os.unlink(tmp_input)

        # Aggregate
        df["created_date"] = pd.to_datetime(df["created_at"]).dt.date.astype(str)
        summary = (
            df.groupby("created_date")
            .agg(
                user_count=("id", "count"),
                total_amount=("amount", "sum"),
                avg_amount=("amount", "mean"),
            )
            .reset_index()
        )
        summary["total_amount"] = summary["total_amount"].round(2)
        summary["avg_amount"] = summary["avg_amount"].round(2)

        print(f"Aggregated {len(df)} rows into {len(summary)} date groups")

        # Write to gold
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_output = f.name
        summary.to_parquet(tmp_output, index=False)

        object_key = f"aggregated/user_summary/{ds}/summary.parquet"
        upload_to_layer(
            tmp_output,
            StorageLayer.GOLD,
            object_key,
            metadata={"groups": str(len(summary)), "execution_date": ds},
        )
        os.unlink(tmp_output)
        return object_key

    @task
    def verify_layers(**context):
        """Verify that data exists in all three layers after the pipeline run."""
        ds = context["ds"]

        for layer, prefix in [
            (StorageLayer.BRONZE, f"sample/users/{ds}/"),
            (StorageLayer.SILVER, f"cleaned/users/{ds}/"),
            (StorageLayer.GOLD, f"aggregated/user_summary/{ds}/"),
        ]:
            objects = list_layer_objects(layer, prefix=prefix)
            count = len(objects)
            print(f"{layer.name} ({layer.value}): {count} objects with prefix '{prefix}'")
            if count == 0:
                raise ValueError(
                    f"Expected objects in {layer.name} layer with prefix '{prefix}'"
                )

        print("All layers verified successfully.")

    # Define task dependencies: bronze -> silver -> gold -> verify
    bronze_key = ingest_to_bronze()
    silver_key = clean_to_silver(bronze_key)
    gold_key = aggregate_to_gold(silver_key)
    verify_layers()


medallion_pipeline()
