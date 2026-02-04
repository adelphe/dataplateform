"""Generate sample input data files for testing the batch ingestion pipeline.

Creates CSV and JSON files in the ``data/input/`` directory with
realistic sample data for customers, products, and transactions.

Usage:
    python scripts/generate_sample_input.py
    python scripts/generate_sample_input.py --output-dir /custom/path --date 2024-02-01
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta


def generate_customers(output_dir: str, date_str: str, count: int = 50) -> str:
    """Generate a sample customers CSV file.

    Args:
        output_dir: Target directory.
        date_str: Date string for the filename (YYYYMMDD).
        count: Number of customer records.

    Returns:
        Path to the generated file.
    """
    first_names = [
        "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace",
        "Henry", "Iris", "Jack", "Karen", "Leo", "Mia", "Noah",
        "Olivia", "Peter", "Quinn", "Rachel", "Sam", "Tina",
    ]
    last_names = [
        "Johnson", "Smith", "Williams", "Brown", "Davis", "Miller",
        "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson",
        "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez",
    ]
    cities = [
        "New York", "San Francisco", "Chicago", "Austin", "Seattle",
        "Boston", "Denver", "Portland", "Miami", "Atlanta",
        "Los Angeles", "Houston", "Phoenix", "Philadelphia", "Dallas",
    ]

    file_path = os.path.join(output_dir, f"customers_{date_str}.csv")
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "phone", "city", "signup_date"])
        for i in range(1, count + 1):
            first = random.choice(first_names)
            last = random.choice(last_names)
            name = f"{first} {last}"
            email = f"{first.lower()}.{last.lower()}@example.com"
            phone = f"+1-555-{random.randint(1000, 9999):04d}"
            city = random.choice(cities)
            base_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            signup = base_date.strftime("%Y-%m-%d")
            writer.writerow([i, name, email, phone, city, signup])

    print(f"Generated {count} customers -> {file_path}")
    return file_path


def generate_products(output_dir: str, date_str: str, count: int = 30) -> str:
    """Generate a sample products CSV file.

    Args:
        output_dir: Target directory.
        date_str: Date string for the filename (YYYYMMDD).
        count: Number of product records.

    Returns:
        Path to the generated file.
    """
    categories = ["Electronics", "Office Supplies", "Home & Office", "Accessories"]
    adjectives = ["Wireless", "Premium", "Compact", "Pro", "Ultra", "Mini", "Smart"]
    nouns = [
        "Mouse", "Keyboard", "Cable", "Lamp", "Speaker", "Webcam",
        "Monitor", "Headset", "Charger", "Stand", "Hub", "Adapter",
    ]

    file_path = os.path.join(output_dir, f"products_{date_str}.csv")
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "name", "category", "price", "stock_quantity", "created_at"])
        for i in range(1, count + 1):
            name = f"{random.choice(adjectives)} {random.choice(nouns)}"
            category = random.choice(categories)
            price = round(random.uniform(5.0, 200.0), 2)
            stock = random.randint(10, 500)
            base_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            created = base_date.strftime("%Y-%m-%d")
            writer.writerow([200 + i, name, category, price, stock, created])

    print(f"Generated {count} products -> {file_path}")
    return file_path


def generate_transactions(
    output_dir: str, date_str: str, count: int = 100
) -> str:
    """Generate a sample transactions JSON file.

    Args:
        output_dir: Target directory.
        date_str: Date string for the filename (YYYYMMDD).
        count: Number of transaction records.

    Returns:
        Path to the generated file.
    """
    currencies = ["USD", "EUR", "GBP"]

    records = []
    base_dt = datetime.strptime(date_str, "%Y%m%d")
    for i in range(1, count + 1):
        ts = base_dt + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        records.append({
            "transaction_id": 1000 + i,
            "customer_id": random.randint(1, 50),
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(currencies),
            "product_id": random.randint(201, 230),
            "quantity": random.randint(1, 5),
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    file_path = os.path.join(output_dir, f"transactions_{date_str}.json")
    with open(file_path, "w") as f:
        json.dump(records, f, indent=2)

    print(f"Generated {count} transactions -> {file_path}")
    return file_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample input data for the batch ingestion pipeline"
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data", "input"),
        help="Output directory for generated files (default: data/input/)",
    )
    parser.add_argument(
        "--date",
        default="20240101",
        help="Date string for filenames in YYYYMMDD format (default: 20240101)",
    )
    parser.add_argument(
        "--customers", type=int, default=50, help="Number of customer records"
    )
    parser.add_argument(
        "--products", type=int, default=30, help="Number of product records"
    )
    parser.add_argument(
        "--transactions", type=int, default=100, help="Number of transaction records"
    )
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Generating sample data in {output_dir}")
    print(f"Date: {args.date}")
    print()

    generate_customers(output_dir, args.date, args.customers)
    generate_products(output_dir, args.date, args.products)
    generate_transactions(output_dir, args.date, args.transactions)

    print()
    print("Sample data generation complete!")


if __name__ == "__main__":
    main()
