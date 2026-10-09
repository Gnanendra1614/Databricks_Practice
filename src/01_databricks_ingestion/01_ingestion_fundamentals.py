import csv
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

SOURCE_FILE = DATA_DIR / "orders.csv"

orders = [
    {
        "order_id": 101,
        "customer_name": "Ravi",
        "product": "Laptop",
        "amount": 55000
    },
    {
        "order_id": 102,
        "customer_name": "Priya",
        "product": "Mobile",
        "amount": 25000
    },
    {
        "order_id": 103,
        "customer_name": "Kiran",
        "product": "Headphones",
        "amount": 2000
    }
]

# Write source data
with SOURCE_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

print(f"Source file created: {SOURCE_FILE}")

# Read the CSV as a batch
with SOURCE_FILE.open("r", newline="", encoding="utf-8") as file:
    loaded_orders = list(csv.DictReader(file))

# Validate the records
print(f"Records ingested: {len(loaded_orders)}")

for order in loaded_orders:
    order["amount"] = float(order["amount"])
    print(order)

assert len(loaded_orders) == 3
print("Batch ingestion completed successfully!")
