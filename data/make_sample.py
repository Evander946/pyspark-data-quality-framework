"""Create small synthetic CSVs with known problems (fake data, seeded)."""
import csv
import random
from pathlib import Path

rnd = random.Random(1)
out = Path(__file__).parent
with open(out / "customers.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["customer_id", "email", "age", "country"])
    for i in range(1, 101):
        w.writerow([f"C{i:03d}", f"user{i}@example.com", rnd.randint(18, 70), rnd.choice(["IN", "US", "GB", "DE"])])
    w.writerow(["C050", "user50@example.com", 33, "IN"])          # duplicate key
    w.writerow(["C101", "not-an-email", 150, "XX"])               # bad email, age, country
    w.writerow(["C102", "", 25, "IN"])                            # missing email
with open(out / "orders.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["order_id", "customer_id", "amount", "status"])
    for i in range(1, 301):
        w.writerow([f"O{i:04d}", f"C{rnd.randint(1, 100):03d}", round(rnd.uniform(5, 400), 2), rnd.choice(["placed", "shipped", "delivered"])])
    w.writerow(["O0300", "C001", 20.0, "placed"])                  # duplicate key
    w.writerow(["O0301", "C999", 15.0, "shipped"])                 # orphan customer
    w.writerow(["O0302", "C002", -5.0, "shipped"])                 # negative amount
    w.writerow(["O0303", "C003", 10.0, "teleported"])              # invalid status
with open(out / "orders_source_extract.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["order_id"])
    for i in range(1, 306): w.writerow([f"O{i:04d}"])
