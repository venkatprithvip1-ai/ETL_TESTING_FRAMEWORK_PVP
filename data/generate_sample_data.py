"""
generate_sample_data.py
=======================
Generates 5 large realistic CSV datasets for ETL DQ testing.

Datasets
--------
  1. employees.csv       — 100,000 rows  (HR data)
  2. orders.csv          — 200,000 rows  (E-commerce orders)
  3. products.csv        —  50,000 rows  (Product catalog)
  4. transactions.csv    — 500,000 rows  (Financial transactions)
  5. customers.csv       — 150,000 rows  (Customer profiles)

Each dataset has ~5–8% intentional data quality issues so that
the DQ checkers have something real to catch.

Run this ONCE before running the main DQ framework:
    python data/generate_sample_data.py
"""

import csv
import os
import random
import string
from datetime import date, timedelta

# ── Reproducible output — same data every run ────────────────────────────────
random.seed(42)

# ── Output folder ─────────────────────────────────────────────────────────────
OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Shared helpers ────────────────────────────────────────────────────────────

def rand_date(start_year: int = 2015, end_year: int = 2024) -> str:
    """Returns a random date string between start and end year."""
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")

def rand_future_date() -> str:
    """Returns a date that is in the future — intentional DQ issue."""
    future = date.today() + timedelta(days=random.randint(1, 365))
    return future.strftime("%Y-%m-%d")

def rand_email(name: str) -> str:
    """Generates a realistic email from a name."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.org", "work.net"]
    clean   = name.lower().replace(" ", ".")
    return f"{clean}@{random.choice(domains)}"

def inject_issues(value, issue_type: str, pct: float = 0.05):
    """
    Randomly replaces a value with a DQ issue.
    pct controls how often (5% by default).
    """
    if random.random() > pct:
        return value     # most of the time, return the clean value

    # Pick a random issue to inject
    if issue_type == "null":
        return ""
    elif issue_type == "duplicate_id":
        return str(random.randint(1, 100))  # force collision with a small number
    elif issue_type == "bad_email":
        return random.choice(["not_an_email", "missing@", "@nodomain", "test@test.com"])
    elif issue_type == "bad_date":
        return random.choice(["99-99-9999", "2025/01/01", "not-a-date", rand_future_date()])
    elif issue_type == "bad_number":
        return random.choice(["-1", "0", "9999999", "abc", ""])
    elif issue_type == "whitespace":
        return f"  {value}  "   # leading and trailing spaces
    elif issue_type == "wrong_case":
        return value.lower()    # should be upper
    elif issue_type == "placeholder":
        return random.choice(["N/A", "NA", "null", "none", "test", "dummy", "TBD"])
    elif issue_type == "special_char":
        return value + random.choice(["#", "$", "%", "!", "&"])
    return value


# ==============================================================================
# DATASET 1 : employees.csv  (100,000 rows)
# ==============================================================================

def generate_employees(n: int = 100_000) -> None:
    """
    HR employee master data.
    Columns: employee_id, employee_name, email, phone, department,
             salary, join_date, manager_id, status, city
    """
    print(f"  Generating employees.csv  ({n:,} rows)...")

    departments  = ["HR", "IT", "Finance", "Marketing", "Operations", "Legal", "Sales"]
    cities       = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
                    "Dubai", "Singapore", "London", "New York", "Sydney"]
    statuses     = ["ACTIVE", "INACTIVE", "ON_LEAVE"]
    first_names  = ["Priya", "Rahul", "Ankit", "Sunita", "Mohammed", "Sarah",
                    "David", "Chen", "Fatima", "James", "Maria", "Ravi"]
    last_names   = ["Kumar", "Sharma", "Patel", "Reddy", "Khan", "Smith",
                    "Johnson", "Lee", "Ali", "Williams", "Singh", "Nair"]

    filepath = os.path.join(OUT_DIR, "employees.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "employee_id", "employee_name", "email", "phone",
            "department", "salary", "join_date", "manager_id", "status", "city"
        ])
        used_ids = set()
        for i in range(1, n + 1):
            # Generate base values
            emp_id  = inject_issues(str(i), "duplicate_id", pct=0.01)
            name    = f"{random.choice(first_names)} {random.choice(last_names)}"
            email   = inject_issues(rand_email(name), "bad_email",    pct=0.03)
            email   = inject_issues(email,            "null",         pct=0.02)
            phone   = inject_issues(
                          str(random.randint(7000000000, 9999999999)),
                          "bad_number", pct=0.03)
            dept    = inject_issues(random.choice(departments), "wrong_case", pct=0.04)
            dept    = inject_issues(dept,                       "null",       pct=0.02)
            salary  = inject_issues(
                          str(round(random.uniform(30000, 250000), 2)),
                          "bad_number", pct=0.03)
            jdate   = inject_issues(rand_date(2010, 2024), "bad_date",   pct=0.02)
            mgr_id  = str(random.randint(1, 500)) if random.random() > 0.05 else ""
            status  = random.choice(statuses)
            city    = inject_issues(random.choice(cities), "whitespace", pct=0.03)

            writer.writerow([
                emp_id, name, email, phone, dept,
                salary, jdate, mgr_id, status, city
            ])

    print(f"    ✓ Saved: {filepath}")


# ==============================================================================
# DATASET 2 : orders.csv  (200,000 rows)
# ==============================================================================

def generate_orders(n: int = 200_000) -> None:
    """
    E-commerce order data.
    Columns: order_id, customer_id, product_id, quantity, unit_price,
             total_amount, order_date, delivery_date, status, channel
    """
    print(f"  Generating orders.csv  ({n:,} rows)...")

    statuses  = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
    channels  = ["ONLINE", "STORE", "MOBILE", "PARTNER"]

    filepath = os.path.join(OUT_DIR, "orders.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "customer_id", "product_id", "quantity",
            "unit_price", "total_amount", "order_date",
            "delivery_date", "status", "channel"
        ])
        for i in range(1, n + 1):
            order_id   = inject_issues(str(i), "duplicate_id", pct=0.008)
            cust_id    = random.randint(1, 150_000)
            prod_id    = random.randint(1, 50_000)
            qty        = inject_issues(str(random.randint(1, 50)), "bad_number", pct=0.02)
            unit_price = round(random.uniform(5.0, 5000.0), 2)
            total      = inject_issues(
                             str(round(unit_price * int(qty) if qty.isdigit() else 0, 2)),
                             "bad_number", pct=0.02)
            odate      = rand_date(2022, 2024)
            ddate      = inject_issues(rand_date(2022, 2025), "bad_date",  pct=0.02)
            ddate      = inject_issues(ddate,                 "null",      pct=0.03)
            status     = inject_issues(random.choice(statuses), "placeholder", pct=0.02)
            channel    = inject_issues(random.choice(channels),  "null",       pct=0.01)

            writer.writerow([
                order_id, cust_id, prod_id, qty,
                unit_price, total, odate, ddate, status, channel
            ])

    print(f"    ✓ Saved: {filepath}")


# ==============================================================================
# DATASET 3 : products.csv  (50,000 rows)
# ==============================================================================

def generate_products(n: int = 50_000) -> None:
    """
    Product catalog data.
    Columns: product_id, sku, product_name, category, sub_category,
             price, cost_price, stock_qty, supplier_id, is_active
    """
    print(f"  Generating products.csv  ({n:,} rows)...")

    categories = {
        "Electronics"  : ["Laptops", "Phones", "Tablets", "Accessories"],
        "Clothing"     : ["Men", "Women", "Kids", "Sportswear"],
        "Furniture"    : ["Office", "Bedroom", "Living Room", "Outdoor"],
        "Food"         : ["Snacks", "Beverages", "Dairy", "Frozen"],
        "Books"        : ["Fiction", "Technical", "Educational", "Comics"],
    }
    adjectives = ["Premium", "Pro", "Ultra", "Classic", "Smart", "Eco", "Max"]
    nouns      = ["Series", "Edition", "Pack", "Bundle", "Kit", "Set", "Plus"]

    filepath = os.path.join(OUT_DIR, "products.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "product_id", "sku", "product_name", "category",
            "sub_category", "price", "cost_price", "stock_qty",
            "supplier_id", "is_active"
        ])
        for i in range(1, n + 1):
            prod_id  = inject_issues(str(i), "duplicate_id", pct=0.005)
            # SKU format: CAT-XXXXX  e.g. ELC-00042
            cat_code = "".join(random.choices(string.ascii_uppercase, k=3))
            sku      = inject_issues(
                           f"{cat_code}-{i:05d}",
                           "special_char", pct=0.02)
            cat      = random.choice(list(categories.keys()))
            sub_cat  = random.choice(categories[cat])
            pname    = f"{random.choice(adjectives)} {sub_cat} {random.choice(nouns)} {i}"
            pname    = inject_issues(pname, "null",        pct=0.01)
            price    = round(random.uniform(1.0, 10000.0), 2)
            cost     = inject_issues(
                           str(round(price * random.uniform(0.3, 0.8), 2)),
                           "bad_number", pct=0.02)
            stock    = inject_issues(str(random.randint(0, 10000)), "bad_number", pct=0.02)
            supplier = str(random.randint(1, 500))
            active   = inject_issues(
                           random.choice(["true", "false"]),
                           "placeholder", pct=0.02)

            writer.writerow([
                prod_id, sku, pname, cat, sub_cat,
                price, cost, stock, supplier, active
            ])

    print(f"    ✓ Saved: {filepath}")


# ==============================================================================
# DATASET 4 : transactions.csv  (500,000 rows)
# ==============================================================================

def generate_transactions(n: int = 500_000) -> None:
    """
    Financial transaction data.
    Columns: txn_id, account_id, txn_type, amount, currency,
             txn_date, value_date, status, reference_no, description
    """
    print(f"  Generating transactions.csv  ({n:,} rows)...")

    txn_types  = ["CREDIT", "DEBIT", "TRANSFER", "REFUND", "REVERSAL"]
    currencies = ["USD", "EUR", "GBP", "INR", "AED", "SGD"]
    statuses   = ["SUCCESS", "FAILED", "PENDING", "REVERSED"]
    descs      = ["Salary", "Rent", "Grocery", "Transfer", "EMI",
                  "Insurance", "Subscription", "Dividend", "Refund"]

    filepath = os.path.join(OUT_DIR, "transactions.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "txn_id", "account_id", "txn_type", "amount",
            "currency", "txn_date", "value_date",
            "status", "reference_no", "description"
        ])
        for i in range(1, n + 1):
            txn_id   = inject_issues(str(i), "duplicate_id", pct=0.005)
            acct_id  = random.randint(10000, 999999)
            txn_type = inject_issues(random.choice(txn_types), "null",        pct=0.01)
            amount   = inject_issues(
                           str(round(random.uniform(0.01, 1_000_000.0), 4)),
                           "bad_number", pct=0.02)
            currency = inject_issues(random.choice(currencies), "placeholder", pct=0.01)
            tdate    = inject_issues(rand_date(2020, 2024), "bad_date",   pct=0.02)
            vdate    = rand_date(2020, 2024)
            status   = inject_issues(random.choice(statuses), "null",        pct=0.01)
            ref_no   = inject_issues(
                           f"REF{i:010d}",
                           "special_char", pct=0.02)
            desc     = inject_issues(random.choice(descs), "whitespace", pct=0.03)

            writer.writerow([
                txn_id, acct_id, txn_type, amount,
                currency, tdate, vdate, status, ref_no, desc
            ])

    print(f"    ✓ Saved: {filepath}")


# ==============================================================================
# DATASET 5 : customers.csv  (150,000 rows)
# ==============================================================================

def generate_customers(n: int = 150_000) -> None:
    """
    Customer profile data.
    Columns: customer_id, full_name, email, phone, city, country,
             registration_date, tier, credit_score, is_verified
    """
    print(f"  Generating customers.csv  ({n:,} rows)...")

    countries = ["India", "UAE", "Singapore", "UK", "USA", "Australia", "Germany"]
    cities_by = {
        "India"     : ["Mumbai", "Delhi", "Bangalore", "Hyderabad"],
        "UAE"       : ["Dubai", "Abu Dhabi", "Sharjah"],
        "Singapore" : ["Singapore"],
        "UK"        : ["London", "Manchester", "Birmingham"],
        "USA"       : ["New York", "Chicago", "Los Angeles"],
        "Australia" : ["Sydney", "Melbourne", "Brisbane"],
        "Germany"   : ["Berlin", "Munich", "Hamburg"],
    }
    tiers      = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    first_names= ["Alice", "Bob", "Carlos", "Diana", "Elisa", "Frank",
                  "Grace", "Henry", "Irene", "Jack", "Karen", "Leo"]
    last_names = ["Wang", "Patel", "Santos", "Müller", "Brown", "Kim",
                  "Ahmed", "Russo", "Tanaka", "Okafor", "Novak", "Green"]

    filepath = os.path.join(OUT_DIR, "customers.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "full_name", "email", "phone",
            "city", "country", "registration_date",
            "tier", "credit_score", "is_verified"
        ])
        for i in range(1, n + 1):
            cust_id  = inject_issues(str(i), "duplicate_id", pct=0.006)
            fname    = f"{random.choice(first_names)} {random.choice(last_names)}"
            email    = inject_issues(rand_email(fname), "bad_email",   pct=0.04)
            email    = inject_issues(email,             "null",        pct=0.02)
            phone    = inject_issues(
                           str(random.randint(1000000000, 9999999999)),
                           "bad_number", pct=0.03)
            country  = random.choice(countries)
            city     = inject_issues(
                           random.choice(cities_by[country]),
                           "whitespace", pct=0.03)
            reg_date = inject_issues(rand_date(2015, 2024), "bad_date",   pct=0.02)
            tier     = inject_issues(random.choice(tiers),  "wrong_case", pct=0.04)
            tier     = inject_issues(tier,                  "null",       pct=0.02)
            credit   = inject_issues(
                           str(random.randint(300, 900)),
                           "bad_number", pct=0.02)
            verified = inject_issues(
                           random.choice(["true", "false"]),
                           "placeholder", pct=0.02)

            writer.writerow([
                cust_id, fname, email, phone, city, country,
                reg_date, tier, credit, verified
            ])

    print(f"    ✓ Saved: {filepath}")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  SAMPLE DATA GENERATOR  —  ETL DQ Framework v6")
    print("=" * 60)
    print(f"\n  Output folder: {OUT_DIR}\n")

    generate_employees(100_000)
    generate_orders(200_000)
    generate_products(50_000)
    generate_transactions(500_000)
    generate_customers(150_000)

    print("\n" + "=" * 60)
    print("  ALL 5 DATASETS GENERATED SUCCESSFULLY")
    print("=" * 60)
    print("""
  Files created:
    data/employees.csv      100,000 rows
    data/orders.csv         200,000 rows
    data/products.csv        50,000 rows
    data/transactions.csv   500,000 rows
    data/customers.csv      150,000 rows

  Total: 1,000,000 rows across 5 datasets
  Each has ~5-8%% intentional DQ issues for testing.
    """)
