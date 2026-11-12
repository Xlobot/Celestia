"""
Data Generator for Pipeline RCA System
=======================================
Generates synthetic raw data that simulates a real e-commerce pipeline.

Usage:
    python data/generator.py                        # normal data for today
    python data/generator.py --scenario row_drop    # 80% row count drop
    python data/generator.py --scenario schema_drift
    python data/generator.py --scenario null_spike
    python data/generator.py --scenario missing_file
    python data/generator.py --date 2026-11-10 --scenario normal

Scenarios:
    normal       - Clean data, realistic volumes with weekday/weekend variation
    row_drop     - Only 20% of expected rows (simulates partial ingestion failure)
    schema_drift - customer_id changes from int to string (e.g. "CUST_1234")
    null_spike   - Revenue column has 60% nulls (simulates upstream ETL bug)
    missing_file - Orders file is not generated at all (simulates source failure)
    duplicate    - 30% of customer rows are duplicated (simulates double-load bug)
"""

import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime, date


# ─────────────────────────────────────────────
# SECTION 1: CONFIGURATION
# ─────────────────────────────────────────────
# These are your "normal" baseline numbers.
# The monitoring system will learn what's normal from these.

BASE_CUSTOMER_COUNT = 10_000   # expected rows on a normal weekday
ORDERS_PER_CUSTOMER = 5        # average orders each customer has

# Weekend naturally has fewer records - this prevents false alerts
WEEKDAY_MULTIPLIER = 1.0
WEEKEND_MULTIPLIER = 0.6       # Saturday/Sunday = 60% of normal volume

# Output directory
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "raw")


# ─────────────────────────────────────────────
# SECTION 2: CUSTOMER GENERATOR
# ─────────────────────────────────────────────

def generate_customers(n_records: int, run_date: date, seed: int = 42) -> pd.DataFrame:
    """
    Generate a DataFrame of raw customer records.

    Parameters:
        n_records  : how many customer rows to create
        run_date   : the date this pipeline "ran" (used for signup_date logic)
        seed       : random seed for reproducibility

    Returns:
        pd.DataFrame with columns: customer_id, name, email, country,
                                   signup_date, is_active, age
    """
    rng = np.random.default_rng(seed)  # modern numpy random generator

    # Generate sequential customer IDs - simple integers
    customer_ids = np.arange(1, n_records + 1)

    # Random first + last names (simplified pool for simulation)
    first_names = ["Alice", "Bob", "Carlos", "Diana", "Eve", "Frank",
                   "Grace", "Henry", "Isabel", "James", "Karen", "Leo"]
    last_names  = ["Smith", "Johnson", "Williams", "Brown", "Jones",
                   "Garcia", "Miller", "Davis", "Wilson", "Moore"]

    names = [
        f"{rng.choice(first_names)} {rng.choice(last_names)}"
        for _ in range(n_records)
    ]

    # Email derived from name (realistic pattern)
    emails = [
        f"{name.lower().replace(' ', '.')}{rng.integers(1, 999)}@example.com"
        for name in names
    ]

    countries = rng.choice(
        ["US", "UK", "IN", "DE", "FR", "CA", "AU"],
        size=n_records,
        p=[0.35, 0.15, 0.20, 0.10, 0.08, 0.07, 0.05]  # realistic distribution
    )

    # Signup dates spread over past 3 years
    days_offset = rng.integers(0, 365 * 3, size=n_records)
    signup_dates = [
        (pd.Timestamp(run_date) - pd.Timedelta(days=int(d))).date()
        for d in days_offset
    ]

    # 90% of customers are active - realistic churn
    is_active = rng.choice([True, False], size=n_records, p=[0.90, 0.10])

    ages = rng.integers(18, 75, size=n_records)

    df = pd.DataFrame({
        "customer_id": customer_ids,          # integer - important for schema_drift scenario
        "name":        names,
        "email":       emails,
        "country":     countries,
        "signup_date": signup_dates,
        "is_active":   is_active,
        "age":         ages
    })

    return df


# ─────────────────────────────────────────────
# SECTION 3: ORDERS GENERATOR
# ─────────────────────────────────────────────

def generate_orders(customers_df: pd.DataFrame, run_date: date, seed: int = 42) -> pd.DataFrame:
    """
    Generate order records linked to actual customer IDs.

    Why linked? Because referential integrity matters - an order with a
    customer_id that doesn't exist in customers is a data quality problem.
    Our monitoring system should catch that if it happens.

    Parameters:
        customers_df : the customer DataFrame (we use its customer_ids)
        run_date     : the date this pipeline "ran"
        seed         : random seed

    Returns:
        pd.DataFrame with columns: order_id, customer_id, product_id,
                                   product_name, quantity, unit_price,
                                   revenue, order_date, status
    """
    rng = np.random.default_rng(seed + 1)  # +1 so orders get different randomness than customers

    # Only active customers place orders
    active_customers = customers_df[customers_df["is_active"] == True]["customer_id"].values

    # Each active customer places 1-10 orders (avg ~5)
    orders_per_customer = rng.integers(1, 10, size=len(active_customers))
    total_orders = int(orders_per_customer.sum())

    # Repeat customer IDs based on how many orders they have
    customer_ids = np.repeat(active_customers, orders_per_customer)

    products = {
        "Laptop":    1200.00,
        "Phone":      800.00,
        "Headphones": 150.00,
        "Tablet":     500.00,
        "Monitor":    300.00,
        "Keyboard":    80.00,
        "Mouse":       40.00,
    }
    product_names = list(products.keys())
    product_prices = list(products.values())

    chosen_products = rng.choice(product_names, size=total_orders)
    unit_prices = np.array([products[p] for p in chosen_products])

    # Add slight price variation (+/- 5%) to simulate discount/surge pricing
    price_variation = rng.uniform(0.95, 1.05, size=total_orders)
    unit_prices = np.round(unit_prices * price_variation, 2)

    quantities = rng.integers(1, 5, size=total_orders)
    revenues   = np.round(unit_prices * quantities, 2)

    statuses = rng.choice(
        ["completed", "pending", "refunded", "failed"],
        size=total_orders,
        p=[0.75, 0.15, 0.07, 0.03]
    )

    # Order timestamps throughout the run_date
    hour_offsets = rng.integers(0, 86400, size=total_orders)  # seconds in a day
    order_dates = [
        pd.Timestamp(run_date) + pd.Timedelta(seconds=int(s))
        for s in hour_offsets
    ]

    df = pd.DataFrame({
        "order_id":    np.arange(1, total_orders + 1),
        "customer_id": customer_ids,
        "product_id":  rng.integers(100, 200, size=total_orders),
        "product_name": chosen_products,
        "quantity":    quantities,
        "unit_price":  unit_prices,
        "revenue":     revenues,
        "order_date":  order_dates,
        "status":      statuses
    })

    return df


# ─────────────────────────────────────────────
# SECTION 4: ANOMALY INJECTOR
# ─────────────────────────────────────────────

def apply_scenario(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame | None,
    scenario: str,
    rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """
    Inject a controlled fault into the data.

    This is what makes the generator useful for testing:
    you can reproduce any failure on demand.

    Returns the modified (customers_df, orders_df) tuple.
    """

    if scenario == "normal":
        # No changes - clean data
        return customers_df, orders_df

    elif scenario == "row_drop":
        # Simulate a partial ingestion failure - only 20% of rows loaded
        # Real world: source system timed out mid-export
        keep_frac = 0.20
        customers_df = customers_df.sample(frac=keep_frac, random_state=42)
        if orders_df is not None:
            # Only keep orders for customers that survived the drop
            surviving_ids = set(customers_df["customer_id"])
            orders_df = orders_df[orders_df["customer_id"].isin(surviving_ids)]
        print(f"  [row_drop] Kept {len(customers_df)} customers (expected ~{BASE_CUSTOMER_COUNT})")

    elif scenario == "schema_drift":
        # Simulate a schema change: customer_id goes from int to string
        # Real world: upstream team changed their export format
        customers_df["customer_id"] = "CUST_" + customers_df["customer_id"].astype(str)
        if orders_df is not None:
            orders_df["customer_id"] = "CUST_" + orders_df["customer_id"].astype(str)
        print(f"  [schema_drift] customer_id changed from int to string (e.g. 'CUST_1234')")

    elif scenario == "null_spike":
        # Simulate 60% of revenue values going null
        # Real world: currency conversion service was down during ETL
        null_mask = rng.random(size=len(orders_df)) < 0.60
        orders_df = orders_df.copy()
        orders_df.loc[null_mask, "revenue"] = np.nan
        null_count = null_mask.sum()
        print(f"  [null_spike] {null_count} revenue values set to null ({null_count/len(orders_df)*100:.1f}%)")

    elif scenario == "duplicate":
        # Simulate a double-load bug - 30% of customer rows duplicated
        # Real world: pipeline retried after a false failure signal
        duplicate_frac = 0.30
        dupes = customers_df.sample(frac=duplicate_frac, random_state=42)
        customers_df = pd.concat([customers_df, dupes], ignore_index=True)
        print(f"  [duplicate] Added {len(dupes)} duplicate rows ({len(customers_df)} total)")

    elif scenario == "missing_file":
        # Orders file won't be written at all
        # Real world: source API was unreachable
        print(f"  [missing_file] Orders file will NOT be generated (simulates source failure)")
        orders_df = None

    else:
        raise ValueError(f"Unknown scenario: '{scenario}'. "
                         f"Valid options: normal, row_drop, schema_drift, null_spike, duplicate, missing_file")

    return customers_df, orders_df


# ─────────────────────────────────────────────
# SECTION 5: MAIN ENTRY POINT
# ─────────────────────────────────────────────

def generate_data(
    scenario: str = "normal",
    run_date: date | None = None,
    seed: int = 42,
    output_dir: str = RAW_DATA_DIR
) -> dict:
    """
    Generate and save a full dataset for a given pipeline run date.

    Parameters:
        scenario   : which fault to inject (default: 'normal' = clean data)
        run_date   : the date to simulate (default: today)
        seed       : random seed for reproducibility
        output_dir : where to save the CSV files

    Returns:
        dict with metadata about what was generated (used by the pipeline jobs later)
    """
    if run_date is None:
        run_date = date.today()

    rng = np.random.default_rng(seed)

    # Weekend naturally produces fewer records - important!
    # Without this, every Monday would trigger a false "row drop" alert
    day_of_week = pd.Timestamp(run_date).day_of_week  # 0=Monday, 6=Sunday
    is_weekend = day_of_week >= 5
    multiplier = WEEKEND_MULTIPLIER if is_weekend else WEEKDAY_MULTIPLIER

    # Add small daily noise (+/- 5%) to simulate realistic variation
    noise = rng.uniform(0.95, 1.05)
    n_customers = int(BASE_CUSTOMER_COUNT * multiplier * noise)

    date_str = run_date.strftime("%Y%m%d")
    print(f"\n{'='*50}")
    print(f"Generating data for: {run_date} ({'weekend' if is_weekend else 'weekday'})")
    print(f"Scenario: {scenario}")
    print(f"Expected customers: {n_customers:,}")
    print(f"{'='*50}")

    # Generate base data
    customers_df = generate_customers(n_customers, run_date, seed)
    orders_df    = generate_orders(customers_df, run_date, seed)

    # Apply the fault scenario
    customers_df, orders_df = apply_scenario(customers_df, orders_df, scenario, rng)

    # Save to CSV files with date in filename
    os.makedirs(output_dir, exist_ok=True)

    customers_path = os.path.join(output_dir, f"raw_customers_{date_str}.csv")
    customers_df.to_csv(customers_path, index=False)
    print(f"\n  Saved: raw_customers_{date_str}.csv  ({len(customers_df):,} rows)")

    orders_path = None
    if orders_df is not None:
        orders_path = os.path.join(output_dir, f"raw_orders_{date_str}.csv")
        orders_df.to_csv(orders_path, index=False)
        print(f"  Saved: raw_orders_{date_str}.csv     ({len(orders_df):,} rows)")
    else:
        print(f"  Skipped: raw_orders_{date_str}.csv  (missing_file scenario)")

    # Return metadata - this will feed into the pipeline execution metadata layer
    return {
        "run_date":        str(run_date),
        "scenario":        scenario,
        "seed":            seed,
        "is_weekend":      is_weekend,
        "customers_count": len(customers_df),
        "orders_count":    len(orders_df) if orders_df is not None else 0,
        "customers_path":  customers_path,
        "orders_path":     orders_path,
    }


# ─────────────────────────────────────────────
# SECTION 6: CLI INTERFACE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic pipeline data")
    parser.add_argument(
        "--scenario",
        default="normal",
        choices=["normal", "row_drop", "schema_drift", "null_spike", "duplicate", "missing_file"],
        help="Which fault scenario to inject (default: normal)"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Run date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )

    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()

    metadata = generate_data(
        scenario=args.scenario,
        run_date=run_date,
        seed=args.seed
    )

    print(f"\nMetadata: {metadata}")

