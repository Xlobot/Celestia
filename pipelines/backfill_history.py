"""
Backfill script — generates historical baseline data for the last N days.
Run this ONCE to populate the database with enough history for the
monitoring engine to build meaningful baselines.

We generate "normal" data for past days, then run the pipeline for each.
The fault-scenario days (Nov 6-12) already exist and stay as-is.
"""

import sys
from pathlib import Path
from datetime import date, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data.generator import generate_data
from pipelines.run_pipeline import run_pipeline

# ── Config ────────────────────────────────────────────────────
BACKFILL_DAYS = 21          # how many past days to generate
START_FROM    = date(2026, 11, 5)   # go backwards from here (before our fault days)

def backfill(days: int, end_date: date):
    print(f"\nBackfilling {days} days of normal historical data...")
    print("=" * 60)

    for i in range(days):
        run_date = end_date - timedelta(days=i + 1)

        # Skip dates we already have data for (Nov 6-12 are fault scenarios)
        date_str   = run_date.strftime("%Y%m%d")
        cust_file  = PROJECT_ROOT / "data" / "raw" / f"raw_customers_{date_str}.csv"

        if cust_file.exists():
            print(f"  Skipping {run_date} — already exists")
            continue

        # Use a different seed per day so data isn't identical
        seed = 100 + i

        # Generate normal data
        generate_data(scenario="normal", run_date=run_date, seed=seed)

        # Run the pipeline (logs to DB)
        run_pipeline(run_date)

    print("\nBackfill complete.")

if __name__ == "__main__":
    backfill(BACKFILL_DAYS, START_FROM)

