"""
Pipeline Job Simulator
======================
Simulates the three pipeline jobs for a given run date.
Each job reads from data/raw/, processes data, and writes
execution metadata to PostgreSQL (job_execution_logs table).

Jobs in order:
  Job A: job_ingest_customers   → reads raw_customers CSV
  Job B: job_ingest_orders      → reads raw_orders CSV
  Job C: job_build_sales_summary → joins customers + orders

Usage:
    python pipelines/run_pipeline.py --date 2026-11-12
    python pipelines/run_pipeline.py --date 2026-11-11   ← row_drop scenario
"""

import pandas as pd
import numpy as np
import os
import sys
import json
import hashlib
import argparse
import time
from datetime import datetime, date, timezone
from pathlib import Path

# ── Make project root importable ─────────────────────────────
# This lets us do "from db.connection import ..." from anywhere
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import get_connection, execute_returning, fetch_all

RAW_DIR       = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
LOGS_DIR      = PROJECT_ROOT / "logs"

# Ensure output directories exist
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────

def schema_fingerprint(df: pd.DataFrame) -> str:
    """
    Compute an MD5 hash of a DataFrame's column names + dtypes.

    Why? If the fingerprint changes between runs, it means the
    schema changed (columns added/removed/renamed/type-changed).
    Much faster than comparing full schemas row by row.

    Example:
        Day 1:  customer_id=int64, name=object  → fingerprint: "a3f9..."
        Day 2:  customer_id=object, name=object → fingerprint: "7bc1..."  ← DRIFT!
    """
    schema_str = "|".join(
        f"{col}:{dtype}" for col, dtype in sorted(df.dtypes.items())
    )
    return hashlib.md5(schema_str.encode()).hexdigest()


def compute_null_rates(df: pd.DataFrame) -> dict:
    """
    Returns the null rate (0.0 to 1.0) for each column.
    e.g. {"revenue": 0.596, "customer_id": 0.0}
    """
    return {col: round(df[col].isnull().mean(), 4) for col in df.columns}


def log_start(job_id: str, run_date: date) -> int:
    """
    Insert a 'running' row into job_execution_logs.
    Returns the execution_id (auto-generated primary key).

    Why log the start? So we can detect jobs that crashed mid-run
    (they'll stay in 'running' status forever — that itself is an anomaly).
    """
    execution_id = execute_returning(
        """
        INSERT INTO job_execution_logs
            (job_id, run_date, started_at, status)
        VALUES
            (%s, %s, %s, 'running')
        RETURNING execution_id
        """,
        (job_id, run_date, datetime.now(timezone.utc))
    )
    return execution_id


def log_finish(
    execution_id: int,
    status: str,
    rows_read: int,
    rows_written: int,
    schema_fp: str = None,
    input_row_counts: dict = None,
    output_row_counts: dict = None,
    error_message: str = None,
    error_type: str = None,
    log_reference: str = None,
):
    """
    Update the existing 'running' log row with final results.
    Sets ended_at (duration is auto-calculated by Postgres).
    """
    execute_returning(
        """
        UPDATE job_execution_logs SET
            ended_at          = %s,
            status            = %s,
            rows_read         = %s,
            rows_written      = %s,
            schema_fingerprint = %s,
            input_row_counts  = %s,
            output_row_counts = %s,
            error_message     = %s,
            error_type        = %s,
            log_reference     = %s
        WHERE execution_id = %s
        RETURNING execution_id
        """,
        (
            datetime.now(timezone.utc),
            status,
            rows_read,
            rows_written,
            schema_fp,
            json.dumps(input_row_counts or {}),
            json.dumps(output_row_counts or {}),
            error_message,
            error_type,
            log_reference,
            execution_id,
        )
    )


def save_dataset_snapshot(
    dataset_name: str,
    snapshot_date: date,
    df: pd.DataFrame,
    file_path: str,
    execution_id: int,
):
    """
    Write a daily health snapshot of a dataset to dataset_snapshots.
    Used later by the monitoring layer to detect anomalies.
    """
    is_weekend = pd.Timestamp(snapshot_date).day_of_week >= 5
    null_rates = compute_null_rates(df)
    schema_snap = {col: str(dtype) for col, dtype in df.dtypes.items()}
    fp = schema_fingerprint(df)
    file_size_kb = os.path.getsize(file_path) / 1024 if os.path.exists(file_path) else 0

    execute_returning(
        """
        INSERT INTO dataset_snapshots
            (dataset_name, snapshot_date, row_count, is_weekend,
             null_rates, schema_fingerprint, schema_snapshot,
             file_path, file_size_kb, execution_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_name, snapshot_date) DO UPDATE SET
            row_count          = EXCLUDED.row_count,
            null_rates         = EXCLUDED.null_rates,
            schema_fingerprint = EXCLUDED.schema_fingerprint,
            schema_snapshot    = EXCLUDED.schema_snapshot,
            file_size_kb       = EXCLUDED.file_size_kb
        RETURNING snapshot_id
        """,
        (
            dataset_name,
            snapshot_date,
            len(df),
            is_weekend,
            json.dumps(null_rates),
            fp,
            json.dumps(schema_snap),
            str(file_path),
            round(file_size_kb, 2),
            execution_id,
        )
    )


def print_separator(char="-", width=60):
    print(char * width)


# ─────────────────────────────────────────────────────────────
# JOB A: Ingest Customers
# ─────────────────────────────────────────────────────────────

def run_job_ingest_customers(run_date: date) -> tuple[str, pd.DataFrame | None]:
    """
    Reads raw_customers CSV, validates it, saves to processed/,
    logs execution metadata to PostgreSQL.

    Returns: (status, dataframe_or_None)
    """
    job_id  = "job_ingest_customers"
    date_str = run_date.strftime("%Y%m%d")
    raw_path = RAW_DIR / f"raw_customers_{date_str}.csv"
    out_path = PROCESSED_DIR / f"clean_customers_{date_str}.csv"

    print(f"\n[Job A] {job_id}")
    print_separator()

    # Log job start (status = 'running')
    exec_id = log_start(job_id, run_date)

    try:
        # ── Check file exists ─────────────────────────────────
        if not raw_path.exists():
            raise FileNotFoundError(f"Source file not found: {raw_path}")

        # ── Read raw data ─────────────────────────────────────
        df = pd.read_csv(raw_path)
        rows_read = len(df)
        print(f"  Read:  {rows_read:,} rows from {raw_path.name}")

        # ── Basic validation ──────────────────────────────────
        required_cols = {"customer_id", "name", "email", "country", "is_active"}
        missing_cols  = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # ── Compute schema fingerprint before writing ─────────
        fp = schema_fingerprint(df)

        # ── Write clean file ──────────────────────────────────
        df.to_csv(out_path, index=False)
        rows_written = len(df)
        print(f"  Wrote: {rows_written:,} rows to {out_path.name}")
        print(f"  Schema fingerprint: {fp[:12]}...")

        # ── Save dataset snapshot ─────────────────────────────
        save_dataset_snapshot("raw.customers", run_date, df, raw_path, exec_id)
        save_dataset_snapshot("clean.customers", run_date, df, out_path, exec_id)

        # ── Log success ───────────────────────────────────────
        status = "success"
        log_finish(
            exec_id, status,
            rows_read=rows_read,
            rows_written=rows_written,
            schema_fp=fp,
            input_row_counts={"raw.customers": rows_read},
            output_row_counts={"clean.customers": rows_written},
            log_reference=str(out_path),
        )
        print(f"  Status: {status}")
        return status, df

    except FileNotFoundError as e:
        print(f"  [ERROR] {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type="FileNotFoundError")
        return "failed", None

    except ValueError as e:
        print(f"  [ERROR] {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type="SchemaValidationError")
        return "failed", None

    except Exception as e:
        print(f"  [ERROR] Unexpected: {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type=type(e).__name__)
        return "failed", None


# ─────────────────────────────────────────────────────────────
# JOB B: Ingest Orders
# ─────────────────────────────────────────────────────────────

def run_job_ingest_orders(run_date: date) -> tuple[str, pd.DataFrame | None]:
    """
    Reads raw_orders CSV, validates it, saves to processed/.
    If the file is missing → 'failed' status (not a crash — expected failure mode).
    """
    job_id   = "job_ingest_orders"
    date_str = run_date.strftime("%Y%m%d")
    raw_path = RAW_DIR / f"raw_orders_{date_str}.csv"
    out_path = PROCESSED_DIR / f"clean_orders_{date_str}.csv"

    print(f"\n[Job B] {job_id}")
    print_separator()

    exec_id = log_start(job_id, run_date)

    try:
        if not raw_path.exists():
            raise FileNotFoundError(f"Source file not found: {raw_path.name}")

        df = pd.read_csv(raw_path)
        rows_read = len(df)
        print(f"  Read:  {rows_read:,} rows from {raw_path.name}")

        # Validate required columns
        required_cols = {"order_id", "customer_id", "revenue", "order_date"}
        missing_cols  = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        fp = schema_fingerprint(df)

        # Check null rate in revenue — log warning if high
        revenue_null_pct = df["revenue"].isnull().mean() * 100
        if revenue_null_pct > 10:
            status = "partial_load_warning"
            print(f"  [WARNING] Revenue null rate is {revenue_null_pct:.1f}% (threshold: 10%)")
        else:
            status = "success"

        df.to_csv(out_path, index=False)
        rows_written = len(df)
        print(f"  Wrote: {rows_written:,} rows to {out_path.name}")
        print(f"  Schema fingerprint: {fp[:12]}...")

        save_dataset_snapshot("raw.orders", run_date, df, raw_path, exec_id)
        save_dataset_snapshot("clean.orders", run_date, df, out_path, exec_id)

        log_finish(
            exec_id, status,
            rows_read=rows_read,
            rows_written=rows_written,
            schema_fp=fp,
            input_row_counts={"raw.orders": rows_read},
            output_row_counts={"clean.orders": rows_written},
            log_reference=str(out_path),
        )
        print(f"  Status: {status}")
        return status, df

    except FileNotFoundError as e:
        print(f"  [ERROR] {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type="FileNotFoundError")
        return "failed", None

    except Exception as e:
        print(f"  [ERROR] Unexpected: {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type=type(e).__name__)
        return "failed", None


# ─────────────────────────────────────────────────────────────
# JOB C: Build Sales Summary
# ─────────────────────────────────────────────────────────────

def run_job_build_sales_summary(
    run_date: date,
    customers_df: pd.DataFrame | None,
    orders_df: pd.DataFrame | None,
) -> str:
    """
    Joins clean customers + clean orders to build the analytics summary.

    This job DEPENDS on Job A and Job B succeeding.
    If either upstream job failed, this job is SKIPPED.
    That dependency is exactly what the graph layer will model.
    """
    job_id   = "job_build_sales_summary"
    date_str = run_date.strftime("%Y%m%d")
    out_path = PROCESSED_DIR / f"customer_sales_summary_{date_str}.csv"

    print(f"\n[Job C] {job_id}")
    print_separator()

    exec_id = log_start(job_id, run_date)

    # ── Dependency check ──────────────────────────────────────
    # If upstream data is missing, skip this job entirely
    if customers_df is None or orders_df is None:
        reason = []
        if customers_df is None: reason.append("clean.customers unavailable")
        if orders_df is None:    reason.append("clean.orders unavailable")
        msg = f"Skipped: upstream dependency failed — {', '.join(reason)}"
        print(f"  [SKIPPED] {msg}")
        log_finish(exec_id, "skipped", 0, 0,
                   error_message=msg, error_type="DependencyFailure")
        return "skipped"

    try:
        rows_read_cust   = len(customers_df)
        rows_read_orders = len(orders_df)

        # ── The actual transformation (simplified join + aggregation) ──
        # In a real pipeline this would be a SQL query on a warehouse.
        # Here we simulate it with pandas.

        # Only use completed orders for revenue
        completed = orders_df[orders_df["status"] == "completed"].copy()

        # Handle nulls in revenue before aggregation
        completed = completed.dropna(subset=["revenue"])

        summary = (
            completed
            .groupby("customer_id")
            .agg(
                total_orders   = ("order_id", "count"),
                total_revenue  = ("revenue", "sum"),
                avg_order_value= ("revenue", "mean"),
                last_order_date= ("order_date", "max"),
            )
            .reset_index()
        )

        # Enrich with customer info
        # Note: if schema_drift happened, customer_id types may not match!
        try:
            summary = summary.merge(
                customers_df[["customer_id", "name", "country"]],
                on="customer_id", how="left"
            )
        except Exception as merge_err:
            # Schema drift scenario: int vs string customer_id join fails
            raise ValueError(
                f"Join failed — possible schema drift in customer_id: {merge_err}"
            )

        summary["total_revenue"]   = summary["total_revenue"].round(2)
        summary["avg_order_value"] = summary["avg_order_value"].round(2)

        rows_written = len(summary)
        fp = schema_fingerprint(summary)

        summary.to_csv(out_path, index=False)
        print(f"  Joined {rows_read_cust:,} customers + {rows_read_orders:,} orders")
        print(f"  Wrote: {rows_written:,} rows to {out_path.name}")
        print(f"  Schema fingerprint: {fp[:12]}...")

        save_dataset_snapshot(
            "analytics.customer_sales_summary",
            run_date, summary, out_path, exec_id
        )

        log_finish(
            exec_id, "success",
            rows_read=rows_read_cust + rows_read_orders,
            rows_written=rows_written,
            schema_fp=fp,
            input_row_counts={
                "clean.customers": rows_read_cust,
                "clean.orders":    rows_read_orders,
            },
            output_row_counts={"analytics.customer_sales_summary": rows_written},
            log_reference=str(out_path),
        )
        print(f"  Status: success")
        return "success"

    except ValueError as e:
        print(f"  [ERROR] {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type="TransformationError")
        return "failed"

    except Exception as e:
        print(f"  [ERROR] Unexpected: {e}")
        log_finish(exec_id, "failed", 0, 0,
                   error_message=str(e), error_type=type(e).__name__)
        return "failed"


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE RUNNER
# ─────────────────────────────────────────────────────────────

def run_pipeline(run_date: date) -> dict:
    """
    Runs all three jobs in sequence for the given date.
    Returns a summary of what happened.
    """
    print(f"\n{'='*60}")
    print(f"  PIPELINE RUN — {run_date}")
    print(f"{'='*60}")

    t_start = time.time()

    status_a, customers_df = run_job_ingest_customers(run_date)
    status_b, orders_df    = run_job_ingest_orders(run_date)
    status_c               = run_job_build_sales_summary(run_date, customers_df, orders_df)

    elapsed = round(time.time() - t_start, 2)

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE — {elapsed}s")
    print(f"  Job A (ingest_customers):    {status_a}")
    print(f"  Job B (ingest_orders):       {status_b}")
    print(f"  Job C (build_sales_summary): {status_c}")

    all_ok = all(s in ("success", "partial_load_warning")
                 for s in [status_a, status_b, status_c])
    overall = "success" if all_ok else "degraded"
    print(f"  Overall:                     {overall}")
    print(f"{'='*60}")

    return {
        "run_date": str(run_date),
        "job_a": status_a,
        "job_b": status_b,
        "job_c": status_c,
        "overall": overall,
        "elapsed_sec": elapsed,
    }


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline for a given date")
    parser.add_argument("--date", default=None,
                        help="Run date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()
    result   = run_pipeline(run_date)

