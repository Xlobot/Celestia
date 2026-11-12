"""
Database connection manager for the Pipeline RCA system.

Why a separate connection module?
  - One place to change DB credentials / host
  - Every other module just imports `get_connection()` - no repeated config
  - Later we can swap this for environment variables or a secrets manager

Usage:
    from db.connection import get_connection, get_engine

    # For raw psycopg2 (direct SQL):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipeline_jobs")

    # For pandas (read SQL into DataFrame):
    from db.connection import get_engine
    df = pd.read_sql("SELECT * FROM pipeline_jobs", get_engine())
"""

import psycopg2
import psycopg2.extras  # gives us dict-style cursor rows
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd

# ─────────────────────────────────────────────
# CONFIG - change these if your setup differs
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "pipeline_rca",
    "user":     "postgres",
    "password": "123",
}

# SQLAlchemy connection string (used by pandas read_sql / to_sql)
_DB_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)


def get_connection() -> psycopg2.extensions.connection:
    """
    Returns a raw psycopg2 connection.
    Use this when you want full control over SQL execution.

    Always use as a context manager:
        with get_connection() as conn:
            ...
    The context manager auto-commits on success, rolls back on error.
    """
    return psycopg2.connect(**DB_CONFIG)


def get_engine() -> Engine:
    """
    Returns a SQLAlchemy engine.
    Use this with pandas .read_sql() and .to_sql().
    The engine is NOT recreated on every call (pool is reused).
    """
    return create_engine(_DB_URL)


def test_connection() -> bool:
    """
    Quick sanity check - returns True if DB is reachable.
    Run this to confirm setup is working.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result[0] == 1
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


def fetch_all(query: str, params: tuple = None) -> list[dict]:
    """
    Run a SELECT and return all rows as a list of dicts.

    Example:
        rows = fetch_all("SELECT * FROM pipeline_jobs WHERE is_active = %s", (True,))
        print(rows[0]["job_name"])
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def execute(query: str, params: tuple = None) -> None:
    """
    Run an INSERT / UPDATE / DELETE statement.
    Auto-commits on success.

    Example:
        execute(
            "INSERT INTO incidents (run_date, trigger_type, severity) VALUES (%s, %s, %s)",
            ('2026-11-12', 'row_count_anomaly', 'high')
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


def execute_returning(query: str, params: tuple = None):
    """
    Run an INSERT ... RETURNING and get back the generated value.
    Useful for getting the auto-generated primary key after an insert.

    Example:
        incident_id = execute_returning(
            "INSERT INTO incidents (...) VALUES (...) RETURNING incident_id",
            (...)
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
        conn.commit()
    return result[0] if result else None


# ─────────────────────────────────────────────
# Quick self-test when run directly
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing database connection...")

    if test_connection():
        print("[PASS] Connected to pipeline_rca successfully\n")

        # Show all registered pipeline jobs
        jobs = fetch_all("SELECT job_id, job_name, sla_max_duration_sec FROM pipeline_jobs ORDER BY job_id")
        print(f"Registered pipeline jobs ({len(jobs)}):")
        for job in jobs:
            print(f"  - {job['job_id']:35s}  SLA: {job['sla_max_duration_sec']}s")

        # Show table row counts
        print("\nTable row counts:")
        tables = ["pipeline_jobs", "job_execution_logs", "dataset_snapshots", "incidents"]
        for table in tables:
            rows = fetch_all(f"SELECT COUNT(*) AS cnt FROM {table}")
            print(f"  {table:30s}  {rows[0]['cnt']} rows")
    else:
        print("[FAIL] Could not connect. Check DB_CONFIG settings.")

