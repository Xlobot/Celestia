"""
Database connection manager for the Pipeline RCA system.
Loads credentials dynamically from environment variables / .env.
"""

import os
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path

# Load .env file manually if present (avoids strict dependency on python-dotenv)
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    with open(_env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# ─────────────────────────────────────────────
# DYNAMIC CONFIG FROM ENVIRONMENT
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "pipeline_rca"),
    "user":     os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "123"),
}

# SQLAlchemy connection string
_DB_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)


def get_connection() -> psycopg2.extensions.connection:
    """Returns a raw psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_engine() -> Engine:
    """Returns a SQLAlchemy engine."""
    return create_engine(_DB_URL)


def test_connection() -> bool:
    """Quick sanity check - returns True if DB is reachable."""
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
    """Run a SELECT and return all rows as a list of dicts."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def execute(query: str, params: tuple = None) -> None:
    """Run an INSERT / UPDATE / DELETE statement."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


def execute_returning(query: str, params: tuple = None):
    """Run an INSERT ... RETURNING and get back the generated value."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
        conn.commit()
    return result[0] if result else None
