"""
Monitoring Checks — 4 independent checks for data quality.
=============================================================
Each function returns a list of anomaly dicts (empty = no issues).
They are PURE detection functions — they don't write to the DB.
The monitor.py orchestrator handles writing incidents.

Anomaly dict structure:
{
    "check":        "row_count_anomaly",
    "dataset":      "raw.customers",
    "severity":     "high",
    "detail":       "Row count 2055 is below lower fence 9555 (baseline: 10200 median)",
    "trigger_type": "row_count_anomaly",   ← matches incidents.trigger_type CHECK
    "evidence": {
        "today_value": 2055,
        "baseline_median": 10200,
        "lower_fence": 9555,
        "upper_fence": 10875,
    }
}
"""

import json
import numpy as np
import pandas as pd
from datetime import date, timedelta
from typing import Optional

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all


# ─────────────────────────────────────────────────────────────
# SHARED HELPER: load snapshot history
# ─────────────────────────────────────────────────────────────

def _get_snapshot_history(
    dataset_name: str,
    before_date: date,
    lookback_days: int = 30,
    same_day_type: bool = True,
) -> list[dict]:
    """
    Fetch historical snapshots for a dataset.

    Parameters:
        dataset_name   : e.g. 'raw.customers'
        before_date    : only look at dates BEFORE this (exclude today)
        lookback_days  : how many days back to look
        same_day_type  : if True, only return weekdays when checking a weekday
                         (prevents weekend dips from skewing weekday baselines)
    """
    cutoff = before_date - timedelta(days=lookback_days)

    rows = fetch_all(
        """
        SELECT snapshot_date, row_count, null_rates, schema_fingerprint,
               is_weekend, schema_snapshot
        FROM dataset_snapshots
        WHERE dataset_name = %s
          AND snapshot_date < %s
          AND snapshot_date >= %s
        ORDER BY snapshot_date DESC
        """,
        (dataset_name, before_date, cutoff)
    )

    if same_day_type and rows:
        # Is the check date a weekend?
        target_is_weekend = pd.Timestamp(before_date).day_of_week >= 5
        rows = [r for r in rows if r["is_weekend"] == target_is_weekend]

    return rows


def _iqr_bounds(values: list[float], k: float = 1.5) -> tuple[float, float]:
    """
    Compute Tukey fences using IQR.

    IQR = Interquartile Range = Q3 - Q1
    Lower fence = Q1 - k * IQR
    Upper fence = Q3 + k * IQR

    Anything outside these bounds = outlier (anomaly).
    k=1.5 is the standard Tukey value. Use k=3.0 for "extreme" outliers only.

    Why IQR instead of mean/stddev?
      Mean and stddev are pulled toward outliers. If one day had a massive
      spike, it inflates the stddev and makes the threshold too wide.
      IQR only uses the middle 50% of data — it's "robust" to outliers.
    """
    arr = np.array(values)
    q1 = np.percentile(arr, 25)
    q3 = np.percentile(arr, 75)
    iqr = q3 - q1
    return q1 - k * iqr, q3 + k * iqr


# ─────────────────────────────────────────────────────────────
# CHECK 1: FRESHNESS
# ─────────────────────────────────────────────────────────────

def check_freshness(
    dataset_name: str,
    run_date: date,
    max_age_hours: int = 25,       # allow up to 25h (1 day + buffer)
) -> list[dict]:
    """
    Check whether a dataset has been updated recently enough.

    We look for a snapshot within max_age_hours of run_date.
    If none found -> freshness violation.

    Why 25h not 24h?
      Pipeline scheduling has natural drift. A job scheduled at 2am
      might run at 2:03am. 24h exactly would fire a false alert.
      25h gives a reasonable buffer.
    """
    # Look for any snapshot on run_date itself
    rows = fetch_all(
        """
        SELECT snapshot_date, row_count
        FROM dataset_snapshots
        WHERE dataset_name = %s
          AND snapshot_date = %s
        """,
        (dataset_name, run_date)
    )

    if rows:
        return []   # Snapshot exists for today -> fresh

    # Look for most recent snapshot
    latest = fetch_all(
        """
        SELECT snapshot_date FROM dataset_snapshots
        WHERE dataset_name = %s
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (dataset_name,)
    )

    if not latest:
        # Never seen this dataset before
        return [{
            "check":        "freshness_violation",
            "dataset":      dataset_name,
            "severity":     "high",
            "trigger_type": "freshness_violation",
            "detail":       f"No historical snapshots found for '{dataset_name}'",
            "evidence":     {"last_seen": None, "max_age_hours": max_age_hours},
        }]

    last_date = latest[0]["snapshot_date"]
    age_days  = (run_date - last_date).days
    age_hours = age_days * 24

    if age_hours > max_age_hours:
        severity = "critical" if age_hours > 72 else "high"
        return [{
            "check":        "freshness_violation",
            "dataset":      dataset_name,
            "severity":     severity,
            "trigger_type": "freshness_violation",
            "detail":       (
                f"Dataset '{dataset_name}' last updated {age_hours}h ago "
                f"(SLA: {max_age_hours}h). Last snapshot: {last_date}"
            ),
            "evidence": {
                "last_snapshot_date": str(last_date),
                "age_hours":          age_hours,
                "max_age_hours":      max_age_hours,
            },
        }]

    return []


# ─────────────────────────────────────────────────────────────
# CHECK 2: SCHEMA DRIFT
# ─────────────────────────────────────────────────────────────

def check_schema_drift(
    dataset_name: str,
    run_date: date,
) -> list[dict]:
    """
    Compare today's schema fingerprint to the most recent previous day.

    If the fingerprint changed -> something about columns or types changed.
    We then dig into the schema_snapshot JSON to find exactly what changed.
    """
    # Get today's snapshot
    today_rows = fetch_all(
        """
        SELECT schema_fingerprint, schema_snapshot
        FROM dataset_snapshots
        WHERE dataset_name = %s AND snapshot_date = %s
        """,
        (dataset_name, run_date)
    )

    if not today_rows:
        return []  # No snapshot today -> freshness check handles this

    today_fp     = today_rows[0]["schema_fingerprint"]
    today_schema = today_rows[0]["schema_snapshot"] or {}

    # Get the most recent previous snapshot
    prev_rows = fetch_all(
        """
        SELECT schema_fingerprint, schema_snapshot, snapshot_date
        FROM dataset_snapshots
        WHERE dataset_name = %s AND snapshot_date < %s
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (dataset_name, run_date)
    )

    if not prev_rows:
        return []  # First time seeing this dataset — no baseline to compare

    prev_fp     = prev_rows[0]["schema_fingerprint"]
    prev_schema = prev_rows[0]["schema_snapshot"] or {}
    prev_date   = prev_rows[0]["snapshot_date"]

    if today_fp == prev_fp:
        return []  # No change

    # ── Fingerprints differ — find exactly what changed ───────
    today_cols = set(today_schema.keys())
    prev_cols  = set(prev_schema.keys())

    added   = today_cols - prev_cols
    removed = prev_cols  - today_cols
    changed_types = {
        col: {"before": prev_schema[col], "after": today_schema[col]}
        for col in today_cols & prev_cols
        if today_schema.get(col) != prev_schema.get(col)
    }

    changes_summary = []
    if added:        changes_summary.append(f"Added columns: {sorted(added)}")
    if removed:      changes_summary.append(f"Removed columns: {sorted(removed)}")
    if changed_types:
        for col, diff in changed_types.items():
            changes_summary.append(
                f"Type change: '{col}' {diff['before']} -> {diff['after']}"
            )

    severity = "critical" if changed_types or removed else "medium"

    return [{
        "check":        "schema_drift",
        "dataset":      dataset_name,
        "severity":     severity,
        "trigger_type": "schema_drift",
        "detail":       f"Schema changed vs {prev_date}: " + "; ".join(changes_summary),
        "evidence": {
            "previous_date":        str(prev_date),
            "previous_fingerprint": prev_fp,
            "current_fingerprint":  today_fp,
            "added_columns":        sorted(added),
            "removed_columns":      sorted(removed),
            "changed_types":        changed_types,
        },
    }]


# ─────────────────────────────────────────────────────────────
# CHECK 3: ROW COUNT ANOMALY
# ─────────────────────────────────────────────────────────────

def check_row_count(
    dataset_name: str,
    run_date: date,
    lookback_days: int = 30,
    min_history: int = 5,          # need at least 5 data points for IQR
) -> list[dict]:
    """
    Compare today's row count against an IQR-based historical baseline.

    Only compares weekdays to weekdays, weekends to weekends
    (avoids false alerts every Monday morning).

    Returns anomaly if today's count is outside the Tukey fence.
    """
    # Get today's row count
    today_rows = fetch_all(
        "SELECT row_count FROM dataset_snapshots WHERE dataset_name=%s AND snapshot_date=%s",
        (dataset_name, run_date)
    )
    if not today_rows:
        return []

    today_count = today_rows[0]["row_count"]

    # Get historical row counts (same day type only)
    history = _get_snapshot_history(dataset_name, run_date, lookback_days, same_day_type=True)

    if len(history) < min_history:
        # Not enough history to build a reliable baseline
        # Don't fire a false alert — just skip
        return []

    hist_counts = [r["row_count"] for r in history]
    lower, upper = _iqr_bounds(hist_counts)
    median = float(np.median(hist_counts))

    if lower <= today_count <= upper:
        return []  # Normal

    # Determine severity based on how far outside the bounds we are
    pct_change = abs(today_count - median) / max(median, 1) * 100
    if pct_change > 70:
        severity = "critical"
    elif pct_change > 40:
        severity = "high"
    else:
        severity = "medium"

    direction = "below" if today_count < lower else "above"
    fence     = lower if direction == "below" else upper

    return [{
        "check":        "row_count_anomaly",
        "dataset":      dataset_name,
        "severity":     severity,
        "trigger_type": "row_count_anomaly",
        "detail": (
            f"Row count {today_count:,} is {direction} the {direction.split()[0]} fence "
            f"{fence:,.0f} (baseline median: {median:,.0f}, change: {pct_change:.1f}%)"
        ),
        "evidence": {
            "today_count":       today_count,
            "baseline_median":   round(median, 1),
            "lower_fence":       round(lower, 1),
            "upper_fence":       round(upper, 1),
            "pct_change":        round(pct_change, 1),
            "history_days_used": len(hist_counts),
        },
    }]


# ─────────────────────────────────────────────────────────────
# CHECK 4: NULL RATE SPIKE
# ─────────────────────────────────────────────────────────────

def check_null_rates(
    dataset_name: str,
    run_date: date,
    lookback_days: int = 30,
    min_history: int = 5,
    absolute_threshold: float = 0.05,   # always alert if null rate > 5% (even with no history)
) -> list[dict]:
    """
    Check if any column's null rate spiked compared to its historical baseline.

    Two-layer detection:
      1. If null rate > absolute_threshold (5%) with no history -> alert
      2. If null rate is outside IQR fence compared to history -> alert

    Per-column analysis — a spike in 'revenue' is not the same as a spike in 'name'.
    """
    today_rows = fetch_all(
        "SELECT null_rates FROM dataset_snapshots WHERE dataset_name=%s AND snapshot_date=%s",
        (dataset_name, run_date)
    )
    if not today_rows:
        return []

    today_nulls = today_rows[0]["null_rates"] or {}
    if isinstance(today_nulls, str):
        today_nulls = json.loads(today_nulls)

    history = _get_snapshot_history(dataset_name, run_date, lookback_days, same_day_type=True)

    anomalies = []

    for col, today_rate in today_nulls.items():
        today_rate = float(today_rate)

        # Gather historical null rates for this column
        hist_rates = []
        for snap in history:
            snap_nulls = snap["null_rates"] or {}
            if isinstance(snap_nulls, str):
                snap_nulls = json.loads(snap_nulls)
            if col in snap_nulls:
                hist_rates.append(float(snap_nulls[col]))

        if len(hist_rates) >= min_history:
            lower, upper = _iqr_bounds(hist_rates)
            median = float(np.median(hist_rates))

            if today_rate > upper and today_rate > 0.01:
                pct_spike = (today_rate - median) / max(median, 0.001) * 100
                severity = "critical" if today_rate > 0.50 else "high" if today_rate > 0.20 else "medium"
                anomalies.append({
                    "check":        "null_rate_spike",
                    "dataset":      dataset_name,
                    "severity":     severity,
                    "trigger_type": "null_rate_spike",
                    "detail": (
                        f"Column '{col}' null rate {today_rate*100:.1f}% "
                        f"exceeds upper fence {upper*100:.2f}% "
                        f"(baseline median: {median*100:.3f}%)"
                    ),
                    "evidence": {
                        "column":           col,
                        "today_null_rate":  round(today_rate, 4),
                        "baseline_median":  round(median, 4),
                        "upper_fence":      round(upper, 4),
                        "history_days":     len(hist_rates),
                    },
                })
        else:
            # No history — use absolute threshold as fallback
            if today_rate > absolute_threshold:
                severity = "critical" if today_rate > 0.50 else "high"
                anomalies.append({
                    "check":        "null_rate_spike",
                    "dataset":      dataset_name,
                    "severity":     severity,
                    "trigger_type": "null_rate_spike",
                    "detail": (
                        f"Column '{col}' null rate {today_rate*100:.1f}% "
                        f"exceeds absolute threshold {absolute_threshold*100:.0f}% "
                        f"(insufficient history)"
                    ),
                    "evidence": {
                        "column":          col,
                        "today_null_rate": round(today_rate, 4),
                        "threshold":       absolute_threshold,
                        "history_days":    len(hist_rates),
                    },
                })

    return anomalies
