"""
Monitor Orchestrator
====================
Runs all 4 checks for all datasets on a given date.
Writes detected anomalies to the incidents table.

Usage:
    python src/monitoring/monitor.py --date 2026-11-11   ← should catch row_drop
    python src/monitoring/monitor.py --date 2026-11-09   ← should catch schema_drift
    python src/monitoring/monitor.py --date 2026-11-08   ← should catch null_spike
    python src/monitoring/monitor.py --date 2026-11-06   ← should catch missing_file
    python src/monitoring/monitor.py --date 2026-11-12   ← should be clean
"""

import sys
import json
import argparse
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute_returning
from src.monitoring.checks import (
    check_freshness,
    check_schema_drift,
    check_row_count,
    check_null_rates,
)

# Datasets to monitor and their freshness SLAs (hours)
MONITORED_DATASETS = {
    "raw.customers":                    {"sla_hours": 25},
    "raw.orders":                       {"sla_hours": 25},
    "clean.customers":                  {"sla_hours": 25},
    "clean.orders":                     {"sla_hours": 25},
    "analytics.customer_sales_summary": {"sla_hours": 25},
}

SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1}


def run_monitoring(run_date: date) -> list[dict]:
    """
    Run all checks for all monitored datasets on run_date.
    Writes any detected anomalies to the incidents table.
    Returns list of incidents created.
    """
    print(f"\n{'='*60}")
    print(f"  MONITORING RUN — {run_date}")
    print(f"{'='*60}")

    all_anomalies = []

    for dataset_name, config in MONITORED_DATASETS.items():
        print(f"\n  Checking: {dataset_name}")
        print(f"  {'-'*50}")

        dataset_anomalies = []

        # ── Run all 4 checks ─────────────────────────────────
        checks = [
            ("Freshness",    check_freshness(dataset_name, run_date, config["sla_hours"])),
            ("Schema Drift", check_schema_drift(dataset_name, run_date)),
            ("Row Count",    check_row_count(dataset_name, run_date)),
            ("Null Rates",   check_null_rates(dataset_name, run_date)),
        ]

        for check_name, results in checks:
            if results:
                for r in results:
                    print(f"    [ALERT] {check_name}: {r['detail']}")
                dataset_anomalies.extend(results)
            else:
                print(f"    [OK]    {check_name}")

        all_anomalies.extend(dataset_anomalies)

    # ── Write incidents to database ───────────────────────────
    incidents_created = []

    if not all_anomalies:
        print(f"\n  No anomalies detected for {run_date}. All checks passed.")
    else:
        print(f"\n  {len(all_anomalies)} anomaly/anomalies detected. Writing incidents...")

        # Group anomalies: one incident per unique (trigger_type, dataset)
        for anomaly in all_anomalies:
            incident_id = execute_returning(
                """
                INSERT INTO incidents
                    (run_date, trigger_type, severity, affected_datasets,
                     ranked_causes, evidence_package)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING incident_id
                """,
                (
                    run_date,
                    anomaly["trigger_type"],
                    anomaly["severity"],
                    json.dumps([anomaly["dataset"]]),
                    json.dumps([]),                     # filled by RCA engine (Week 4)
                    json.dumps({
                        "check":    anomaly["check"],
                        "detail":   anomaly["detail"],
                        "evidence": anomaly.get("evidence", {}),
                    }),
                )
            )
            anomaly["incident_id"] = incident_id
            incidents_created.append(anomaly)
            print(f"    -> Incident #{incident_id}: [{anomaly['severity'].upper()}] "
                  f"{anomaly['trigger_type']} on {anomaly['dataset']}")

        # ── Automatically invoke RCA to populate ranked causes ───
        print("\n  Triggering Root Cause Analysis on newly created incidents...")
        try:
            from src.rca.engine import RootCauseEngine
            rca_engine = RootCauseEngine()
            for anomaly in incidents_created:
                inc_id = anomaly["incident_id"]
                rca_res = rca_engine.investigate_incident(inc_id)
                top_c = rca_res.get("top_root_cause", "Unknown")
                print(f"    [RCA Resolved] Incident #{inc_id} attributed to: {top_c}")
        except Exception as rca_err:
            print(f"    [WARNING] Automated RCA failed to run: {rca_err}")

    print(f"\n{'='*60}")
    print(f"  Monitoring complete. {len(incidents_created)} incident(s) opened.")
    print(f"{'='*60}\n")

    return incidents_created


def print_incident_summary(run_date: date):
    """Print a formatted summary of all incidents for a date."""
    rows = fetch_all(
        """
        SELECT incident_id, trigger_type, severity, affected_datasets,
               evidence_package, status, detected_at
        FROM incidents
        WHERE run_date = %s
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                WHEN 'medium'   THEN 3 ELSE 4
            END
        """,
        (run_date,)
    )

    if not rows:
        print(f"\n  No incidents for {run_date}")
        return

    print(f"\n{'='*60}")
    print(f"  INCIDENT REPORT — {run_date}  ({len(rows)} incidents)")
    print(f"{'='*60}")

    for r in rows:
        pkg  = r["evidence_package"] or {}
        datasets = r["affected_datasets"] or []
        print(f"\n  Incident #{r['incident_id']}  [{r['severity'].upper()}]")
        print(f"    Type    : {r['trigger_type']}")
        print(f"    Dataset : {datasets}")
        print(f"    Detail  : {pkg.get('detail', 'N/A')}")
        print(f"    Status  : {r['status']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run monitoring checks for a date")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--report-only", action="store_true",
                        help="Only print incident report, don't run checks")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()

    if args.report_only:
        print_incident_summary(run_date)
    else:
        run_monitoring(run_date)
        print_incident_summary(run_date)

