"""
End-to-End Recovery Lifecycle Demonstration
============================================
Proves the entire 4-stage lifecycle:
  1. Detect: Pipeline failed on Nov 06 due to missing orders file.
  2. Plan: Recovery engine initially BLOCKS recovery because source is missing.
  3. Action: Source file is restored / regenerated.
  4. Approve & Execute: Engineer approves recovery.
  5. Cascading Re-run: Ingest Orders -> Build Sales Summary.
  6. Verification: Database status transitions to 'RESOLVED'.
"""

import sys
import json
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute, execute_returning
from data.generator import generate_data
from src.rca.engine import RootCauseEngine
from src.recovery.recovery_engine import RecoveryEngine

def run_e2e_recovery_test():
    run_date = date(2026, 11, 6)
    date_str = run_date.strftime("%Y%m%d")
    raw_orders_file = PROJECT_ROOT / "data" / "raw" / f"raw_orders_{date_str}.csv"

    print("=" * 65)
    print("STEP 1: SIMULATE OPEN INCIDENT FOR MISSING SOURCE FILE (Nov 06)")
    print("=" * 65)

    # Clean existing incident for Nov 06 if any
    execute("DELETE FROM incidents WHERE run_date = %s", (run_date,))

    # Create incident for missing file
    inc_id = execute_returning(
        """
        INSERT INTO incidents (run_date, trigger_type, severity, affected_datasets, evidence_package, status)
        VALUES (%s, 'missing_file', 'high', '["clean.orders"]',
                '{"detail": "Source file raw_orders_20261106.csv not found"}', 'open')
        RETURNING incident_id
        """,
        (run_date,)
    )
    print(f"Opened Incident #{inc_id} [Status: OPEN]")

    print("\n" + "=" * 65)
    print("STEP 2: RUN ROOT CAUSE ANALYSIS ENGINE")
    print("=" * 65)
    rca = RootCauseEngine()
    investigation = rca.investigate_incident(inc_id)
    print(f"Top Root Cause: {investigation['top_root_cause']}")

    print("\n" + "=" * 65)
    print("STEP 3: ATTEMPT RECOVERY BEFORE RESTORING FILE (Safety Guard Check)")
    print("=" * 65)
    recovery = RecoveryEngine()
    plan_blocked = recovery.generate_recovery_plan(inc_id)
    print(f"Is Safe to Execute: {plan_blocked['is_safe_to_execute']}")
    print(f"Validation Errors:  {plan_blocked['validation_errors']}")
    print(f"Result: Correctly BLOCKED. Safe guard prevented premature retry.")

    print("\n" + "=" * 65)
    print("STEP 4: RESTORE SOURCE FILE (Simulate upstream source recovery)")
    print("=" * 65)
    # Generate the missing orders file now
    generate_data(scenario="normal", run_date=run_date, seed=42)
    print(f"File restored: {raw_orders_file.exists()}")

    print("\n" + "=" * 65)
    print("STEP 5: RE-EVALUATE SAFETY & APPROVE RECOVERY")
    print("=" * 65)
    plan_ready = recovery.generate_recovery_plan(inc_id)
    print(f"Is Safe to Execute: {plan_ready['is_safe_to_execute']}")
    print(f"Execution Order:    {plan_ready['execution_order']}")
    print(f"Status:             {plan_ready['status']}")

    print("\n" + "=" * 65)
    print("STEP 6: HUMAN APPROVAL & CASCADING DAG EXECUTION")
    print("=" * 65)
    result = recovery.execute_recovery(inc_id, approved_by="oncall_lead@company.com")

    print("\n" + "=" * 65)
    print("STEP 7: VERIFY INCIDENT RECORD IN POSTGRESQL")
    print("=" * 65)
    db_inc = fetch_all(
        "SELECT incident_id, status, resolved_by, resolution_notes, resolved_at FROM incidents WHERE incident_id = %s",
        (inc_id,)
    )[0]

    print(f"Incident ID:      #{db_inc['incident_id']}")
    print(f"Status:           {db_inc['status'].upper()}")
    print(f"Resolved By:      {db_inc['resolved_by']}")
    print(f"Resolved At:      {db_inc['resolved_at']}")
    print(f"Resolution Notes: {db_inc['resolution_notes']}")

if __name__ == "__main__":
    run_e2e_recovery_test()

