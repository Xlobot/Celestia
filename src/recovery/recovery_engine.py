"""
Controlled Recovery Engine
==========================
Implements the 3-stage human-in-the-loop pipeline recovery system:
  Stage 1: Recommend (Match evidence to playbook)
  Stage 2: Validate  (Safety preconditions & anti-loop guards)
  Stage 3: Approve & Execute (Simulate retry and cascade down the DAG)
  Stage 4: Post-Recovery Health Verification (Prove anomaly is resolved)
"""

import os
import sys
import json
from datetime import date
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute
from src.graph.pipeline_graph import PipelineGraph
from src.rca.evidence_packager import EvidencePackager
from src.monitoring.checks import check_freshness, check_row_count, check_null_rates, check_schema_drift
from pipelines.run_pipeline import run_pipeline, run_job_ingest_customers, run_job_ingest_orders, run_job_build_sales_summary


class RecoveryEngine:
    """
    Manages safe, human-approved recovery execution and verification.
    """

    MAX_RETRIES = 3

    def __init__(self):
        self.graph = PipelineGraph()
        self.packager = EvidencePackager()

    def generate_recovery_plan(self, incident_id: int) -> Dict[str, Any]:
        """
        Stage 1 & 2: Analyzes incident and generates a validated recovery plan.
        """
        pkg = self.packager.build_package(incident_id)
        if "error" in pkg:
            return pkg

        rec = pkg.get("recommended_recovery", {})
        action = rec.get("action", "MANUAL_INVESTIGATION")
        target_job = rec.get("target_job")
        cascade = rec.get("cascade_downstream", False)

        run_date = date.fromisoformat(pkg["incident_date"])
        date_str = run_date.strftime("%Y%m%d")

        # ── Stage 2: Validation & Safety Preconditions ────────
        validation_errors = []
        validation_warnings = []
        is_safe = True

        # Rule 1: Target job must exist in pipeline DAG
        if not target_job or not self.graph.graph.has_node(target_job):
            validation_errors.append(f"Target job '{target_job}' is not registered in the pipeline DAG.")
            is_safe = False

        # Rule 2: Missing file check
        if pkg["trigger_type"] == "missing_file":
            raw_orders_file = PROJECT_ROOT / "data" / "raw" / f"raw_orders_{date_str}.csv"
            raw_cust_file = PROJECT_ROOT / "data" / "raw" / f"raw_customers_{date_str}.csv"
            
            if target_job == "job_ingest_orders" and not raw_orders_file.exists():
                validation_errors.append(
                    f"Precondition failed: Raw orders source file 'raw_orders_{date_str}.csv' is still missing on disk. Restore source before retry."
                )
                is_safe = False
            elif target_job == "job_ingest_customers" and not raw_cust_file.exists():
                validation_errors.append(
                    f"Precondition failed: Raw customers source file 'raw_customers_{date_str}.csv' is still missing on disk."
                )
                is_safe = False

        # Rule 3: Schema drift safety gate
        if pkg["trigger_type"] == "schema_drift":
            validation_warnings.append(
                "Warning: Schema drift detected. Retrying without acknowledging schema changes may fail joins downstream."
            )

        # Rule 4: Cascading execution path calculation
        execution_order = []
        if target_job:
            execution_order.append(target_job)
            if cascade:
                downstream_jobs = self.graph.get_downstream_impact(target_job, node_type="job")
                execution_order.extend(downstream_jobs)

        plan = {
            "incident_id": incident_id,
            "incident_date": str(run_date),
            "action": action,
            "target_job": target_job,
            "execution_order": execution_order,
            "cascade_downstream": cascade,
            "is_safe_to_execute": is_safe,
            "validation_errors": validation_errors,
            "validation_warnings": validation_warnings,
            "requires_human_approval": True,
            "status": "pending_approval" if is_safe else "blocked",
            "evidence_summary": pkg["anomaly_detail"],
        }

        return plan

    def execute_recovery(
        self,
        incident_id: int,
        approved_by: str = "engineer@example.com",
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Stage 3 & 4: Executes an approved recovery plan, then verifies health.
        """
        plan = self.generate_recovery_plan(incident_id)

        if not plan.get("is_safe_to_execute") and not force:
            return {
                "success": False,
                "error": "Recovery blocked by safety validation checks.",
                "details": plan["validation_errors"]
            }

        run_date = date.fromisoformat(plan["incident_date"])
        print(f"\n{'='*60}")
        print(f"EXECUTING RECOVERY PLAN FOR INCIDENT #{incident_id}")
        print(f"Authorized by: {approved_by}")
        print(f"Action: {plan['action']} on {plan['target_job']}")
        print(f"Execution sequence: {' -> '.join(plan['execution_order'])}")
        print(f"{'='*60}")

        # Update DB status: recovering
        execute(
            "UPDATE incidents SET status = 'investigating' WHERE incident_id = %s",
            (incident_id,)
        )

        # ── Execute the Cascade ───────────────────────────────
        execution_results = {}
        for job_id in plan["execution_order"]:
            print(f"\n[Recovery Execution] Re-running: {job_id} for {run_date}...")

            if job_id == "job_ingest_customers":
                status, df = run_job_ingest_customers(run_date)
            elif job_id == "job_ingest_orders":
                status, df = run_job_ingest_orders(run_date)
            elif job_id == "job_build_sales_summary":
                # For sales summary, load current clean data
                date_str = run_date.strftime("%Y%m%d")
                cust_path = PROJECT_ROOT / "data" / "processed" / f"clean_customers_{date_str}.csv"
                ord_path = PROJECT_ROOT / "data" / "processed" / f"clean_orders_{date_str}.csv"

                import pandas as pd
                c_df = pd.read_csv(cust_path) if cust_path.exists() else None
                o_df = pd.read_csv(ord_path) if ord_path.exists() else None
                status = run_job_build_sales_summary(run_date, c_df, o_df)
            else:
                status = "unknown_job"

            execution_results[job_id] = status
            print(f"  Result: {status}")

            if status not in ("success", "partial_load_warning"):
                print(f"  [ABORT] Step failed, halting downstream cascade.")
                break

        # ── Stage 4: Post-Recovery Health Verification ────────
        print(f"\n{'-'*60}")
        print("STAGE 4: POST-RECOVERY HEALTH VERIFICATION")
        print(f"{'-'*60}")

        # Check all affected datasets from incident
        inc_rows = fetch_all("SELECT affected_datasets FROM incidents WHERE incident_id = %s", (incident_id,))
        aff_raw = inc_rows[0]["affected_datasets"] if inc_rows else []
        if isinstance(aff_raw, str):
            aff_ds = json.loads(aff_raw)
        else:
            aff_ds = aff_raw or []

        remaining_anomalies = []
        for ds in aff_ds:
            row_chk = check_row_count(ds, run_date)
            null_chk = check_null_rates(ds, run_date)
            schema_chk = check_schema_drift(ds, run_date)

            for alert in row_chk + null_chk + schema_chk:
                remaining_anomalies.append(alert["detail"])

        is_resolved = len(remaining_anomalies) == 0 and all(
            s == "success" for s in execution_results.values()
        )

        resolution_status = "resolved" if is_resolved else "investigating"
        notes = (
            f"Recovery executed by {approved_by}. Jobs rerun: {list(execution_results.keys())}. "
            f"Post-verification: {'All clear' if is_resolved else 'Unresolved: ' + str(remaining_anomalies)}"
        )

        # Update database with resolution
        execute(
            """
            UPDATE incidents SET
                status = %s,
                resolution_notes = %s,
                resolved_at = NOW(),
                resolved_by = %s
            WHERE incident_id = %s
            """,
            (resolution_status, notes, approved_by, incident_id)
        )

        print(f"\nFinal Incident Status: {resolution_status.upper()}")
        print(f"Notes: {notes}\n")

        return {
            "incident_id": incident_id,
            "status": resolution_status,
            "jobs_executed": execution_results,
            "is_resolved": is_resolved,
            "notes": notes,
        }


# ─────────────────────────────────────────────────────────────
# Demonstration
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    recovery = RecoveryEngine()

    print("=" * 65)
    print("CONTROLLED RECOVERY ENGINE DEMO")
    print("=" * 65)

    # 1. Test Recovery Planning for Incident #16 (Nov 08 Null Spike)
    plan = recovery.generate_recovery_plan(16)
    print("\n[PLAN CREATED FOR INCIDENT #16]")
    print(f"  Target Job:      {plan['target_job']}")
    print(f"  Action:          {plan['action']}")
    print(f"  Execution Order: {plan['execution_order']}")
    print(f"  Is Safe:         {plan['is_safe_to_execute']}")
    print(f"  Status:          {plan['status']}")
