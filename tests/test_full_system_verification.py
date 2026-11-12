"""
Master System Verification Suite
=================================
Runs automated health checks across all components built in Weeks 1 - 4:
  1. PostgreSQL Database & Tables
  2. NetworkX Bipartite DAG
  3. Monitoring Checks & IQR Baselines
  4. Isolation Forest ML Detector
  5. Root Cause Analysis Ranking Engine
  6. Evidence Packaging Contract
  7. Controlled Recovery Engine (Safety Preconditions & Cascading Re-run)
"""

import sys
import json
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import test_connection, fetch_all
from src.graph.pipeline_graph import PipelineGraph
from src.monitoring.checks import check_freshness, check_schema_drift, check_row_count, check_null_rates
from src.anomaly.isolation_forest import JobExecutionAnomalyDetector
from src.rca.engine import RootCauseEngine
from src.rca.evidence_packager import EvidencePackager
from src.recovery.recovery_engine import RecoveryEngine

def run_suite():
    print("=" * 70)
    print("AI DATA PIPELINE RCA & RECOVERY ASSISTANT — MASTER HEALTH CHECK")
    print("=" * 70)

    scorecard = []

    def log_result(name: str, passed: bool, details: str = ""):
        tag = "[PASS]" if passed else "[FAIL]"
        print(f"  {tag:<7} | {name:<42} | {details}")
        scorecard.append((name, passed, details))

    # ─────────────────────────────────────────────────────────
    # 1. DATABASE CONNECTIVITY & RECORD COUNTS
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 1: Database & Storage]")
    db_ok = test_connection()
    log_result("PostgreSQL Connection", db_ok, "Connected to pipeline_rca")

    if db_ok:
        counts = {}
        for tbl in ["pipeline_jobs", "job_execution_logs", "dataset_snapshots", "incidents"]:
            res = fetch_all(f"SELECT COUNT(*) as c FROM {tbl}")
            counts[tbl] = res[0]["c"]

        log_result(
            "Registered Jobs in DB",
            counts.get("pipeline_jobs", 0) >= 3,
            f"{counts.get('pipeline_jobs', 0)} jobs registered"
        )
        log_result(
            "Execution Logs Populated",
            counts.get("job_execution_logs", 0) >= 20,
            f"{counts.get('job_execution_logs', 0)} run records"
        )
        log_result(
            "Dataset Snapshots Baseline",
            counts.get("dataset_snapshots", 0) >= 50,
            f"{counts.get('dataset_snapshots', 0)} health profiles"
        )

    # ─────────────────────────────────────────────────────────
    # 2. NETWORKX DEPENDENCY DAG
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 2: Pipeline Dependency Graph (NetworkX)]")
    try:
        pg = PipelineGraph()
        node_count = pg.graph.number_of_nodes()
        edge_count = pg.graph.number_of_edges()
        log_result("Graph Construction", node_count == 8 and edge_count == 7, f"{node_count} nodes, {edge_count} edges")

        ancestors = pg.get_upstream_ancestors("analytics.customer_sales_summary", node_type="job")
        expected_ancestors = {"job_ingest_customers", "job_ingest_orders", "job_build_sales_summary"}
        log_result("Upstream Ancestry Lineage", set(ancestors) == expected_ancestors, f"Identified: {ancestors}")

        blast = pg.get_downstream_impact("job_ingest_customers", node_type="dataset")
        expected_blast = {"clean.customers", "analytics.customer_sales_summary"}
        log_result("Blast Radius Calculation", set(blast) == expected_blast, f"Impact: {blast}")
    except Exception as e:
        log_result("Graph Construction", False, str(e))

    # ─────────────────────────────────────────────────────────
    # 3. MONITORING & STATISTICAL CHECKS
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 3: Monitoring & Validation Engine]")
    try:
        # Check normal day (2026-11-12) -> should have 0 alerts
        norm_row = check_row_count("clean.customers", date(2026, 11, 12))
        norm_null = check_null_rates("clean.orders", date(2026, 11, 12))
        log_result("Normal Day (Zero False Positives)", len(norm_row) == 0 and len(norm_null) == 0, "No alerts on clean data")

        # Check row drop day (2026-11-11) -> should catch row count anomaly
        drop_alerts = check_row_count("clean.customers", date(2026, 11, 11))
        log_result("Row Count Anomaly Detection (IQR)", len(drop_alerts) > 0, f"Caught: {drop_alerts[0]['detail'] if drop_alerts else 'None'}")

        # Check null spike day (2026-11-08) -> should catch revenue null spike
        null_alerts = check_null_rates("clean.orders", date(2026, 11, 8))
        log_result("Null Rate Spike Detection", len(null_alerts) > 0, f"Caught: {null_alerts[0]['detail'] if null_alerts else 'None'}")

        # Check schema drift day (2026-11-09) -> should catch type change
        drift_alerts = check_schema_drift("clean.customers", date(2026, 11, 9))
        log_result("Schema Drift Detection", len(drift_alerts) > 0, f"Caught: {drift_alerts[0]['detail'] if drift_alerts else 'None'}")
    except Exception as e:
        log_result("Monitoring Engine", False, str(e))

    # ─────────────────────────────────────────────────────────
    # 4. ISOLATION FOREST ML DETECTOR
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 4: Machine Learning Anomaly Detection]")
    try:
        iso_detector = JobExecutionAnomalyDetector(contamination=0.08)
        anoms = iso_detector.detect_anomalies_for_job("job_ingest_customers")
        anom_dates = [a["run_date"] for a in anoms]
        # Check that Isolation Forest correctly flagged known volume drop runs
        known_drop_dates = {"2026-11-11", "2026-12-04", "2026-12-08", "2026-11-07"}
        has_overlap = bool(set(anom_dates).intersection(known_drop_dates))
        log_result("Isolation Forest Execution Scoring", has_overlap, f"Flagged dates: {anom_dates}")
    except Exception as e:
        log_result("Isolation Forest ML Detector", False, str(e))

    # ─────────────────────────────────────────────────────────
    # 5. ROOT CAUSE ANALYSIS ENGINE
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 5: Root Cause Ranking Engine]")
    try:
        rca = RootCauseEngine()
        # On Nov 08 null spike, top root cause must be job_ingest_orders
        inv_08 = rca.investigate_date(date(2026, 11, 8))
        top_cause = inv_08.get("top_root_cause")
        log_result(
            "RCA Grounded Attribution (Null Spike)",
            top_cause == "job_ingest_orders",
            f"Rank #1: {top_cause} (Score: {inv_08['ranked_causes'][0]['evidence_score']} pts)"
        )

        # On Nov 11 row drop, top root cause must be ingestion
        inv_11 = rca.investigate_date(date(2026, 11, 11))
        top_cause_11 = inv_11.get("top_root_cause")
        log_result(
            "RCA Grounded Attribution (Row Drop)",
            top_cause_11 in ("job_ingest_customers", "job_ingest_orders"),
            f"Rank #1: {top_cause_11}"
        )
    except Exception as e:
        log_result("RCA Engine", False, str(e))

    # ─────────────────────────────────────────────────────────
    # 6. EVIDENCE PACKAGER CONTRACT
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 6: Evidence Packaging Contract]")
    try:
        packager = EvidencePackager()
        inc_list = fetch_all("SELECT incident_id FROM incidents ORDER BY incident_id DESC LIMIT 1")
        if inc_list:
            pkg = packager.build_package(inc_list[0]["incident_id"])
            required_keys = {"incident_id", "incident_date", "trigger_type", "severity", "root_cause_analysis", "blast_radius", "recommended_recovery"}
            has_keys = required_keys.issubset(set(pkg.keys()))
            log_result("Evidence Contract Completeness", has_keys, f"All {len(required_keys)} contract keys present")
        else:
            log_result("Evidence Contract Completeness", False, "No incidents found to package")
    except Exception as e:
        log_result("Evidence Packager", False, str(e))

    # ─────────────────────────────────────────────────────────
    # 7. CONTROLLED RECOVERY ENGINE
    # ─────────────────────────────────────────────────────────
    print("\n[Layer 7: Controlled Recovery Engine]")
    try:
        recovery = RecoveryEngine()
        if inc_list:
            plan = recovery.generate_recovery_plan(inc_list[0]["incident_id"])
            log_result("Recovery Plan Formulation", plan.get("action") is not None, f"Action: {plan.get('action')}")
            log_result("Safety Gate Assessment", "is_safe_to_execute" in plan, f"Status: {plan.get('status')}")
        else:
            log_result("Recovery Plan Formulation", False, "No incident to test")
    except Exception as e:
        log_result("Recovery Engine", False, str(e))

    # ─────────────────────────────────────────────────────────
    # SUMMARY SCORECARD
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("VERIFICATION SCORECARD")
    print("=" * 70)
    total_checks = len(scorecard)
    passed_checks = sum(1 for _, p, _ in scorecard if p)

    print(f"Total Tests Run : {total_checks}")
    print(f"Passed          : {passed_checks}")
    print(f"Failed          : {total_checks - passed_checks}")
    print(f"Success Rate    : {passed_checks / total_checks * 100:.1f}%\n")

    if passed_checks == total_checks:
        print(">> ALL SYSTEMS OPERATIONAL. 100% PASS RATE. <<")
    else:
        print(">> SOME CHECKS FAILED. PLEASE REVIEW LOG ABOVE. <<")
    print("=" * 70)

if __name__ == "__main__":
    run_suite()

