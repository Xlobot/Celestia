"""
Root Cause Analysis (RCA) Ranking Engine
========================================
Combines:
  1. Dependency DAG lineage (NetworkX)
  2. Execution statuses & timings (PostgreSQL job_execution_logs)
  3. Monitoring anomalies (incidents table)
  4. Isolation Forest ML execution scores

Produces:
  A ranked list of candidate root causes with grounded evidence and confidence scores.
"""

import sys
import json
from datetime import date
from pathlib import Path
from typing import List, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute
from src.graph.pipeline_graph import PipelineGraph
from src.anomaly.isolation_forest import JobExecutionAnomalyDetector


class RootCauseEngine:
    """
    Investigates data pipeline incidents to identify and rank the true root cause.
    """

    def __init__(self):
        self.graph = PipelineGraph()
        self.ml_detector = JobExecutionAnomalyDetector(contamination=0.08)

    def investigate_incident(self, incident_id: int) -> Dict[str, Any]:
        """
        Investigates a specific incident by ID.
        """
        inc_rows = fetch_all(
            "SELECT incident_id, run_date, trigger_type, severity, affected_datasets, evidence_package "
            "FROM incidents WHERE incident_id = %s",
            (incident_id,)
        )
        if not inc_rows:
            return {"error": f"Incident #{incident_id} not found."}

        incident = inc_rows[0]
        run_date = incident["run_date"]
        affected_datasets = incident["affected_datasets"]
        if isinstance(affected_datasets, str):
            affected_datasets = json.loads(affected_datasets)

        return self.investigate_date(run_date, target_datasets=affected_datasets, incident_id=incident_id)

    def investigate_date(
        self,
        run_date: date,
        target_datasets: Optional[List[str]] = None,
        incident_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Runs comprehensive root cause analysis for all symptoms observed on `run_date`.
        """
        # 1. Fetch all job executions for this date
        exec_rows = fetch_all(
            """
            SELECT execution_id, job_id, run_date, started_at, ended_at,
                   duration_seconds, status, rows_read, rows_written,
                   error_message, error_type, schema_fingerprint
            FROM job_execution_logs
            WHERE run_date = %s
            ORDER BY started_at ASC
            """,
            (run_date,)
        )
        exec_map = {row["job_id"]: row for row in exec_rows}

        # 2. Fetch all incidents / anomalies for this date
        incidents = fetch_all(
            """
            SELECT incident_id, trigger_type, severity, affected_datasets, evidence_package
            FROM incidents
            WHERE run_date = %s
            """,
            (run_date,)
        )

        # Build map of datasets that showed anomalies
        anomalous_datasets = set()
        for inc in incidents:
            d_list = inc["affected_datasets"]
            if isinstance(d_list, str):
                d_list = json.loads(d_list)
            anomalous_datasets.update(d_list)

        # If target_datasets specified, use them; otherwise use all anomalous datasets
        if not target_datasets:
            target_datasets = list(anomalous_datasets)

        # 3. Fetch Isolation Forest anomalies for this date
        iso_anomalies = {}
        for job_id in exec_map.keys():
            job_anoms = self.ml_detector.detect_anomalies_for_job(job_id)
            for a in job_anoms:
                if a["run_date"] == str(run_date):
                    iso_anomalies[job_id] = a

        # 4. Evaluate and score all candidate jobs
        candidates = []
        registered_jobs = fetch_all("SELECT job_id, job_name, input_datasets, output_datasets FROM pipeline_jobs")

        for job_meta in registered_jobs:
            job_id = job_meta["job_id"]
            job_exec = exec_map.get(job_id)

            score = 0
            evidence_points = []

            # ── Factor 1: Upstream Lineage ───────────────────────
            # Check if this job is upstream of any of the affected datasets
            is_upstream = False
            for ds in target_datasets:
                ancestors = self.graph.get_upstream_ancestors(ds, node_type="job")
                if job_id in ancestors or job_id == ds:
                    is_upstream = True
                    break

            if is_upstream:
                score += 25
                evidence_points.append(
                    f"Job '{job_id}' is directly upstream in the DAG lineage of the affected dataset(s)."
                )

            # ── Factor 2: Job Execution Status ───────────────────
            if job_exec:
                status = job_exec["status"]
                if status == "failed":
                    score += 35
                    err_type = job_exec.get("error_type") or "JobFailed"
                    err_msg = job_exec.get("error_message") or ""
                    evidence_points.append(
                        f"Job execution explicitly failed with status 'failed' ({err_type}: {err_msg})."
                    )
                elif status == "partial_load_warning":
                    score += 25
                    evidence_points.append(
                        f"Job completed with a 'partial_load_warning' (data quality degradation detected during run)."
                    )
                elif status == "skipped":
                    score += 10
                    evidence_points.append(
                        f"Job was skipped due to an upstream dependency failure."
                    )
            else:
                # Job never ran on this date!
                score += 30
                evidence_points.append(
                    f"Job did not execute on {run_date} (missing run record)."
                )

            # ── Factor 3: Input/Output Anomaly Association ───────
            # Did this job produce or consume an anomalous dataset?
            outputs = job_meta["output_datasets"]
            if isinstance(outputs, str):
                outputs = json.loads(outputs)
            inputs = job_meta["input_datasets"]
            if isinstance(inputs, str):
                inputs = json.loads(inputs)

            produced_anomalies = [ds for ds in outputs if ds in anomalous_datasets]
            consumed_anomalies = [ds for ds in inputs if ds in anomalous_datasets]

            if produced_anomalies:
                score += 20
                evidence_points.append(
                    f"Job produces dataset(s) exhibiting verified anomalies: {produced_anomalies}."
                )

            # ── Factor 4: ML Anomaly Score (Isolation Forest) ────
            if job_id in iso_anomalies:
                iso_item = iso_anomalies[job_id]
                score += 10
                evidence_points.append(
                    f"Isolation Forest flagged execution metrics as anomalous (score: {iso_item['anomaly_score']}, throughput: {iso_item['throughput']} rows/s)."
                )

            # ── Factor 5: Earliest Fault in Timeline ─────────────
            # If this job has an error or produced the earliest anomaly, bonus score
            if is_upstream and (job_exec and job_exec["status"] in ("failed", "partial_load_warning") or produced_anomalies):
                # Check if upstream jobs from this one are clean
                upstream_of_job = self.graph.get_upstream_ancestors(job_id, node_type="job")
                upstream_failed = any(
                    exec_map.get(uj, {}).get("status") in ("failed", "partial_load_warning")
                    for uj in upstream_of_job
                )
                if not upstream_failed:
                    score += 10
                    evidence_points.append(
                        f"This is the earliest (most upstream) failing component in the dependency chain."
                    )

            # Only consider jobs that have at least some relevance
            if score > 0:
                confidence_level = "HIGH" if score >= 60 else "MEDIUM" if score >= 35 else "LOW"
                candidates.append({
                    "job_id": job_id,
                    "job_name": job_meta["job_name"],
                    "evidence_score": min(score, 100),
                    "confidence_level": confidence_level,
                    "evidence": evidence_points,
                    "execution_status": job_exec.get("status") if job_exec else "missing",
                    "impacted_downstream": self.graph.get_downstream_impact(job_id, node_type="dataset")
                })

        # Sort candidates descending by evidence score
        candidates.sort(key=lambda x: x["evidence_score"], reverse=True)

        # Assign ranks
        for idx, c in enumerate(candidates, start=1):
            c["rank"] = idx

        # Determine Root Cause (Rank 1)
        top_cause = candidates[0] if candidates else None

        result_package = {
            "run_date": str(run_date),
            "target_datasets": target_datasets,
            "anomalous_datasets": list(anomalous_datasets),
            "top_root_cause": top_cause["job_id"] if top_cause else None,
            "ranked_causes": candidates,
            "total_candidates_evaluated": len(candidates)
        }

        # 5. Persist the ranked causes back into the incidents table if incident_id provided
        if incident_id:
            execute(
                """
                UPDATE incidents
                SET ranked_causes = %s,
                    status = 'investigating'
                WHERE incident_id = %s
                """,
                (json.dumps(candidates), incident_id)
            )

        return result_package

    def batch_investigate_all_open_incidents(self) -> int:
        """
        Runs RCA for all open incidents in the database and updates them with ranked causes.
        """
        open_incs = fetch_all("SELECT incident_id, run_date FROM incidents WHERE status = 'open'")
        print(f"Investigating {len(open_incs)} open incidents...")

        for inc in open_incs:
            self.investigate_incident(inc["incident_id"])

        print(f"All {len(open_incs)} incidents investigated and updated with ranked root causes.")
        return len(open_incs)


# ─────────────────────────────────────────────────────────────
# CLI & Demonstration
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    engine = RootCauseEngine()

    print("=" * 65)
    print("ROOT CAUSE RANKING ENGINE — SCENARIO VERIFICATION")
    print("=" * 65)

    test_dates = [
        ("2026-11-11", "Row Drop Scenario (2055 rows)"),
        ("2026-11-09", "Schema Drift Scenario (int -> str)"),
        ("2026-11-08", "Null Spike Scenario (revenue nulls)"),
        ("2026-11-06", "Missing File Scenario (orders missing)"),
    ]

    for d_str, label in test_dates:
        d = date.fromisoformat(d_str)
        print(f"\n{'-'*65}")
        print(f"Investigation for: {label} [{d_str}]")
        print(f"{'-'*65}")

        report = engine.investigate_date(d)
        print(f"Target/Anomalous Datasets: {report['anomalous_datasets']}")
        print(f"Top Root Cause Identified: {report['top_root_cause']}\n")

        for cand in report["ranked_causes"]:
            print(f"  Rank #{cand['rank']}: {cand['job_id']} [{cand['confidence_level']} - {cand['evidence_score']} pts]")
            for ev in cand["evidence"]:
                print(f"    - {ev}")
            print(f"    Blast radius: {cand['impacted_downstream']}")

