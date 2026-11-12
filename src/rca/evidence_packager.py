"""
Evidence Packager for LLM & Reporting
======================================
Prepares a structured, grounded evidence bundle from an investigated incident.
The LLM in Week 5 will receive this exact contract — it guarantees the AI
narrator only speaks about verifiable facts grounded in the pipeline metadata.
"""

import sys
import json
from datetime import date
from pathlib import Path
from typing import Dict, Any, Optional, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all
from src.graph.pipeline_graph import PipelineGraph
from src.rca.engine import RootCauseEngine


class EvidencePackager:
    """
    Bundles incident findings, lineage relationships, log snippets,
    and recovery recommendations into a clean evidence payload.
    """

    def __init__(self):
        self.graph = PipelineGraph()
        self.rca_engine = RootCauseEngine()

    def build_package(self, incident_id: int) -> Dict[str, Any]:
        """
        Builds a comprehensive evidence package for a given incident ID.
        """
        # 1. Fetch incident record
        inc_rows = fetch_all(
            """
            SELECT incident_id, run_date, trigger_type, severity,
                   affected_datasets, ranked_causes, evidence_package, status, detected_at
            FROM incidents
            WHERE incident_id = %s
            """,
            (incident_id,)
        )
        if not inc_rows:
            return {"error": f"Incident #{incident_id} not found."}

        inc = inc_rows[0]
        run_date = inc["run_date"]
        trigger_type = inc["trigger_type"]
        severity = inc["severity"].upper()

        affected_datasets = inc["affected_datasets"]
        if isinstance(affected_datasets, str):
            affected_datasets = json.loads(affected_datasets)

        raw_evidence = inc["evidence_package"]
        if isinstance(raw_evidence, str):
            raw_evidence = json.loads(raw_evidence)

        # 2. Extract or run Root Cause investigation
        ranked_causes = inc["ranked_causes"]
        if isinstance(ranked_causes, str):
            ranked_causes = json.loads(ranked_causes)

        if not ranked_causes:
            investigation = self.rca_engine.investigate_incident(incident_id)
            ranked_causes = investigation.get("ranked_causes", [])

        top_cause = ranked_causes[0] if ranked_causes else None

        # 3. Fetch supporting log excerpts for top cause
        log_excerpts = []
        if top_cause:
            job_id = top_cause["job_id"]
            job_logs = fetch_all(
                """
                SELECT execution_id, status, error_message, error_type,
                       rows_read, rows_written, duration_seconds, log_reference
                FROM job_execution_logs
                WHERE job_id = %s AND run_date = %s
                ORDER BY execution_id DESC
                LIMIT 1
                """,
                (job_id, run_date)
            )
            if job_logs:
                jl = job_logs[0]
                if jl["error_message"]:
                    log_excerpts.append(f"[{jl['error_type'] or 'ERROR'}] {jl['error_message']}")
                if jl["status"] == "partial_load_warning":
                    log_excerpts.append(f"[WARNING] Job finished with partial_load_warning (rows read: {jl['rows_read']:,}, written: {jl['rows_written']:,})")
                if jl["log_reference"]:
                    log_excerpts.append(f"Log path reference: {jl['log_reference']}")

        # 4. Determine overall blast radius using DAG
        full_blast_radius = set()
        for ds in affected_datasets:
            full_blast_radius.update(self.graph.get_downstream_impact(ds, node_type="dataset"))
        if top_cause:
            full_blast_radius.update(top_cause.get("impacted_downstream", []))

        # 5. Determine initial recovery recommendation strategy
        recommendation_action = self._determine_recommended_action(trigger_type, top_cause)

        # 6. Assemble the structured package
        package = {
            "incident_id": incident_id,
            "incident_date": str(run_date),
            "detected_at": str(inc["detected_at"]),
            "trigger_type": trigger_type,
            "severity": severity,
            "affected_datasets": affected_datasets,
            "anomaly_detail": raw_evidence.get("detail", "N/A"),
            "root_cause_analysis": {
                "identified": top_cause is not None,
                "top_candidate": top_cause["job_id"] if top_cause else "Unknown",
                "candidate_name": top_cause["job_name"] if top_cause else "Unknown",
                "evidence_score": top_cause["evidence_score"] if top_cause else 0,
                "confidence_level": top_cause["confidence_level"] if top_cause else "NONE",
                "supporting_facts": top_cause["evidence"] if top_cause else [],
                "all_ranked_candidates": [
                    {
                        "rank": c["rank"],
                        "job_id": c["job_id"],
                        "score": c["evidence_score"],
                        "confidence": c["confidence_level"],
                    }
                    for c in ranked_causes
                ],
            },
            "blast_radius": sorted(list(full_blast_radius)),
            "supporting_log_excerpts": log_excerpts,
            "recommended_recovery": recommendation_action,
        }

        return package

    def _determine_recommended_action(self, trigger_type: str, top_cause: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Maps the trigger type and root cause evidence to a specific recovery playbook.
        """
        if not top_cause:
            return {
                "action": "MANUAL_INVESTIGATION",
                "reason": "Could not identify a clear upstream root cause job."
            }

        job_id = top_cause["job_id"]

        if trigger_type == "missing_file":
            return {
                "action": "RESTORE_SOURCE_AND_RETRY",
                "target_job": job_id,
                "cascade_downstream": True,
                "reason": "Source file was missing or unreachable. Verify file presence before triggering retry."
            }
        elif trigger_type == "schema_drift":
            return {
                "action": "SCHEMA_PATCH_AND_MIGRATE",
                "target_job": job_id,
                "cascade_downstream": False,
                "reason": "Column types changed. Requires human approval of schema evolution before re-running."
            }
        elif trigger_type == "null_rate_spike":
            return {
                "action": "FILTER_QUARANTINE_AND_REPROCESS",
                "target_job": job_id,
                "cascade_downstream": True,
                "reason": "Abnormal null rates detected. Quarantine defective rows and re-aggregate downstream."
            }
        elif trigger_type == "row_count_anomaly":
            return {
                "action": "VALIDATE_SOURCE_VOLUME_AND_BACKFILL",
                "target_job": job_id,
                "cascade_downstream": True,
                "reason": "Severe volume drop observed. Check upstream export completeness and backfill."
            }
        else:
            return {
                "action": "RETRY_JOB",
                "target_job": job_id,
                "cascade_downstream": True,
                "reason": "Standard job execution retry."
            }


# ─────────────────────────────────────────────────────────────
# Self-test
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    packager = EvidencePackager()

    # Test with incident #16 (Nov 08 null spike)
    sample_pkg = packager.build_package(16)
    print("=" * 65)
    print(f"EVIDENCE PACKAGE FOR INCIDENT #{sample_pkg.get('incident_id')}")
    print("=" * 65)
    print(json.dumps(sample_pkg, indent=2))

