"""
Fault Injection & System Evaluation Benchmark
==============================================
Runs controlled failure experiments against the pipeline and computes
empirical performance metrics:
  1. Top-1 Root Cause Accuracy
  2. Top-3 Root Cause Accuracy
  3. False Positive Rate (FPR) on normal runs
  4. Blast Radius (Impact) Precision and Recall
  5. Mean Diagnosis Time (latency)
"""

import sys
import time
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import execute, fetch_all
from data.generator import generate_data
from pipelines.run_pipeline import run_pipeline
from src.monitoring.monitor import run_monitoring
from src.rca.engine import RootCauseEngine
from src.graph.pipeline_graph import PipelineGraph


class SystemEvaluator:
    """
    Automated benchmark harness to evaluate RCA accuracy and reliability.
    """

    def __init__(self):
        self.rca = RootCauseEngine()
        self.graph = PipelineGraph()

    def run_benchmark(self) -> Dict[str, Any]:
        """
        Executes randomized benchmark trials and returns statistical metrics.
        """
        print("=" * 70)
        print("STARTING FORMAL SYSTEM EVALUATION BENCHMARK")
        print("=" * 70)

        trials = [
            # Normal runs to test False Positive Rate
            {"scenario": "normal", "date": date(2026, 12, 1), "expected_cause": None, "label": "Normal Run #1"},
            {"scenario": "normal", "date": date(2026, 12, 2), "expected_cause": None, "label": "Normal Run #2"},
            {"scenario": "normal", "date": date(2026, 12, 3), "expected_cause": None, "label": "Normal Run #3"},

            # Fault injection runs
            {"scenario": "row_drop", "date": date(2026, 12, 4), "expected_cause": "job_ingest_customers", "label": "80% Volume Drop"},
            {"scenario": "null_spike", "date": date(2026, 12, 5), "expected_cause": "job_ingest_orders", "label": "Revenue Null Spike"},
            {"scenario": "missing_file", "date": date(2026, 12, 6), "expected_cause": "job_ingest_orders", "label": "Missing Source File"},
            {"scenario": "schema_drift", "date": date(2026, 12, 7), "expected_cause": "job_ingest_customers", "label": "Schema Drift (int->str)"},
            {"scenario": "row_drop", "date": date(2026, 12, 8), "expected_cause": "job_ingest_customers", "label": "Repeat Volume Drop"},
            {"scenario": "null_spike", "date": date(2026, 12, 9), "expected_cause": "job_ingest_orders", "label": "Repeat Null Spike"},
        ]

        total_fault_trials = 0
        top1_correct = 0
        top3_correct = 0

        normal_trials = 0
        false_positives = 0

        latencies = []
        precision_scores = []
        recall_scores = []

        for idx, t in enumerate(trials, start=1):
            t_date = t["date"]
            scen = t["scenario"]
            expected = t["expected_cause"]
            label = t["label"]

            print(f"\n[Trial {idx}/{len(trials)}] Testing: {label} ({scen}) on {t_date}...")

            # Clean any old test records for this date
            execute("DELETE FROM incidents WHERE run_date = %s", (t_date,))
            execute("DELETE FROM job_execution_logs WHERE run_date = %s", (t_date,))
            execute("DELETE FROM dataset_snapshots WHERE snapshot_date = %s", (t_date,))

            # 1. Generate data with scenario
            generate_data(scenario=scen, run_date=t_date, seed=500 + idx)

            # 2. Execute pipeline
            run_pipeline(t_date)

            # 3. Run monitoring checks
            incidents_created = run_monitoring(t_date)

            # 4. Measure diagnosis time & accuracy
            if scen == "normal":
                normal_trials += 1
                if len(incidents_created) > 0:
                    false_positives += 1
                    print(f"  Result: [FAIL] False Positive! {len(incidents_created)} alert(s) triggered on normal data.")
                else:
                    print(f"  Result: [PASS] Zero false alarms.")
            else:
                total_fault_trials += 1

                t_start = time.time()
                inv = self.rca.investigate_date(t_date)
                latency_ms = (time.time() - t_start) * 1000
                latencies.append(latency_ms)

                ranked_jobs = [c["job_id"] for c in inv.get("ranked_causes", [])]
                top_1 = ranked_jobs[0] if ranked_jobs else None
                top_3 = ranked_jobs[:3]

                # Top-1 Check
                is_top1 = (top_1 == expected)
                if is_top1:
                    top1_correct += 1

                # Top-3 Check
                is_top3 = (expected in top_3)
                if is_top3:
                    top3_correct += 1

                # Blast radius accuracy
                if top_1:
                    predicted_blast = set(self.graph.get_downstream_impact(top_1, node_type="dataset"))
                    actual_anomalous = set(inv.get("anomalous_datasets", []))

                    # Precision: of predicted blast, how many actually had anomalies?
                    intersect = predicted_blast.intersection(actual_anomalous)
                    prec = len(intersect) / len(predicted_blast) if predicted_blast else 1.0
                    rec = len(intersect) / len(actual_anomalous) if actual_anomalous else 1.0

                    precision_scores.append(prec)
                    recall_scores.append(rec)

                print(f"  Result: Top-1={top_1} (Expected={expected}) | Top-1 OK: {is_top1} | Latency: {latency_ms:.1f}ms")

        # ── Compute Summary Metrics ───────────────────────────
        top1_acc = (top1_correct / total_fault_trials * 100) if total_fault_trials else 0
        top3_acc = (top3_correct / total_fault_trials * 100) if total_fault_trials else 0
        fpr = (false_positives / normal_trials * 100) if normal_trials else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        avg_precision = (sum(precision_scores) / len(precision_scores) * 100) if precision_scores else 100
        avg_recall = (sum(recall_scores) / len(recall_scores) * 100) if recall_scores else 100

        metrics = {
            "total_trials": len(trials),
            "fault_trials": total_fault_trials,
            "normal_trials": normal_trials,
            "top1_accuracy_pct": round(top1_acc, 1),
            "top3_accuracy_pct": round(top3_acc, 1),
            "false_positive_rate_pct": round(fpr, 1),
            "avg_diagnosis_time_ms": round(avg_latency, 2),
            "blast_radius_precision_pct": round(avg_precision, 1),
            "blast_radius_recall_pct": round(avg_recall, 1),
        }

        print("\n" + "=" * 70)
        print("BENCHMARK EVALUATION SUMMARY")
        print("=" * 70)
        print(f"  Top-1 Root Cause Accuracy       : {metrics['top1_accuracy_pct']}% ({top1_correct}/{total_fault_trials})")
        print(f"  Top-3 Root Cause Accuracy       : {metrics['top3_accuracy_pct']}% ({top3_correct}/{total_fault_trials})")
        print(f"  False Positive Rate (FPR)       : {metrics['false_positive_rate_pct']}% ({false_positives}/{normal_trials})")
        print(f"  Blast Radius Precision          : {metrics['blast_radius_precision_pct']}%")
        print(f"  Blast Radius Recall             : {metrics['blast_radius_recall_pct']}%")
        print(f"  Average Diagnosis Time          : {metrics['avg_diagnosis_time_ms']} ms")
        print("=" * 70)

        return metrics


if __name__ == "__main__":
    evaluator = SystemEvaluator()
    evaluator.run_benchmark()

