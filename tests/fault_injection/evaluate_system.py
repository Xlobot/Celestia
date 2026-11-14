"""
Fault Injection & System Evaluation Benchmark
==============================================
Runs controlled failure experiments against the pipeline and computes
empirical performance metrics against explicit ground-truth expectations:
  1. Top-1 Root Cause Accuracy
  2. Top-3 Root Cause Accuracy
  3. False Positive Rate (FPR) on normal runs
  4. Blast Radius (Impact) Precision and Recall against Ground Truth Lineage
  5. Mean Diagnosis Time (latency)
"""

import sys
import time
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Set

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

        # Ground truth architectural blast radius per root cause
        # When job_ingest_customers fails -> clean.customers & analytics.customer_sales_summary
        # When job_ingest_orders fails -> clean.orders & analytics.customer_sales_summary
        GROUND_TRUTH_BLAST = {
            "job_ingest_customers": {"clean.customers", "analytics.customer_sales_summary"},
            "job_ingest_orders":    {"clean.orders", "analytics.customer_sales_summary"},
            "job_build_sales_summary": {"analytics.customer_sales_summary"},
        }

        trials = [
            # Normal baseline runs to test False Positive Rate
            {"scenario": "normal", "date": date(2026, 12, 1), "expected_cause": None, "label": "Normal Baseline #1"},
            {"scenario": "normal", "date": date(2026, 12, 2), "expected_cause": None, "label": "Normal Baseline #2"},
            {"scenario": "normal", "date": date(2026, 12, 3), "expected_cause": None, "label": "Normal Baseline #3"},
            {"scenario": "normal", "date": date(2026, 12, 10), "expected_cause": None, "label": "Normal Baseline #4"},
            {"scenario": "normal", "date": date(2026, 12, 11), "expected_cause": None, "label": "Normal Baseline #5"},

            # Fault injection runs (10 trials)
            {"scenario": "row_drop", "date": date(2026, 12, 4), "expected_cause": "job_ingest_customers", "label": "80% Customer Volume Drop"},
            {"scenario": "null_spike", "date": date(2026, 12, 5), "expected_cause": "job_ingest_orders", "label": "Revenue Null Spike (60%)"},
            {"scenario": "missing_file", "date": date(2026, 12, 6), "expected_cause": "job_ingest_orders", "label": "Missing Orders File"},
            {"scenario": "schema_drift", "date": date(2026, 12, 7), "expected_cause": "job_ingest_customers", "label": "Schema Drift (customer_id int->str)"},
            {"scenario": "row_drop", "date": date(2026, 12, 8), "expected_cause": "job_ingest_customers", "label": "Customer Volume Drop Trial 2"},
            {"scenario": "null_spike", "date": date(2026, 12, 9), "expected_cause": "job_ingest_orders", "label": "Revenue Null Spike Trial 2"},
            {"scenario": "missing_file", "date": date(2026, 12, 12), "expected_cause": "job_ingest_orders", "label": "Missing Orders File Trial 2"},
            {"scenario": "schema_drift", "date": date(2026, 12, 13), "expected_cause": "job_ingest_customers", "label": "Schema Drift Trial 2"},
            {"scenario": "row_drop", "date": date(2026, 12, 14), "expected_cause": "job_ingest_customers", "label": "Customer Volume Drop Trial 3"},
            {"scenario": "null_spike", "date": date(2026, 12, 15), "expected_cause": "job_ingest_orders", "label": "Revenue Null Spike Trial 3"},
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

            print(f"\n[Trial {idx:02d}/{len(trials)}] Testing: {label} ({scen}) on {t_date}...")

            # Clean any old test records for this date (respecting foreign key order)
            execute("DELETE FROM incidents WHERE run_date = %s", (t_date,))
            execute("DELETE FROM dataset_snapshots WHERE snapshot_date = %s", (t_date,))
            execute("DELETE FROM job_execution_logs WHERE run_date = %s", (t_date,))

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
                    print(f"  Result: [FAIL] False Positive! {len(incidents_created)} alert(s) triggered on clean data.")
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

                # ── Ground Truth Blast Radius Evaluation ─────────
                # Ground truth: the actual set of downstream datasets affected by this failure
                gt_blast = GROUND_TRUTH_BLAST.get(expected, set())

                # Predicted blast: what our RCA engine calculated for the top identified candidate
                top_candidate_info = inv.get("ranked_causes", [{}])[0]
                pred_blast = set(top_candidate_info.get("impacted_downstream", []))

                if pred_blast and gt_blast:
                    overlap = pred_blast.intersection(gt_blast)
                    prec = len(overlap) / len(pred_blast)
                    rec = len(overlap) / len(gt_blast)
                elif not pred_blast and not gt_blast:
                    prec, rec = 1.0, 1.0
                else:
                    prec, rec = 0.0, 0.0

                precision_scores.append(prec)
                recall_scores.append(rec)

                print(f"  Result: Top-1={top_1} (Expected={expected}) | Top-1 OK: {is_top1} | Blast Prec: {prec*100:.1f}%, Rec: {rec*100:.1f}% | Latency: {latency_ms:.1f}ms")

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
        print("FORMAL BENCHMARK EVALUATION SUMMARY (GROUND-TRUTH GROUNDED)")
        print("=" * 70)
        print(f"  Total Trials Evaluated          : {metrics['total_trials']} ({total_fault_trials} faults, {normal_trials} baselines)")
        print(f"  Top-1 Root Cause Accuracy       : {metrics['top1_accuracy_pct']}% ({top1_correct}/{total_fault_trials})")
        print(f"  Top-3 Root Cause Accuracy       : {metrics['top3_accuracy_pct']}% ({top3_correct}/{total_fault_trials})")
        print(f"  False Positive Rate (FPR)       : {metrics['false_positive_rate_pct']}% ({false_positives}/{normal_trials})")
        print(f"  Blast Radius Precision          : {metrics['blast_radius_precision_pct']}% (Evaluated vs Ground Truth DAG)")
        print(f"  Blast Radius Recall             : {metrics['blast_radius_recall_pct']}% (Evaluated vs Ground Truth DAG)")
        print(f"  Average Diagnosis Time          : {metrics['avg_diagnosis_time_ms']} ms")
        print("=" * 70)

        return metrics


if __name__ == "__main__":
    evaluator = SystemEvaluator()
    evaluator.run_benchmark()
