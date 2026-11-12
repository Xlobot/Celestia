"""
Isolation Forest Detector for Job Execution Metadata
====================================================
Uses Scikit-Learn's Isolation Forest to identify anomalous pipeline job executions.
Unlike univariate statistical thresholds (IQR), Isolation Forest examines
multi-dimensional correlations:
  - duration_seconds
  - rows_read
  - rows_written
  - processing_speed (rows per second)

Why this matters in Root Cause Analysis:
  A job may not throw an unhandled exception (it reports 'success'), but its execution
  dynamics (e.g. 10x slower processing speed or abnormal read/write ratio)
  strongly signal an upstream bottleneck or hidden degradation.
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all


class JobExecutionAnomalyDetector:
    """
    Fits an Isolation Forest model per job_id based on historical execution logs.
    """

    def __init__(self, contamination: float = 0.05):
        """
        contamination: The expected proportion of outliers in the data (default: 5%)
        """
        self.contamination = contamination

    def get_job_history(self, job_id: str, before_date: Optional[str] = None) -> pd.DataFrame:
        """Loads historical execution records for a job."""
        query = """
            SELECT execution_id, job_id, run_date, status,
                   rows_read, rows_written, duration_seconds
            FROM job_execution_logs
            WHERE job_id = %s
        """
        params = [job_id]
        if before_date:
            query += " AND run_date <= %s"
            params.append(before_date)

        query += " ORDER BY run_date ASC"

        rows = fetch_all(query, tuple(params))
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        # Handle 0-second durations safely for division
        df["duration_safe"] = df["duration_seconds"].apply(lambda x: max(float(x or 1), 0.5))
        df["throughput"] = df["rows_written"] / df["duration_safe"]
        return df

    def detect_anomalies_for_job(self, job_id: str, min_samples: int = 10) -> List[Dict[str, Any]]:
        """
        Trains an Isolation Forest on the historical runs of `job_id`
        and identifies any anomalous executions.
        """
        df = self.get_job_history(job_id)
        if len(df) < min_samples:
            return []

        # Feature matrix: multi-dimensional profile of execution
        features = ["rows_written", "duration_safe", "throughput"]
        X = df[features].fillna(0).values

        # Initialize and train Isolation Forest
        # random_state ensures reproducible anomaly scores
        iso = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )
        
        # -1 = anomaly, 1 = normal
        predictions = iso.fit_predict(X)
        # Anomaly score: the lower (more negative), the more abnormal
        decision_scores = iso.decision_function(X)

        df["is_anomaly"] = predictions == -1
        df["anomaly_score"] = decision_scores

        anomalies = []
        for _, row in df[df["is_anomaly"]].iterrows():
            anomalies.append({
                "execution_id": row["execution_id"],
                "job_id": row["job_id"],
                "run_date": str(row["run_date"]),
                "status": row["status"],
                "rows_written": int(row["rows_written"]),
                "duration_seconds": int(row["duration_seconds"] or 0),
                "throughput": round(float(row["throughput"]), 1),
                "anomaly_score": round(float(row["anomaly_score"]), 4),
                "detail": (
                    f"Job '{job_id}' on {row['run_date']} had anomalous execution metrics: "
                    f"{row['rows_written']:,} rows in {row['duration_seconds']}s "
                    f"({row['throughput']:.1f} rows/s, score: {row['anomaly_score']:.4f})"
                )
            })

        return anomalies

    def evaluate_all_jobs(self) -> Dict[str, List[Dict[str, Any]]]:
        """Runs anomaly detection across all registered jobs."""
        jobs = fetch_all("SELECT DISTINCT job_id FROM pipeline_jobs")
        results = {}
        for j in jobs:
            job_id = j["job_id"]
            anomalies = self.detect_anomalies_for_job(job_id)
            if anomalies:
                results[job_id] = anomalies
        return results


# ─────────────────────────────────────────────────────────────
# Self-test if executed directly
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("TESTING ISOLATION FOREST JOB ANOMALY DETECTOR")
    print("=" * 60)

    detector = JobExecutionAnomalyDetector(contamination=0.08)
    all_anomalies = detector.evaluate_all_jobs()

    if not all_anomalies:
        print("No job execution anomalies detected across all historical runs.")
    else:
        for job_id, anomalies in all_anomalies.items():
            print(f"\nJob: {job_id} ({len(anomalies)} anomalies found)")
            for a in anomalies:
                print(f"  [{a['run_date']}] Status: {a['status']:<15} | Rows: {a['rows_written']:<7} | Score: {a['anomaly_score']}")
                print(f"    Detail: {a['detail']}")

