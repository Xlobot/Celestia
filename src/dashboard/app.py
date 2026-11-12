"""
Data Pipeline Root Cause Analysis & Recovery Assistant
======================================================
Interactive Streamlit Dashboard
Run with:
    streamlit run src/dashboard/app.py
"""

import sys
import json
import streamlit as st
import pandas as pd
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute
from src.graph.pipeline_graph import PipelineGraph
from src.rca.engine import RootCauseEngine
from src.llm.assistant import DiagnosticAssistant
from src.recovery.recovery_engine import RecoveryEngine
from pipelines.run_pipeline import run_pipeline

# ─────────────────────────────────────────────────────────────
# PAGE CONFIGURATION
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pipeline RCA & Recovery Assistant",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .metric-card {
        background-color: #1E232F;
        padding: 18px;
        border-radius: 10px;
        border: 1px solid #2B3245;
        text-align: center;
    }
    .badge-critical { background-color: #FF4B4B; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-high { background-color: #FFA500; color: black; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-medium { background-color: #F0E68C; color: black; padding: 3px 8px; border-radius: 4px; }
    .badge-resolved { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_graph():
    return PipelineGraph()

@st.cache_resource
def get_rca():
    return RootCauseEngine()

@st.cache_resource
def get_assistant():
    return DiagnosticAssistant()

@st.cache_resource
def get_recovery():
    return RecoveryEngine()


def load_kpis():
    jobs_count = fetch_all("SELECT COUNT(*) as c FROM pipeline_jobs")[0]["c"]
    incidents_total = fetch_all("SELECT COUNT(*) as c FROM incidents")[0]["c"]
    incidents_open = fetch_all("SELECT COUNT(*) as c FROM incidents WHERE status IN ('open', 'investigating')")[0]["c"]
    incidents_resolved = fetch_all("SELECT COUNT(*) as c FROM incidents WHERE status = 'resolved'")[0]["c"]
    return jobs_count, incidents_total, incidents_open, incidents_resolved


# ─────────────────────────────────────────────────────────────
# HEADER & SIDEBAR
# ─────────────────────────────────────────────────────────────
st.title("🔍 Data Pipeline Root Cause Analysis & Recovery Assistant")
st.caption("Grounded Lineage Tracing · ML Anomaly Detection · AI Diagnostics · Controlled Human-in-the-Loop Recovery")

jobs_count, inc_total, inc_open, inc_resolved = load_kpis()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Monitored Jobs", f"{jobs_count} DAG Jobs")
with col2:
    st.metric("Total Incidents", f"{inc_total} Detected")
with col3:
    st.metric("Active / Open", f"{inc_open} Requiring Action", delta=f"-{inc_open}" if inc_open > 0 else "0", delta_color="inverse")
with col4:
    st.metric("Resolved Incidents", f"{inc_resolved} Restored", delta=f"+{inc_resolved}")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────
tab_incidents, tab_graph, tab_runs, tab_simulator = st.tabs([
    "🚨 Incident Triage & AI Diagnostics",
    "🕸️ Pipeline Lineage DAG",
    "📊 Job Execution Logs",
    "🧪 Fault Simulator & Runner"
])


# ═════════════════════════════════════════════════════════════
# TAB 1: INCIDENT TRIAGE & RECOVERY
# ═════════════════════════════════════════════════════════════
with tab_incidents:
    st.subheader("🚨 Incident Management & Remediation Console")

    filter_status = st.selectbox(
        "Filter by Status",
        options=["All", "open / investigating", "resolved"],
        index=1
    )

    query = """
        SELECT incident_id, run_date, trigger_type, severity, status,
               affected_datasets, ranked_causes, llm_explanation, llm_recommendation,
               resolved_by, resolution_notes, detected_at
        FROM incidents
    """
    if filter_status == "open / investigating":
        query += " WHERE status IN ('open', 'investigating')"
    elif filter_status == "resolved":
        query += " WHERE status = 'resolved'"

    query += " ORDER BY incident_id DESC"
    incidents = fetch_all(query)

    if not incidents:
        st.success("🎉 No active incidents match the current filter. All monitored datasets are healthy!")
    else:
        # Display incident selection
        incident_options = {
            f"#{r['incident_id']} | {r['run_date']} | [{r['severity'].upper()}] {r['trigger_type'].replace('_', ' ').title()} ({r['status'].upper()})": r["incident_id"]
            for r in incidents
        }
        selected_label = st.selectbox("Select an Incident to Investigate:", options=list(incident_options.keys()))
        selected_id = incident_options[selected_label]

        # Fetch full selected record
        sel_inc = next(r for r in incidents if r["incident_id"] == selected_id)

        st.markdown(f"### Incident Details: #{sel_inc['incident_id']}")
        detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
        detail_col1.write(f"**Date:** `{sel_inc['run_date']}`")
        detail_col2.write(f"**Trigger:** `{sel_inc['trigger_type']}`")
        detail_col3.write(f"**Severity:** `{sel_inc['severity'].upper()}`")
        detail_col4.write(f"**Status:** `{sel_inc['status'].upper()}`")

        # Two-column layout: Investigation & Recovery
        col_left, col_right = st.columns([3, 2])

        with col_left:
            st.markdown("#### 🧠 AI Diagnostic Assistant")
            
            # Action: Generate or display explanation
            explanation = sel_inc.get("llm_explanation")
            recommendation = sel_inc.get("llm_recommendation")

            if st.button("🤖 Generate / Refresh AI Diagnostic Briefing", key=f"btn_ai_{selected_id}"):
                with st.spinner("Analyzing DAG lineage, historical runs, and evidence package..."):
                    asst = get_assistant()
                    res = asst.generate_explanation(selected_id)
                    explanation = res["explanation"]
                    recommendation = res["recommendation"]
                    st.success("AI Diagnostic Briefing Generated & Saved!")

            if explanation:
                st.markdown(explanation)
                if recommendation:
                    st.markdown(recommendation)
            else:
                st.info("Click 'Generate AI Diagnostic Briefing' above to synthesize a grounded report for this incident.")

        with col_right:
            st.markdown("#### 🛠️ Controlled Recovery Console")
            recovery = get_recovery()

            plan = recovery.generate_recovery_plan(selected_id)

            st.write(f"**Recommended Action:** `{plan.get('action')}`")
            st.write(f"**Target Pipeline Job:** `{plan.get('target_job')}`")
            st.write(f"**Execution Sequence:** `{' -> '.join(plan.get('execution_order', []))}`")
            st.write(f"**Safety Preconditions:** {'✅ PASSED' if plan.get('is_safe_to_execute') else '❌ BLOCKED'}")

            if plan.get("validation_errors"):
                for err in plan["validation_errors"]:
                    st.error(err)

            if plan.get("validation_warnings"):
                for warn in plan["validation_warnings"]:
                    st.warning(warn)

            if sel_inc["status"] == "resolved":
                st.success(f"✅ Incident is already RESOLVED.\n\n**Notes:** {sel_inc.get('resolution_notes', 'N/A')}")
            else:
                approver = st.text_input("Human Approver Email", value="oncall_engineer@company.com")
                
                if st.button("⚡ Approve & Execute Recovery Plan", type="primary", disabled=not plan.get("is_safe_to_execute")):
                    with st.spinner("Executing cascading job re-runs and post-verification..."):
                        rec_res = recovery.execute_recovery(selected_id, approved_by=approver)
                        if rec_res.get("is_resolved"):
                            st.success(f"🎉 Recovery Succeeded! Pipeline restored and incident marked RESOLVED.")
                            st.rerun()
                        else:
                            st.error(f"Recovery executed but health checks still detect issues: {rec_res.get('notes')}")


# ═════════════════════════════════════════════════════════════
# TAB 2: PIPELINE LINEAGE DAG
# ═════════════════════════════════════════════════════════════
with tab_graph:
    st.subheader("🕸️ Pipeline Lineage & Bipartite DAG")
    st.write("Models both **Jobs** (actions) and **Datasets** (state) to enable upstream root cause tracing and downstream blast radius calculation.")

    pg = get_graph()

    st.markdown("#### Interactive Mermaid Architecture Lineage")
    st.code(pg.to_mermaid(), language="mermaid")

    st.markdown("---")
    st.markdown("#### 🔍 Lineage & Blast Radius Inspector")
    node_selected = st.selectbox("Inspect Node:", options=sorted(list(pg.graph.nodes())))

    inspect_col1, inspect_col2 = st.columns(2)
    with inspect_col1:
        st.markdown(f"**Upstream Ancestors of `{node_selected}`:**")
        ancestors = pg.get_upstream_ancestors(node_selected)
        if ancestors:
            for a in ancestors:
                st.write(f"- `{a}`")
        else:
            st.write("*(None — this is a root source input)*")

    with inspect_col2:
        st.markdown(f"**Downstream Blast Radius of `{node_selected}`:**")
        impact = pg.get_downstream_impact(node_selected)
        if impact:
            for d in impact:
                st.write(f"- `{d}`")
        else:
            st.write("*(None — this is an end-of-pipeline output)*")


# ═════════════════════════════════════════════════════════════
# TAB 3: JOB EXECUTION LOGS
# ═════════════════════════════════════════════════════════════
with tab_runs:
    st.subheader("📊 Pipeline Execution Records & ML Scores")
    
    logs = fetch_all("""
        SELECT execution_id, job_id, run_date, status, rows_read, rows_written,
               duration_seconds, error_type, started_at
        FROM job_execution_logs
        ORDER BY execution_id DESC
        LIMIT 50
    """)
    if logs:
        df_logs = pd.DataFrame(logs)
        st.dataframe(df_logs, use_container_width=True)
    else:
        st.info("No execution logs recorded yet.")


# ═════════════════════════════════════════════════════════════
# TAB 4: FAULT SIMULATOR
# ═════════════════════════════════════════════════════════════
with tab_simulator:
    st.subheader("🧪 On-Demand Fault Injector & Pipeline Runner")
    st.write("Trigger real pipeline runs on-demand with clean or deliberately broken data to test the assistant.")

    sim_col1, sim_col2 = st.columns(2)
    with sim_col1:
        sim_date = st.date_input("Run Date", value=date.today())
        sim_scenario = st.selectbox(
            "Fault Scenario",
            options=["normal", "row_drop", "schema_drift", "null_spike", "duplicate", "missing_file"],
            index=0
        )

    with sim_col2:
        st.write("**Scenario Description:**")
        descriptions = {
            "normal": "Clean baseline data. No anomalies.",
            "row_drop": "Drops 80% of customer rows (partial load failure).",
            "schema_drift": "Mutates customer_id from integer to string ('CUST_123').",
            "null_spike": "Injects 60% null values into order revenue column.",
            "duplicate": "Duplicates 30% of customer rows (double load error).",
            "missing_file": "Deletes the raw orders file to trigger missing source error."
        }
        st.info(descriptions[sim_scenario])

        if st.button("🚀 Run Pipeline with Scenario", type="primary"):
            with st.spinner("Generating data and executing pipeline jobs..."):
                from data.generator import generate_data
                generate_data(scenario=sim_scenario, run_date=sim_date, seed=99)
                run_res = run_pipeline(sim_date)
                st.success(f"Pipeline executed! Result: {run_res['overall']}")
                st.rerun()

