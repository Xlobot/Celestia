"""
Pipeline Root Cause Analysis & Recovery Assistant
==================================================
Operational Engineering Console
Run with:
    streamlit run src/dashboard/app.py
"""

import sys
import json
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all, execute

# ─────────────────────────────────────────────────────────────
# LAZY-LOADED COMPONENT FACTORIES (DEFENSIVE ERROR TRAPPING)
# ─────────────────────────────────────────────────────────────
_GRAPH_INSTANCE = None
_RCA_INSTANCE = None
_ASSISTANT_INSTANCE = None
_RECOVERY_INSTANCE = None


def get_graph():
    global _GRAPH_INSTANCE
    if _GRAPH_INSTANCE is None:
        from src.graph.pipeline_graph import PipelineGraph
        _GRAPH_INSTANCE = PipelineGraph()
    return _GRAPH_INSTANCE


def reload_graph():
    global _GRAPH_INSTANCE
    from src.graph.pipeline_graph import PipelineGraph
    _GRAPH_INSTANCE = PipelineGraph()
    return _GRAPH_INSTANCE


def get_rca():
    global _RCA_INSTANCE
    if _RCA_INSTANCE is None:
        from src.rca.engine import RootCauseEngine
        _RCA_INSTANCE = RootCauseEngine()
    return _RCA_INSTANCE


def get_assistant():
    global _ASSISTANT_INSTANCE
    if _ASSISTANT_INSTANCE is None:
        from src.llm.assistant import DiagnosticAssistant
        _ASSISTANT_INSTANCE = DiagnosticAssistant()
    return _ASSISTANT_INSTANCE


def get_recovery():
    global _RECOVERY_INSTANCE
    if _RECOVERY_INSTANCE is None:
        from src.recovery.recovery_engine import RecoveryEngine
        _RECOVERY_INSTANCE = RecoveryEngine()
    return _RECOVERY_INSTANCE


# ─────────────────────────────────────────────────────────────
# PAGE CONFIGURATION & QUIET DESIGN SYSTEM
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pipeline Reliability & RCA Console",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Refined, un-slopped dark theme CSS:
# - Graphite neutral ramp (#0d0f12, #15181e, #1c2028, #2a303c)
# - Subdued semantic indicators (4.5:1 contrast, no high-saturation blocks)
# - Monospace data alignment and quiet hairline borders
st.markdown("""
<style>
    /* Global layout & typography */
    .stApp {
        background-color: #0d0f12;
        color: #e2e8f0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    
    /* Quiet Header Bar */
    .app-header {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        padding-bottom: 12px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        margin-bottom: 20px;
    }
    .app-title {
        font-size: 20px;
        font-weight: 500;
        letter-spacing: -0.01em;
        color: #f8fafc;
        margin: 0;
    }
    .app-sub {
        font-size: 13px;
        color: #64748b;
        margin: 0;
    }

    /* KPI Summary Cards */
    .kpi-container {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
        margin-bottom: 24px;
    }
    .kpi-card {
        background-color: #15181e;
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 8px;
        padding: 14px 18px;
    }
    .kpi-label {
        font-size: 12px;
        font-weight: 400;
        color: #94a3b8;
        text-transform: none;
        margin-bottom: 4px;
    }
    .kpi-value {
        font-size: 24px;
        font-weight: 500;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        color: #f1f5f9;
        font-variant-numeric: tabular-nums;
    }
    .kpi-meta {
        font-size: 12px;
        color: #64748b;
        margin-top: 2px;
    }

    /* Status Badges */
    .pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 500;
        line-height: 1.4;
    }
    .pill-critical {
        background-color: rgba(239, 68, 68, 0.12);
        color: #fca5a5;
        border: 1px solid rgba(239, 68, 68, 0.25);
    }
    .pill-high {
        background-color: rgba(245, 158, 11, 0.12);
        color: #fcd34d;
        border: 1px solid rgba(245, 158, 11, 0.25);
    }
    .pill-medium {
        background-color: rgba(234, 179, 8, 0.1);
        color: #fef08a;
        border: 1px solid rgba(234, 179, 8, 0.2);
    }
    .pill-resolved {
        background-color: rgba(34, 197, 94, 0.12);
        color: #86efac;
        border: 1px solid rgba(34, 197, 94, 0.25);
    }
    .pill-neutral {
        background-color: rgba(148, 163, 184, 0.1);
        color: #cbd5e1;
        border: 1px solid rgba(148, 163, 184, 0.2);
    }

    /* Live Signature Dot */
    .status-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        display: inline-block;
    }
    .dot-green { background-color: #22c55e; }
    .dot-amber { background-color: #f59e0b; }
    .dot-red   { background-color: #ef4444; }

    /* Inspection Panels */
    .panel-box {
        background-color: #15181e;
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 8px;
        padding: 18px;
        margin-bottom: 16px;
    }
    .panel-header {
        font-size: 14px;
        font-weight: 500;
        color: #f1f5f9;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }
    .mono {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 13px;
        color: #cbd5e1;
    }
    .meta-label {
        font-size: 12px;
        color: #64748b;
    }
</style>
""", unsafe_allow_html=True)


def load_kpis():
    jobs_count = fetch_all("SELECT COUNT(*) as c FROM pipeline_jobs")[0]["c"]
    incidents_total = fetch_all("SELECT COUNT(*) as c FROM incidents")[0]["c"]
    incidents_open = fetch_all("SELECT COUNT(*) as c FROM incidents WHERE status IN ('open', 'investigating')")[0]["c"]
    incidents_resolved = fetch_all("SELECT COUNT(*) as c FROM incidents WHERE status = 'resolved'")[0]["c"]
    return jobs_count, incidents_total, incidents_open, incidents_resolved


# ─────────────────────────────────────────────────────────────
# HEADER & KPI STRIP
# ─────────────────────────────────────────────────────────────
jobs_count, inc_total, inc_open, inc_resolved = load_kpis()

st.markdown("""
<div class="app-header">
    <div>
        <h1 class="app-title">Pipeline Reliability & Root Cause Console</h1>
        <p class="app-sub">Lineage ancestry tracing, statistical anomaly detection, and human-in-the-loop recovery</p>
    </div>
    <div style="font-family: ui-monospace, monospace; font-size: 12px; color: #64748b;">
        <span class="status-dot dot-green"></span> Engine Online · UTC
    </div>
</div>
""", unsafe_allow_html=True)

# 4-card metric strip
st.markdown(f"""
<div class="kpi-container">
    <div class="kpi-card">
        <div class="kpi-label">Registered DAG Jobs</div>
        <div class="kpi-value">{jobs_count}</div>
        <div class="kpi-meta">3 active pipeline tasks</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Total Recorded Incidents</div>
        <div class="kpi-value">{inc_total}</div>
        <div class="kpi-meta">Historical audit log</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Active / In Triage</div>
        <div class="kpi-value" style="color: {'#fca5a5' if inc_open > 0 else '#86efac'};">{inc_open}</div>
        <div class="kpi-meta">{'Requires investigation' if inc_open > 0 else 'All pipelines nominal'}</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Resolved Incidents</div>
        <div class="kpi-value" style="color: #86efac;">{inc_resolved}</div>
        <div class="kpi-meta">Verified post-recovery</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# CLEAN TAB NAVIGATION
# ─────────────────────────────────────────────────────────────
tab_incidents, tab_graph, tab_runs, tab_simulator = st.tabs([
    "Incidents & Remediation",
    "Lineage DAG",
    "Execution Logs",
    "Fault Simulator"
])


# ═════════════════════════════════════════════════════════════
# TAB 1: INCIDENT TRIAGE & RECOVERY
# ═════════════════════════════════════════════════════════════
with tab_incidents:
    st.markdown("### Incident Triage Console")

    filter_status = st.selectbox(
        "Filter by status",
        options=["Active (open / investigating)", "Resolved", "All records"],
        index=0,
        label_visibility="collapsed"
    )

    query = """
        SELECT incident_id, run_date, trigger_type, severity, status,
               affected_datasets, ranked_causes, llm_explanation, llm_recommendation,
               resolved_by, resolution_notes, detected_at
        FROM incidents
    """
    if filter_status == "Active (open / investigating)":
        query += " WHERE status IN ('open', 'investigating')"
    elif filter_status == "Resolved":
        query += " WHERE status = 'resolved'"

    query += " ORDER BY incident_id DESC"
    incidents = fetch_all(query)

    if not incidents:
        st.markdown("""
        <div style="background-color: #15181e; border: 1px solid rgba(255, 255, 255, 0.07); border-radius: 8px; padding: 24px; text-align: center; color: #94a3b8;">
            No active incidents match this filter. All monitored datasets are operating within expected baselines.
        </div>
        """, unsafe_allow_html=True)
    else:
        # Clean dropdown format without shouty brackets
        incident_options = {}
        for r in incidents:
            label = f"Incident #{r['incident_id']} · {r['run_date']} · {r['trigger_type'].replace('_', ' ')} ({r['status'].capitalize()})"
            incident_options[label] = r["incident_id"]

        selected_label = st.selectbox("Select incident to investigate", options=list(incident_options.keys()))
        selected_id = incident_options[selected_label]
        sel_inc = next(r for r in incidents if r["incident_id"] == selected_id)

        # Incident status summary banner
        status_pill_class = "pill-resolved" if sel_inc["status"] == "resolved" else "pill-critical"
        sev_pill_class = f"pill-{sel_inc['severity'].lower()}" if sel_inc['severity'].lower() in ['critical', 'high', 'medium'] else "pill-neutral"

        # Parse affected datasets
        aff_ds = sel_inc.get("affected_datasets")
        if isinstance(aff_ds, str):
            try:
                aff_ds = json.loads(aff_ds)
            except Exception:
                aff_ds = [aff_ds]
        aff_str = ", ".join(aff_ds) if isinstance(aff_ds, list) else str(aff_ds)

        st.markdown(f"""
        <div style="display: flex; gap: 24px; align-items: center; background-color: #15181e; border: 1px solid rgba(255, 255, 255, 0.07); border-radius: 8px; padding: 12px 18px; margin: 12px 0 20px 0;">
            <div><span class="meta-label">Incident ID</span><br><span class="mono">#{sel_inc['incident_id']}</span></div>
            <div><span class="meta-label">Run Date</span><br><span class="mono">{sel_inc['run_date']}</span></div>
            <div><span class="meta-label">Trigger</span><br><span class="mono">{sel_inc['trigger_type']}</span></div>
            <div><span class="meta-label">Affected Datasets</span><br><span class="mono">{aff_str}</span></div>
            <div><span class="meta-label">Severity</span><br><span class="pill {sev_pill_class}">{sel_inc['severity'].capitalize()}</span></div>
            <div><span class="meta-label">Status</span><br><span class="pill {status_pill_class}">{sel_inc['status'].capitalize()}</span></div>
        </div>
        """, unsafe_allow_html=True)

        col_left, col_right = st.columns([3, 2], gap="large")

        # ── Left Column: Forensic Diagnostics ─────────────
        with col_left:
            st.markdown("#### Diagnostic Investigation")

            # Display Grounded Ranked Causes
            ranked = sel_inc.get("ranked_causes")
            if isinstance(ranked, str):
                try:
                    ranked = json.loads(ranked)
                except Exception:
                    ranked = []

            if ranked and isinstance(ranked, list):
                st.markdown("**Identified Root Causes (Lineage & Evidence Scoring):**")
                for idx, cause in enumerate(ranked, start=1):
                    job_name = cause.get("job_id", "unknown")
                    score = cause.get("score", 0)
                    evidence = cause.get("evidence", [])
                    downstream = cause.get("impacted_downstream", [])

                    st.markdown(f"""
                    <div style="background-color: #181c24; border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 6px; padding: 12px 16px; margin-bottom: 10px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                            <span class="mono" style="font-weight: 500; color: #f8fafc;">#{idx} {job_name}</span>
                            <span class="pill pill-neutral" style="font-family: ui-monospace, monospace;">Score: {score} / 100</span>
                        </div>
                        <div style="font-size: 13px; color: #94a3b8; margin-bottom: 6px;">
                            <strong>Downstream blast radius:</strong> <span class="mono">{', '.join(downstream) if downstream else 'None'}</span>
                        </div>
                        <div style="font-size: 12px; color: #64748b;">
                            {'; '.join(evidence) if evidence else 'Ranked by dependency ancestry'}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

            explanation = sel_inc.get("llm_explanation")
            recommendation = sel_inc.get("llm_recommendation")

            if st.button("Generate Diagnostic Briefing", key=f"btn_ai_{selected_id}"):
                with st.spinner("Compiling DAG lineage, execution metadata, and telemetry facts..."):
                    asst = get_assistant()
                    res = asst.generate_explanation(selected_id)
                    explanation = res["explanation"]
                    recommendation = res["recommendation"]
                    st.rerun()

            if explanation:
                st.markdown("---")
                st.markdown(explanation)
                if recommendation:
                    st.markdown(recommendation)
            else:
                st.caption("No written briefing compiled yet. Click above to synthesize evidence.")

        # ── Right Column: Controlled Remediation ──────────
        with col_right:
            st.markdown("#### Remediation & Recovery")
            recovery = get_recovery()
            plan = recovery.generate_recovery_plan(selected_id)

            action = plan.get("action", "INVESTIGATE_MANUALLY")
            target_job = plan.get("target_job", "N/A")
            exec_order = plan.get("execution_order", [])
            is_safe = plan.get("is_safe_to_execute", False)

            st.markdown(f"""
            <div style="background-color: #181c24; border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 6px; padding: 14px; margin-bottom: 16px;">
                <div style="margin-bottom: 8px;"><span class="meta-label">Action Strategy</span><br><span class="mono">{action}</span></div>
                <div style="margin-bottom: 8px;"><span class="meta-label">Target Task</span><br><span class="mono">{target_job}</span></div>
                <div style="margin-bottom: 8px;"><span class="meta-label">Execution Order</span><br><span class="mono">{' → '.join(exec_order) if exec_order else 'None'}</span></div>
                <div><span class="meta-label">Precondition Assessment</span><br>
                    <span class="pill {'pill-resolved' if is_safe else 'pill-critical'}">
                        {'Preconditions Satisfied' if is_safe else 'Execution Blocked'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)

            if plan.get("validation_errors"):
                for err in plan["validation_errors"]:
                    st.caption(f"Blocked: {err}")

            if plan.get("validation_warnings"):
                for warn in plan["validation_warnings"]:
                    st.caption(f"Notice: {warn}")

            if sel_inc["status"] == "resolved":
                st.markdown(f"""
                <div style="background-color: rgba(34, 197, 94, 0.08); border: 1px solid rgba(34, 197, 94, 0.2); border-radius: 6px; padding: 12px; font-size: 13px; color: #86efac;">
                    Incident marked resolved by <strong>{sel_inc.get('resolved_by', 'system')}</strong>.<br>
                    <span style="color: #64748b;">{sel_inc.get('resolution_notes', 'Verification checks passed.')}</span>
                </div>
                """, unsafe_allow_html=True)
            else:
                approver = st.text_input("Approver email", value="oncall_engineer@company.com")
                
                if st.button("Execute Recovery Action", type="primary", disabled=not is_safe):
                    with st.spinner("Executing sequential recovery re-runs and post-verification..."):
                        rec_res = recovery.execute_recovery(selected_id, approved_by=approver)
                        if rec_res.get("is_resolved"):
                            st.success("Recovery succeeded. Downstream datasets restored and verified.")
                            st.rerun()
                        else:
                            st.error(f"Recovery executed but health checks detected remaining issues: {rec_res.get('notes')}")


# ═════════════════════════════════════════════════════════════
# TAB 2: PIPELINE LINEAGE DAG
# ═════════════════════════════════════════════════════════════
with tab_graph:
    st.markdown("### Pipeline Lineage Architecture")
    st.caption("Bipartite directed acyclic graph mapping transformation jobs and dataset states.")

    col_btn, col_empty = st.columns([1, 4])
    with col_btn:
        if st.button("Refresh Lineage"):
            pg = reload_graph()
        else:
            pg = get_graph()

    # Visual Mermaid Diagram rendered natively via Mermaid.js
    mermaid_code = pg.to_mermaid()
    html_code = f"""
    <div style="background-color: #15181e; border: 1px solid rgba(255, 255, 255, 0.07); border-radius: 8px; padding: 20px; text-align: center;">
        <pre class="mermaid" style="background: transparent; color: #e2e8f0;">
{mermaid_code}
        </pre>
    </div>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'dark',
            themeVariables: {{
                darkMode: true,
                background: '#15181e',
                primaryColor: '#1e293b',
                primaryTextColor: '#f8fafc',
                primaryBorderColor: '#475569',
                lineColor: '#64748b'
            }}
        }});
    </script>
    """
    components.html(html_code, height=360, scrolling=False)

    st.markdown("---")
    st.markdown("#### Lineage Node Inspector")

    node_options = sorted(list(pg.graph.nodes()))
    node_selected = st.selectbox("Select node to trace dependencies", options=node_options)

    col_anc, col_desc = st.columns(2, gap="large")

    with col_anc:
        st.markdown(f"**Upstream Ancestors of `{node_selected}`:**")
        ancestors = pg.get_upstream_ancestors(node_selected)
        if ancestors:
            for a in ancestors:
                st.markdown(f"- <span class='mono'>{a}</span>", unsafe_allow_html=True)
        else:
            st.caption("None (root input source)")

    with col_desc:
        st.markdown(f"**Downstream Blast Radius of `{node_selected}`:**")
        impact = pg.get_downstream_impact(node_selected)
        if impact:
            for d in impact:
                st.markdown(f"- <span class='mono'>{d}</span>", unsafe_allow_html=True)
        else:
            st.caption("None (terminal pipeline dataset)")


# ═════════════════════════════════════════════════════════════
# TAB 3: EXECUTION LOGS
# ═════════════════════════════════════════════════════════════
with tab_runs:
    st.markdown("### Job Execution Records")
    st.caption("Telemetry logs recording execution duration, input/output row counts, and error types.")

    logs = fetch_all("""
        SELECT execution_id, job_id, run_date, status, rows_read, rows_written,
               duration_seconds, error_type, started_at
        FROM job_execution_logs
        ORDER BY execution_id DESC
        LIMIT 50
    """)
    if logs:
        df_logs = pd.DataFrame(logs)
        st.dataframe(df_logs, use_container_width=True, hide_index=True)
    else:
        st.caption("No execution logs recorded.")


# ═════════════════════════════════════════════════════════════
# TAB 4: FAULT SIMULATOR
# ═════════════════════════════════════════════════════════════
with tab_simulator:
    st.markdown("### Controlled Failure Simulation")
    st.caption("Inject controlled anomalies into raw data to benchmark detection and recovery.")

    sim_col1, sim_col2 = st.columns([1, 2], gap="large")

    with sim_col1:
        sim_date = st.date_input("Target Date", value=date.today())
        sim_scenario = st.selectbox(
            "Fault Scenario",
            options=["normal", "row_drop", "schema_drift", "null_spike", "duplicate", "missing_file"],
            index=0
        )

    with sim_col2:
        descriptions = {
            "normal": "Clean baseline data. All rows, null rates, and schemas adhere to SLAs.",
            "row_drop": "Drops 80% of customer records to simulate upstream ingestion failure.",
            "schema_drift": "Mutates customer_id from integer to string ('CUST_1234') to simulate contract drift.",
            "null_spike": "Injects 60% null values into order revenue column to simulate currency service failure.",
            "duplicate": "Duplicates 30% of customer records to simulate double-processing errors.",
            "missing_file": "Omits the raw orders file to simulate source delivery failure."
        }
        st.markdown(f"**Scenario Impact:** {descriptions[sim_scenario]}")

        if st.button("Execute Pipeline Run", type="primary"):
            with st.spinner("Generating scenario data and executing pipeline jobs..."):
                from data.generator import generate_data
                from pipelines.run_pipeline import run_pipeline
                generate_data(scenario=sim_scenario, run_date=sim_date, seed=99)
                run_res = run_pipeline(sim_date)
                st.success(f"Pipeline executed. Overall status: {run_res['overall']}")
                st.rerun()
