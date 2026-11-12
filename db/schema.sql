-- ============================================================
-- Pipeline RCA System - Database Schema
-- ============================================================
-- Run order matters: pipeline_jobs must exist before
-- job_execution_logs (foreign key dependency).
-- ============================================================

-- ── Drop and recreate cleanly (safe for dev) ─────────────────
DROP TABLE IF EXISTS incidents          CASCADE;
DROP TABLE IF EXISTS dataset_snapshots  CASCADE;
DROP TABLE IF EXISTS job_execution_logs CASCADE;
DROP TABLE IF EXISTS pipeline_jobs      CASCADE;


-- ============================================================
-- TABLE 1: pipeline_jobs
-- The registry of all pipeline jobs in the system.
-- You register Job A, Job B, Job C here ONCE.
-- Think of it as the "definition" - not the run history.
-- ============================================================
CREATE TABLE pipeline_jobs (
    job_id              VARCHAR(100)    PRIMARY KEY,
    -- e.g. 'job_ingest_customers', 'job_transform_orders'

    job_name            VARCHAR(200)    NOT NULL,
    description         TEXT,

    -- JSON arrays of dataset names this job reads from / writes to
    -- e.g. '["raw.customers"]' or '["clean.customers", "clean.orders"]'
    input_datasets      JSONB           NOT NULL DEFAULT '[]',
    output_datasets     JSONB           NOT NULL DEFAULT '[]',

    owner               VARCHAR(100),   -- team or person responsible
    sla_max_duration_sec INT,           -- if job takes longer than this → alert
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE pipeline_jobs IS
    'Registry of all pipeline job definitions. One row per job type, not per run.';


-- ============================================================
-- TABLE 2: job_execution_logs  ← THE MOST IMPORTANT TABLE
-- Every single run of every job gets one row here.
-- This IS the evidence the root cause engine investigates.
-- ============================================================
CREATE TABLE job_execution_logs (
    execution_id        BIGSERIAL       PRIMARY KEY,

    job_id              VARCHAR(100)    NOT NULL
                        REFERENCES pipeline_jobs(job_id),
    -- which job ran?

    run_date            DATE            NOT NULL,
    -- the logical date this run was for (not wall-clock start time)

    started_at          TIMESTAMPTZ     NOT NULL,
    ended_at            TIMESTAMPTZ,    -- NULL if still running / crashed

    duration_seconds    INT
                        GENERATED ALWAYS AS (
                            EXTRACT(EPOCH FROM (ended_at - started_at))::INT
                        ) STORED,
    -- auto-calculated from start/end times

    status              VARCHAR(50)     NOT NULL
                        CHECK (status IN (
                            'running',
                            'success',
                            'failed',
                            'partial_load_warning',
                            'skipped',
                            'timeout'
                        )),

    rows_read           BIGINT          DEFAULT 0,
    rows_written        BIGINT          DEFAULT 0,

    -- NULL means no error. Populated on failure.
    error_message       TEXT,
    error_type          VARCHAR(100),
    -- e.g. 'FileNotFoundError', 'SchemaValidationError', 'TimeoutError'

    log_reference       VARCHAR(500),
    -- path to the full log file for this run

    schema_fingerprint  VARCHAR(64),
    -- MD5 hash of the output schema - if this changes unexpectedly → schema drift

    input_row_counts    JSONB           DEFAULT '{}',
    -- e.g. {"raw.customers": 10273, "raw.orders": 46537}

    output_row_counts   JSONB           DEFAULT '{}',
    -- e.g. {"clean.customers": 10200}

    extra_metadata      JSONB           DEFAULT '{}',
    -- catch-all for any other useful info

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE job_execution_logs IS
    'One row per job run. This is the primary evidence source for root cause analysis.';

-- Index for fast lookups by date and job
CREATE INDEX idx_execlog_run_date ON job_execution_logs(run_date DESC);
CREATE INDEX idx_execlog_job_date ON job_execution_logs(job_id, run_date DESC);
CREATE INDEX idx_execlog_status   ON job_execution_logs(status);


-- ============================================================
-- TABLE 3: dataset_snapshots
-- Daily health profile of each dataset.
-- Written by the monitoring layer after each pipeline run.
-- Used to detect: row count anomalies, null spikes, schema drift.
-- ============================================================
CREATE TABLE dataset_snapshots (
    snapshot_id         BIGSERIAL       PRIMARY KEY,

    dataset_name        VARCHAR(200)    NOT NULL,
    -- e.g. 'raw.customers', 'clean.orders', 'analytics.customer_sales_summary'

    snapshot_date       DATE            NOT NULL,
    row_count           BIGINT          NOT NULL,
    is_weekend          BOOLEAN         NOT NULL DEFAULT FALSE,
    -- stored so anomaly detection can compare weekdays to weekdays

    -- Null rate per column, stored as JSON
    -- e.g. {"revenue": 0.596, "customer_id": 0.0, "email": 0.02}
    null_rates          JSONB           NOT NULL DEFAULT '{}',

    -- Schema fingerprint: MD5 of sorted column names + types
    -- If this changes day-to-day → schema drift
    schema_fingerprint  VARCHAR(64),
    schema_snapshot     JSONB           NOT NULL DEFAULT '{}',
    -- e.g. {"customer_id": "int64", "name": "object", "revenue": "float64"}

    file_path           VARCHAR(500),
    file_size_kb        NUMERIC(12, 2),

    -- Which job execution produced this snapshot?
    execution_id        BIGINT          REFERENCES job_execution_logs(execution_id),

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- One snapshot per dataset per day
    UNIQUE (dataset_name, snapshot_date)
);

COMMENT ON TABLE dataset_snapshots IS
    'Daily profile of each dataset: row counts, null rates, schema. Used for anomaly detection baselines.';

CREATE INDEX idx_snapshot_dataset_date ON dataset_snapshots(dataset_name, snapshot_date DESC);


-- ============================================================
-- TABLE 4: incidents
-- When the monitoring system detects an anomaly, it opens
-- an incident here. The root cause engine then fills in
-- the ranked probable causes.
-- ============================================================
CREATE TABLE incidents (
    incident_id         BIGSERIAL       PRIMARY KEY,

    detected_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    run_date            DATE            NOT NULL,

    -- What triggered this incident?
    trigger_type        VARCHAR(100)    NOT NULL
                        CHECK (trigger_type IN (
                            'row_count_anomaly',
                            'schema_drift',
                            'freshness_violation',
                            'null_rate_spike',
                            'duplicate_records',
                            'missing_file',
                            'job_failure',
                            'referential_integrity'
                        )),

    severity            VARCHAR(20)     NOT NULL
                        CHECK (severity IN ('low', 'medium', 'high', 'critical'))
                        DEFAULT 'medium',

    -- Which datasets are showing symptoms?
    affected_datasets   JSONB           NOT NULL DEFAULT '[]',
    -- e.g. ["analytics.customer_sales_summary", "clean.orders"]

    -- Ranked probable root causes (filled by RCA engine)
    -- Each entry: {"rank": 1, "job_id": "job_ingest_customers",
    --              "evidence": "...", "evidence_score": 8}
    ranked_causes       JSONB           DEFAULT '[]',

    -- The evidence package sent to the LLM
    evidence_package    JSONB           DEFAULT '{}',

    -- LLM's explanation (filled in Week 5)
    llm_explanation     TEXT,
    llm_recommendation  TEXT,

    -- Human-approved recovery
    status              VARCHAR(50)     NOT NULL
                        CHECK (status IN (
                            'open',
                            'investigating',
                            'resolved',
                            'false_positive'
                        ))
                        DEFAULT 'open',

    resolution_notes    TEXT,
    resolved_at         TIMESTAMPTZ,
    resolved_by         VARCHAR(100),

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE incidents IS
    'One row per detected anomaly. Tracks the full lifecycle from detection to resolution.';

CREATE INDEX idx_incident_date   ON incidents(run_date DESC);
CREATE INDEX idx_incident_status ON incidents(status);


-- ============================================================
-- SEED DATA: Register our 3 pipeline jobs
-- ============================================================
INSERT INTO pipeline_jobs
    (job_id, job_name, description, input_datasets, output_datasets, owner, sla_max_duration_sec)
VALUES
    (
        'job_ingest_customers',
        'Ingest Raw Customers',
        'Reads raw_customers CSV from data/raw, validates schema, writes to clean layer.',
        '["raw.customers"]',
        '["clean.customers"]',
        'data-engineering-team',
        300  -- should finish within 5 minutes
    ),
    (
        'job_ingest_orders',
        'Ingest Raw Orders',
        'Reads raw_orders CSV from data/raw, validates schema, writes to clean layer.',
        '["raw.orders"]',
        '["clean.orders"]',
        'data-engineering-team',
        600  -- 10 minutes (larger file)
    ),
    (
        'job_build_sales_summary',
        'Build Customer Sales Summary',
        'Joins clean.customers + clean.orders, aggregates into analytics layer.',
        '["clean.customers", "clean.orders"]',
        '["analytics.customer_sales_summary"]',
        'analytics-team',
        180  -- 3 minutes
    );

-- ============================================================
-- VERIFY
-- ============================================================
SELECT
    'pipeline_jobs'      AS table_name, COUNT(*) AS rows FROM pipeline_jobs
UNION ALL SELECT
    'job_execution_logs' AS table_name, COUNT(*) AS rows FROM job_execution_logs
UNION ALL SELECT
    'dataset_snapshots'  AS table_name, COUNT(*) AS rows FROM dataset_snapshots
UNION ALL SELECT
    'incidents'          AS table_name, COUNT(*) AS rows FROM incidents;

