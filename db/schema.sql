-- ============================================================
-- Pipeline RCA System - Safe Schema Initialization
-- ============================================================
-- This script is idempotent and non-destructive.
-- Existing tables and data will NOT be dropped.
-- If you need to wipe and reset your dev DB, run db/reset_dev_db.sql.
-- ============================================================

-- ============================================================
-- TABLE 1: pipeline_jobs
-- Registry of all pipeline job definitions.
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id              VARCHAR(100)    PRIMARY KEY,
    job_name            VARCHAR(200)    NOT NULL,
    description         TEXT,
    input_datasets      JSONB           NOT NULL DEFAULT '[]',
    output_datasets     JSONB           NOT NULL DEFAULT '[]',
    owner               VARCHAR(100),
    sla_max_duration_sec INT,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TABLE 2: job_execution_logs
-- Primary evidence source for root cause analysis.
-- ============================================================
CREATE TABLE IF NOT EXISTS job_execution_logs (
    execution_id        BIGSERIAL       PRIMARY KEY,
    job_id              VARCHAR(100)    NOT NULL REFERENCES pipeline_jobs(job_id),
    run_date            DATE            NOT NULL,
    started_at          TIMESTAMPTZ     NOT NULL,
    ended_at            TIMESTAMPTZ,
    duration_seconds    INT GENERATED ALWAYS AS (
                            EXTRACT(EPOCH FROM (ended_at - started_at))::INT
                        ) STORED,
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
    error_message       TEXT,
    error_type          VARCHAR(100),
    log_reference       VARCHAR(500),
    schema_fingerprint  VARCHAR(64),
    input_row_counts    JSONB           DEFAULT '{}',
    output_row_counts   JSONB           DEFAULT '{}',
    extra_metadata      JSONB           DEFAULT '{}',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execlog_run_date ON job_execution_logs(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_execlog_job_date ON job_execution_logs(job_id, run_date DESC);
CREATE INDEX IF NOT EXISTS idx_execlog_status   ON job_execution_logs(status);

-- ============================================================
-- TABLE 3: dataset_snapshots
-- Daily profile of each dataset for baseline anomaly detection.
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_snapshots (
    snapshot_id         BIGSERIAL       PRIMARY KEY,
    dataset_name        VARCHAR(200)    NOT NULL,
    snapshot_date       DATE            NOT NULL,
    row_count           BIGINT          NOT NULL,
    is_weekend          BOOLEAN         NOT NULL DEFAULT FALSE,
    null_rates          JSONB           NOT NULL DEFAULT '{}',
    schema_fingerprint  VARCHAR(64),
    schema_snapshot     JSONB           NOT NULL DEFAULT '{}',
    file_path           VARCHAR(500),
    file_size_kb        NUMERIC(12, 2),
    execution_id        BIGINT          REFERENCES job_execution_logs(execution_id),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_name, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_dataset_date ON dataset_snapshots(dataset_name, snapshot_date DESC);

-- ============================================================
-- TABLE 4: incidents
-- Incident lifecycle management and root cause tracking.
-- ============================================================
CREATE TABLE IF NOT EXISTS incidents (
    incident_id         BIGSERIAL       PRIMARY KEY,
    detected_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    run_date            DATE            NOT NULL,
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
    affected_datasets   JSONB           NOT NULL DEFAULT '[]',
    ranked_causes       JSONB           DEFAULT '[]',
    evidence_package    JSONB           DEFAULT '{}',
    llm_explanation     TEXT,
    llm_recommendation  TEXT,
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

CREATE INDEX IF NOT EXISTS idx_incident_date   ON incidents(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_incident_status ON incidents(status);

-- ============================================================
-- SEED DATA: Register core pipeline jobs (idempotent)
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
        300
    ),
    (
        'job_ingest_orders',
        'Ingest Raw Orders',
        'Reads raw_orders CSV from data/raw, validates schema, writes to clean layer.',
        '["raw.orders"]',
        '["clean.orders"]',
        'data-engineering-team',
        600
    ),
    (
        'job_build_sales_summary',
        'Build Customer Sales Summary',
        'Joins clean.customers + clean.orders, aggregates into analytics layer.',
        '["clean.customers", "clean.orders"]',
        '["analytics.customer_sales_summary"]',
        'analytics-team',
        180
    )
ON CONFLICT (job_id) DO NOTHING;
