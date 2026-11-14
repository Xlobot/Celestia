-- ============================================================
-- DEVELOPMENT ONLY: Hard Reset Database Script
-- ============================================================
-- WARNING: This will drop all tables and delete all data!
-- Only run this when resetting your local development environment.
-- ============================================================

DROP TABLE IF EXISTS incidents          CASCADE;
DROP TABLE IF EXISTS dataset_snapshots  CASCADE;
DROP TABLE IF EXISTS job_execution_logs CASCADE;
DROP TABLE IF EXISTS pipeline_jobs      CASCADE;

\i 'db/schema.sql'
