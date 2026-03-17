-- ─────────────────────────────────────────────────────────────────────────────
-- Flight Analytics Platform — Database Initialization
-- Creates schemas and extensions needed for the data warehouse
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- ─────────────────────────────────────────────────────────────────────────────
-- Schemas
-- raw     → landing zone: raw JSON blobs exactly as received from producer
-- staging → normalized, cleaned, validated records
-- main    → aggregated, analytical tables consumed by dashboard & ML
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS main;

GRANT ALL ON SCHEMA raw     TO CURRENT_USER;
GRANT ALL ON SCHEMA staging TO CURRENT_USER;
GRANT ALL ON SCHEMA main    TO CURRENT_USER;
