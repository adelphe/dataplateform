-- Initialize tracking tables for the batch ingestion pipeline.
-- This script is idempotent: it uses CREATE IF NOT EXISTS throughout.

-- Create the data_platform schema
CREATE SCHEMA IF NOT EXISTS data_platform;

-- Ingestion metadata: tracks every ingestion run with status, row counts, checksums
CREATE TABLE IF NOT EXISTS data_platform.ingestion_metadata (
    id              SERIAL PRIMARY KEY,
    source_name     VARCHAR(512) NOT NULL,
    source_type     VARCHAR(64)  NOT NULL,
    execution_date  VARCHAR(64)  NOT NULL,
    row_count       INTEGER,
    file_size_bytes BIGINT,
    checksum        VARCHAR(128),
    status          VARCHAR(32)  NOT NULL DEFAULT 'running',
    error_message   TEXT,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for querying by source and status
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_source
    ON data_platform.ingestion_metadata (source_name, status);

-- Watermarks: stores incremental load state per source and column
CREATE TABLE IF NOT EXISTS data_platform.watermarks (
    source_name      VARCHAR(512) NOT NULL,
    watermark_column VARCHAR(256) NOT NULL,
    watermark_value  VARCHAR(512),
    watermark_type   VARCHAR(32)  NOT NULL DEFAULT 'timestamp',
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, watermark_column)
);

-- Schema history: logs schema changes over time for auditing
CREATE TABLE IF NOT EXISTS data_platform.schema_history (
    id               SERIAL PRIMARY KEY,
    source_name      VARCHAR(512) NOT NULL,
    schema_metadata  JSONB        NOT NULL,
    execution_date   VARCHAR(64)  NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for querying schema history by source
CREATE INDEX IF NOT EXISTS idx_schema_history_source
    ON data_platform.schema_history (source_name, created_at DESC);
