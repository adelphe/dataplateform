-- Initialize schemas for the dbt transformation layer.
-- This script is idempotent: it uses CREATE IF NOT EXISTS throughout.

-- Raw schema: data loaded from MinIO Bronze Parquet files
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging schema: dbt staging models (cleaned, standardized data)
CREATE SCHEMA IF NOT EXISTS staging;

-- Intermediate schema: dbt intermediate models (business logic)
CREATE SCHEMA IF NOT EXISTS intermediate;

-- Marts schema: dbt mart models (analytics-ready tables)
CREATE SCHEMA IF NOT EXISTS marts;

-- Reference schema: dbt seed tables (static reference data)
CREATE SCHEMA IF NOT EXISTS reference;

-- dbt working schemas
CREATE SCHEMA IF NOT EXISTS dbt_dev;
CREATE SCHEMA IF NOT EXISTS dbt_prod;
