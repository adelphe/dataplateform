-- OpenMetadata Database Initialization
-- Creates the database and schema for OpenMetadata data catalog

-- Create OpenMetadata database
SELECT 'CREATE DATABASE openmetadata_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db')\gexec

-- Connect to openmetadata_db and create extensions
\c openmetadata_db;

-- Create required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;

-- Create governance-related schemas in main database for tracking
\c airflow;

-- Schema for data governance tracking
CREATE SCHEMA IF NOT EXISTS governance;

-- Data ownership registry
CREATE TABLE IF NOT EXISTS governance.data_owners (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    owner_name VARCHAR(255) NOT NULL,
    owner_email VARCHAR(255) NOT NULL,
    owner_team VARCHAR(100),
    steward_name VARCHAR(255),
    steward_email VARCHAR(255),
    business_domain VARCHAR(100),
    data_classification VARCHAR(50) DEFAULT 'internal',
    pii_flag BOOLEAN DEFAULT FALSE,
    retention_days INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_name, schema_name, database_name)
);

-- Data access requests tracking
CREATE TABLE IF NOT EXISTS governance.access_requests (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    requester_name VARCHAR(255) NOT NULL,
    requester_email VARCHAR(255) NOT NULL,
    requester_team VARCHAR(100),
    dataset_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    access_type VARCHAR(50) NOT NULL, -- 'read', 'write', 'admin'
    justification TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'approved', 'rejected', 'revoked'
    approver_name VARCHAR(255),
    approver_email VARCHAR(255),
    approved_at TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data lineage tracking (supplementary to OpenMetadata)
CREATE TABLE IF NOT EXISTS governance.lineage_edges (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL, -- 'table', 'file', 'api'
    source_name VARCHAR(255) NOT NULL,
    source_schema VARCHAR(100),
    target_type VARCHAR(50) NOT NULL,
    target_name VARCHAR(255) NOT NULL,
    target_schema VARCHAR(100),
    transformation_type VARCHAR(50), -- 'dbt', 'sql', 'python', 'spark'
    transformation_name VARCHAR(255),
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_name, source_schema, target_name, target_schema)
);

-- Data glossary terms
CREATE TABLE IF NOT EXISTS governance.glossary_terms (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    term VARCHAR(255) NOT NULL UNIQUE,
    definition TEXT NOT NULL,
    domain VARCHAR(100),
    owner_email VARCHAR(255),
    synonyms TEXT[],
    related_terms TEXT[],
    status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'approved', 'deprecated'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data quality rules registry
CREATE TABLE IF NOT EXISTS governance.quality_rules (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(255),
    rule_type VARCHAR(50) NOT NULL, -- 'completeness', 'uniqueness', 'validity', 'consistency', 'timeliness'
    rule_definition TEXT NOT NULL,
    threshold DECIMAL(5,2),
    severity VARCHAR(20) DEFAULT 'warning', -- 'info', 'warning', 'critical'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_data_owners_dataset ON governance.data_owners(dataset_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_access_requests_status ON governance.access_requests(status);
CREATE INDEX IF NOT EXISTS idx_access_requests_requester ON governance.access_requests(requester_email);
CREATE INDEX IF NOT EXISTS idx_lineage_source ON governance.lineage_edges(source_name, source_schema);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON governance.lineage_edges(target_name, target_schema);
CREATE INDEX IF NOT EXISTS idx_glossary_domain ON governance.glossary_terms(domain);
CREATE INDEX IF NOT EXISTS idx_quality_rules_dataset ON governance.quality_rules(dataset_name, schema_name);

-- View for active data ownership
CREATE OR REPLACE VIEW governance.vw_active_ownership AS
SELECT
    do.dataset_name,
    do.schema_name,
    do.database_name,
    do.owner_name,
    do.owner_email,
    do.owner_team,
    do.steward_name,
    do.steward_email,
    do.business_domain,
    do.data_classification,
    do.pii_flag,
    COUNT(DISTINCT ar.id) FILTER (WHERE ar.status = 'approved') as active_access_grants,
    COUNT(DISTINCT le.id) as lineage_connections
FROM governance.data_owners do
LEFT JOIN governance.access_requests ar ON do.dataset_name = ar.dataset_name AND do.schema_name = ar.schema_name
LEFT JOIN governance.lineage_edges le ON (do.dataset_name = le.source_name AND do.schema_name = le.source_schema)
    OR (do.dataset_name = le.target_name AND do.schema_name = le.target_schema)
GROUP BY do.dataset_name, do.schema_name, do.database_name, do.owner_name,
         do.owner_email, do.owner_team, do.steward_name, do.steward_email,
         do.business_domain, do.data_classification, do.pii_flag;

COMMENT ON SCHEMA governance IS 'Data governance tracking schema for ownership, access, lineage, and quality rules';
COMMENT ON TABLE governance.data_owners IS 'Registry of data asset owners and stewards';
COMMENT ON TABLE governance.access_requests IS 'Tracking of data access requests and approvals';
COMMENT ON TABLE governance.lineage_edges IS 'Data lineage relationships between assets';
COMMENT ON TABLE governance.glossary_terms IS 'Business glossary of data terms and definitions';
COMMENT ON TABLE governance.quality_rules IS 'Data quality rules and thresholds';
