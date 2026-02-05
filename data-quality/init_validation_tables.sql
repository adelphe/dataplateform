-- Initialize Great Expectations validation tracking tables
-- Run this script to set up PostgreSQL tables for storing validation results

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS data_platform;

-- Validation Results Table
-- Stores individual validation run results for historical tracking
CREATE TABLE IF NOT EXISTS data_platform.validation_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(256) NOT NULL,
    checkpoint_name VARCHAR(256),
    expectation_suite_name VARCHAR(256) NOT NULL,
    data_asset_name VARCHAR(256),
    validation_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    total_expectations INTEGER,
    successful_expectations INTEGER,
    unsuccessful_expectations INTEGER,
    success_percent NUMERIC(5, 2),
    result_json TEXT,
    execution_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_validation_results_run_id
    ON data_platform.validation_results(run_id);

CREATE INDEX IF NOT EXISTS idx_validation_results_suite
    ON data_platform.validation_results(expectation_suite_name);

CREATE INDEX IF NOT EXISTS idx_validation_results_date
    ON data_platform.validation_results(execution_date);

CREATE INDEX IF NOT EXISTS idx_validation_results_success
    ON data_platform.validation_results(success);

CREATE INDEX IF NOT EXISTS idx_validation_results_time
    ON data_platform.validation_results(validation_time DESC);

-- Data Quality Alerts Table
-- Stores alerts generated from validation failures
CREATE TABLE IF NOT EXISTS data_platform.data_quality_alerts (
    id SERIAL PRIMARY KEY,
    validation_result_id INTEGER REFERENCES data_platform.validation_results(id),
    alert_type VARCHAR(64) NOT NULL, -- 'failure', 'warning', 'degradation'
    severity VARCHAR(32) NOT NULL,   -- 'critical', 'high', 'medium', 'low'
    expectation_suite_name VARCHAR(256) NOT NULL,
    data_asset_name VARCHAR(256),
    message TEXT NOT NULL,
    failed_expectations TEXT,        -- JSON array of failed expectation types
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(128),
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_unacknowledged
    ON data_platform.data_quality_alerts(acknowledged)
    WHERE acknowledged = FALSE;

CREATE INDEX IF NOT EXISTS idx_alerts_severity
    ON data_platform.data_quality_alerts(severity, created_at DESC);

-- Data Quality Metrics Table
-- Stores aggregated quality metrics for dashboards
CREATE TABLE IF NOT EXISTS data_platform.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    expectation_suite_name VARCHAR(256) NOT NULL,
    layer VARCHAR(32),              -- 'raw', 'staging', 'marts'
    total_validations INTEGER DEFAULT 0,
    successful_validations INTEGER DEFAULT 0,
    total_expectations INTEGER DEFAULT 0,
    successful_expectations INTEGER DEFAULT 0,
    avg_success_percent NUMERIC(5, 2),
    min_success_percent NUMERIC(5, 2),
    max_success_percent NUMERIC(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metric_date, expectation_suite_name)
);

CREATE INDEX IF NOT EXISTS idx_metrics_date
    ON data_platform.data_quality_metrics(metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_metrics_layer
    ON data_platform.data_quality_metrics(layer, metric_date DESC);

-- Function to update metrics after each validation
CREATE OR REPLACE FUNCTION data_platform.update_quality_metrics()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO data_platform.data_quality_metrics (
        metric_date,
        expectation_suite_name,
        layer,
        total_validations,
        successful_validations,
        total_expectations,
        successful_expectations,
        avg_success_percent,
        min_success_percent,
        max_success_percent
    )
    VALUES (
        NEW.execution_date,
        NEW.expectation_suite_name,
        CASE
            WHEN NEW.expectation_suite_name LIKE 'raw%' THEN 'raw'
            WHEN NEW.expectation_suite_name LIKE 'staging%' THEN 'staging'
            WHEN NEW.expectation_suite_name LIKE 'mart%' THEN 'marts'
            ELSE 'other'
        END,
        1,
        CASE WHEN NEW.success THEN 1 ELSE 0 END,
        NEW.total_expectations,
        NEW.successful_expectations,
        NEW.success_percent,
        NEW.success_percent,
        NEW.success_percent
    )
    ON CONFLICT (metric_date, expectation_suite_name)
    DO UPDATE SET
        total_validations = data_platform.data_quality_metrics.total_validations + 1,
        successful_validations = data_platform.data_quality_metrics.successful_validations +
            CASE WHEN NEW.success THEN 1 ELSE 0 END,
        total_expectations = data_platform.data_quality_metrics.total_expectations +
            NEW.total_expectations,
        successful_expectations = data_platform.data_quality_metrics.successful_expectations +
            NEW.successful_expectations,
        avg_success_percent = (
            data_platform.data_quality_metrics.avg_success_percent *
            data_platform.data_quality_metrics.total_validations + NEW.success_percent
        ) / (data_platform.data_quality_metrics.total_validations + 1),
        min_success_percent = LEAST(
            data_platform.data_quality_metrics.min_success_percent, NEW.success_percent
        ),
        max_success_percent = GREATEST(
            data_platform.data_quality_metrics.max_success_percent, NEW.success_percent
        ),
        updated_at = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to update metrics automatically
DROP TRIGGER IF EXISTS trg_update_quality_metrics ON data_platform.validation_results;
CREATE TRIGGER trg_update_quality_metrics
    AFTER INSERT ON data_platform.validation_results
    FOR EACH ROW
    EXECUTE FUNCTION data_platform.update_quality_metrics();

-- Function to create alerts for failed validations
CREATE OR REPLACE FUNCTION data_platform.create_validation_alert()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT NEW.success THEN
        INSERT INTO data_platform.data_quality_alerts (
            validation_result_id,
            alert_type,
            severity,
            expectation_suite_name,
            data_asset_name,
            message
        )
        VALUES (
            NEW.id,
            'failure',
            CASE
                WHEN NEW.success_percent < 50 THEN 'critical'
                WHEN NEW.success_percent < 75 THEN 'high'
                WHEN NEW.success_percent < 90 THEN 'medium'
                ELSE 'low'
            END,
            NEW.expectation_suite_name,
            NEW.data_asset_name,
            format(
                'Validation failed: %s/%s expectations passed (%.1f%%)',
                NEW.successful_expectations,
                NEW.total_expectations,
                NEW.success_percent
            )
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to create alerts automatically
DROP TRIGGER IF EXISTS trg_create_validation_alert ON data_platform.validation_results;
CREATE TRIGGER trg_create_validation_alert
    AFTER INSERT ON data_platform.validation_results
    FOR EACH ROW
    EXECUTE FUNCTION data_platform.create_validation_alert();

-- View for current data quality status
CREATE OR REPLACE VIEW data_platform.vw_data_quality_status AS
SELECT
    expectation_suite_name,
    layer,
    COUNT(*) as total_runs_7d,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_runs_7d,
    ROUND(AVG(success_percent), 2) as avg_success_percent_7d,
    MAX(validation_time) as last_validation,
    BOOL_AND(success) FILTER (
        WHERE validation_time > CURRENT_TIMESTAMP - INTERVAL '1 day'
    ) as healthy_last_24h
FROM data_platform.validation_results
WHERE validation_time > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY expectation_suite_name,
    CASE
        WHEN expectation_suite_name LIKE 'raw%' THEN 'raw'
        WHEN expectation_suite_name LIKE 'staging%' THEN 'staging'
        WHEN expectation_suite_name LIKE 'mart%' THEN 'marts'
        ELSE 'other'
    END
ORDER BY expectation_suite_name;

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT ON data_platform.validation_results TO data_readers;
-- GRANT SELECT ON data_platform.data_quality_alerts TO data_readers;
-- GRANT SELECT ON data_platform.data_quality_metrics TO data_readers;
-- GRANT SELECT ON data_platform.vw_data_quality_status TO data_readers;
