-- ============================================
-- Monitoring Tables for Data Platform
-- ============================================
-- This script creates tables for tracking data freshness,
-- platform health metrics, and operational monitoring.

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS data_platform;

-- ============================================
-- Data Freshness Metrics Table
-- ============================================
-- Tracks the freshness of data across all layers
CREATE TABLE IF NOT EXISTS data_platform.data_freshness_metrics (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL,
    layer VARCHAR(50) NOT NULL CHECK (layer IN ('raw', 'staging', 'marts')),
    last_update_time TIMESTAMP WITH TIME ZONE,
    row_count BIGINT DEFAULT 0,
    check_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    is_fresh BOOLEAN DEFAULT TRUE,
    staleness_hours NUMERIC(10, 2) DEFAULT 0,
    expected_freshness_hours INTEGER DEFAULT 24,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_freshness_check_time
    ON data_platform.data_freshness_metrics(check_time DESC);
CREATE INDEX IF NOT EXISTS idx_freshness_source_layer
    ON data_platform.data_freshness_metrics(source_name, layer);
CREATE INDEX IF NOT EXISTS idx_freshness_stale
    ON data_platform.data_freshness_metrics(is_fresh) WHERE NOT is_fresh;

-- ============================================
-- Platform Health Metrics Table
-- ============================================
-- Stores aggregated platform health metrics
CREATE TABLE IF NOT EXISTS data_platform.platform_health_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL DEFAULT CURRENT_DATE,
    metric_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    scheduler_healthy BOOLEAN DEFAULT TRUE,
    scheduler_heartbeat_age_seconds INTEGER,
    total_dags INTEGER DEFAULT 0,
    active_dags INTEGER DEFAULT 0,
    paused_dags INTEGER DEFAULT 0,
    failed_tasks_24h INTEGER DEFAULT 0,
    successful_tasks_24h INTEGER DEFAULT 0,
    running_tasks INTEGER DEFAULT 0,
    queued_tasks INTEGER DEFAULT 0,
    sla_compliance_percent NUMERIC(5, 2) DEFAULT 100.00,
    sla_misses_24h INTEGER DEFAULT 0,
    avg_data_freshness_hours NUMERIC(10, 2) DEFAULT 0,
    stale_sources_count INTEGER DEFAULT 0,
    data_quality_success_rate NUMERIC(5, 2) DEFAULT 100.00,
    health_score INTEGER DEFAULT 100 CHECK (health_score >= 0 AND health_score <= 100),
    issues JSONB DEFAULT '[]'::JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_health_metric_date
    ON data_platform.platform_health_metrics(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_health_metric_time
    ON data_platform.platform_health_metrics(metric_time DESC);
CREATE INDEX IF NOT EXISTS idx_health_score
    ON data_platform.platform_health_metrics(health_score);

-- ============================================
-- Alert History Table
-- ============================================
-- Tracks all alerts sent for audit and analysis
CREATE TABLE IF NOT EXISTS data_platform.alert_history (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    subject VARCHAR(500) NOT NULL,
    message TEXT,
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    execution_date TIMESTAMP WITH TIME ZONE,
    channels_notified JSONB DEFAULT '[]'::JSONB,
    email_sent BOOLEAN DEFAULT FALSE,
    slack_sent BOOLEAN DEFAULT FALSE,
    suppressed BOOLEAN DEFAULT FALSE,
    suppression_reason VARCHAR(255),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by VARCHAR(255),
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_alert_history_type
    ON data_platform.alert_history(alert_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_severity
    ON data_platform.alert_history(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_dag
    ON data_platform.alert_history(dag_id, task_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_unack
    ON data_platform.alert_history(acknowledged) WHERE NOT acknowledged;

-- ============================================
-- SLA Tracking Table
-- ============================================
-- Tracks SLA definitions and compliance
CREATE TABLE IF NOT EXISTS data_platform.sla_tracking (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255),
    execution_date DATE NOT NULL,
    expected_completion TIMESTAMP WITH TIME ZONE NOT NULL,
    actual_completion TIMESTAMP WITH TIME ZONE,
    sla_met BOOLEAN,
    delay_seconds INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dag_id, task_id, execution_date)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sla_tracking_dag
    ON data_platform.sla_tracking(dag_id, execution_date DESC);
CREATE INDEX IF NOT EXISTS idx_sla_tracking_missed
    ON data_platform.sla_tracking(sla_met) WHERE sla_met = FALSE;

-- ============================================
-- View: Current Data Freshness Status
-- ============================================
-- Provides the latest freshness status for each source/layer
CREATE OR REPLACE VIEW data_platform.vw_data_freshness_status AS
WITH latest_checks AS (
    SELECT DISTINCT ON (source_name, layer)
        source_name,
        layer,
        last_update_time,
        row_count,
        check_time,
        is_fresh,
        staleness_hours,
        expected_freshness_hours
    FROM data_platform.data_freshness_metrics
    ORDER BY source_name, layer, check_time DESC
)
SELECT
    source_name,
    layer,
    last_update_time,
    row_count,
    check_time,
    is_fresh,
    staleness_hours,
    expected_freshness_hours,
    CASE
        WHEN staleness_hours < 6 THEN 'green'
        WHEN staleness_hours < 24 THEN 'yellow'
        ELSE 'red'
    END AS freshness_color,
    CASE
        WHEN NOT is_fresh THEN 'STALE'
        WHEN staleness_hours < 6 THEN 'FRESH'
        ELSE 'AGING'
    END AS freshness_status
FROM latest_checks;

-- ============================================
-- View: Platform Health Summary
-- ============================================
-- Provides the latest platform health summary
CREATE OR REPLACE VIEW data_platform.vw_platform_health_summary AS
SELECT
    metric_date,
    metric_time,
    scheduler_healthy,
    total_dags,
    active_dags,
    failed_tasks_24h,
    running_tasks,
    queued_tasks,
    sla_compliance_percent,
    avg_data_freshness_hours,
    health_score,
    issues,
    CASE
        WHEN health_score >= 90 THEN 'HEALTHY'
        WHEN health_score >= 70 THEN 'DEGRADED'
        ELSE 'UNHEALTHY'
    END AS health_status,
    CASE
        WHEN health_score >= 90 THEN 'green'
        WHEN health_score >= 70 THEN 'yellow'
        ELSE 'red'
    END AS health_color
FROM data_platform.platform_health_metrics
ORDER BY metric_time DESC
LIMIT 1;

-- ============================================
-- View: Daily Health Trends
-- ============================================
-- Aggregated daily health metrics for trending
CREATE OR REPLACE VIEW data_platform.vw_daily_health_trends AS
SELECT
    metric_date,
    AVG(health_score) AS avg_health_score,
    MIN(health_score) AS min_health_score,
    MAX(health_score) AS max_health_score,
    SUM(failed_tasks_24h) AS total_failed_tasks,
    AVG(sla_compliance_percent) AS avg_sla_compliance,
    AVG(avg_data_freshness_hours) AS avg_data_freshness,
    COUNT(*) AS check_count
FROM data_platform.platform_health_metrics
GROUP BY metric_date
ORDER BY metric_date DESC;

-- ============================================
-- View: Unacknowledged Alerts
-- ============================================
CREATE OR REPLACE VIEW data_platform.vw_unacknowledged_alerts AS
SELECT
    id,
    alert_type,
    severity,
    subject,
    dag_id,
    task_id,
    created_at,
    EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600 AS hours_since_alert
FROM data_platform.alert_history
WHERE acknowledged = FALSE
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
        ELSE 5
    END,
    created_at DESC;

-- ============================================
-- Function: Calculate Health Score
-- ============================================
CREATE OR REPLACE FUNCTION data_platform.calculate_health_score(
    p_scheduler_healthy BOOLEAN,
    p_failed_tasks_24h INTEGER,
    p_sla_compliance NUMERIC,
    p_stale_sources INTEGER,
    p_queued_tasks INTEGER
) RETURNS INTEGER AS $$
DECLARE
    v_score INTEGER := 100;
BEGIN
    -- Scheduler health (20 points)
    IF NOT p_scheduler_healthy THEN
        v_score := v_score - 20;
    END IF;

    -- Task failures (25 points)
    IF p_failed_tasks_24h > 10 THEN
        v_score := v_score - 25;
    ELSIF p_failed_tasks_24h > 5 THEN
        v_score := v_score - 15;
    ELSIF p_failed_tasks_24h > 0 THEN
        v_score := v_score - 5;
    END IF;

    -- SLA compliance (25 points)
    IF p_sla_compliance < 80 THEN
        v_score := v_score - 25;
    ELSIF p_sla_compliance < 90 THEN
        v_score := v_score - 15;
    ELSIF p_sla_compliance < 95 THEN
        v_score := v_score - 5;
    END IF;

    -- Data freshness (20 points)
    IF p_stale_sources > 5 THEN
        v_score := v_score - 20;
    ELSIF p_stale_sources > 2 THEN
        v_score := v_score - 10;
    ELSIF p_stale_sources > 0 THEN
        v_score := v_score - 5;
    END IF;

    -- Queue health (10 points)
    IF p_queued_tasks > 100 THEN
        v_score := v_score - 10;
    ELSIF p_queued_tasks > 50 THEN
        v_score := v_score - 5;
    END IF;

    RETURN GREATEST(v_score, 0);
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Grant permissions
-- ============================================
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA data_platform TO airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA data_platform TO airflow;
GRANT SELECT ON ALL TABLES IN SCHEMA data_platform TO PUBLIC;
