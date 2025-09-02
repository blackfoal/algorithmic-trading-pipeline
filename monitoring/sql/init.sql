-- System Health Monitoring Tables

-- Latency tracking table
CREATE TABLE IF NOT EXISTS latency_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    latency_ms DECIMAL(10,3) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol VARCHAR(20),
    additional_data JSONB
);

-- Event counting table
CREATE TABLE IF NOT EXISTS event_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol VARCHAR(20),
    additional_data JSONB
);

-- System performance metrics
CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,6) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    additional_data JSONB
);

-- Data freshness tracking
CREATE TABLE IF NOT EXISTS data_freshness (
    id SERIAL PRIMARY KEY,
    data_source VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    latest_timestamp TIMESTAMP NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source, symbol)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_latency_metrics_timestamp ON latency_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_latency_metrics_service ON latency_metrics(service_name, metric_name);
CREATE INDEX IF NOT EXISTS idx_event_metrics_timestamp ON event_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_event_metrics_service ON event_metrics(service_name, event_type);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_timestamp ON performance_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_service ON performance_metrics(service_name, metric_name);
CREATE INDEX IF NOT EXISTS idx_data_freshness_source ON data_freshness(data_source, symbol);

-- Create views for common queries
CREATE OR REPLACE VIEW latency_summary AS
SELECT 
    service_name,
    metric_name,
    AVG(latency_ms) as avg_latency_ms,
    MIN(latency_ms) as min_latency_ms,
    MAX(latency_ms) as max_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms,
    COUNT(*) as sample_count,
    MAX(timestamp) as last_updated
FROM latency_metrics 
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY service_name, metric_name;

CREATE OR REPLACE VIEW event_rate_summary AS
SELECT 
    service_name,
    event_type,
    SUM(count) as total_events,
    COUNT(*) as measurement_points,
    MAX(timestamp) as last_updated
FROM event_metrics 
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY service_name, event_type;

CREATE OR REPLACE VIEW data_freshness_summary AS
SELECT 
    data_source,
    symbol,
    latest_timestamp,
    EXTRACT(EPOCH FROM (NOW() - latest_timestamp))::INTEGER AS age_seconds,
    received_at,
    CASE 
        WHEN EXTRACT(EPOCH FROM (NOW() - latest_timestamp)) < 60 THEN 'Fresh'
        WHEN EXTRACT(EPOCH FROM (NOW() - latest_timestamp)) < 300 THEN 'Warning'
        ELSE 'Stale'
    END as freshness_status
FROM data_freshness
ORDER BY age_seconds DESC;



-- Truncate all monitoring tables on startup
TRUNCATE TABLE latency_metrics, event_metrics, performance_metrics, data_freshness RESTART IDENTITY CASCADE;
