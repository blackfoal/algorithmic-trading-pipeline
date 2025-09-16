-- PostgreSQL Initialization Script
-- Creates both grafana and binance databases with their schemas

-- ==============================================
-- CREATE DATABASES
-- ==============================================

-- Create grafana database for monitoring metrics
CREATE DATABASE grafana;

-- Create binance database for ticker data and trading signals
CREATE DATABASE binance;

-- ==============================================
-- GRAFANA DATABASE SCHEMA
-- ==============================================

\c grafana;

-- Performance metrics table
CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15, 6) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Latency metrics table
CREATE TABLE IF NOT EXISTS latency_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    latency_ms DECIMAL(10, 3) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Event metrics table
CREATE TABLE IF NOT EXISTS event_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_count INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data freshness metrics table
CREATE TABLE IF NOT EXISTS data_freshness (
    id SERIAL PRIMARY KEY,
    data_source VARCHAR(50) NOT NULL,
    last_update TIMESTAMP NOT NULL,
    age_seconds INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for grafana database
CREATE INDEX IF NOT EXISTS idx_performance_metrics_service_timestamp ON performance_metrics(service_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_latency_metrics_service_timestamp ON latency_metrics(service_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_event_metrics_service_timestamp ON event_metrics(service_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_data_freshness_source ON data_freshness(data_source);

-- Create views for easier querying in grafana
CREATE OR REPLACE VIEW performance_summary AS
SELECT 
    service_name,
    metric_name,
    AVG(metric_value) as avg_value,
    MIN(metric_value) as min_value,
    MAX(metric_value) as max_value,
    COUNT(*) as sample_count,
    MAX(timestamp) as last_updated
FROM performance_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY service_name, metric_name
ORDER BY service_name, metric_name;

CREATE OR REPLACE VIEW latency_summary AS
SELECT 
    service_name,
    operation,
    AVG(latency_ms) as avg_latency,
    MIN(latency_ms) as min_latency,
    MAX(latency_ms) as max_latency,
    COUNT(*) as sample_count,
    MAX(timestamp) as last_updated
FROM latency_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY service_name, operation
ORDER BY service_name, operation;

CREATE OR REPLACE VIEW event_summary AS
SELECT 
    service_name,
    event_type,
    SUM(event_count) as total_events,
    COUNT(*) as sample_count,
    MAX(timestamp) as last_updated
FROM event_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY service_name, event_type
ORDER BY service_name, event_type;

CREATE OR REPLACE VIEW data_freshness_summary AS
SELECT 
    data_source,
    last_update,
    age_seconds,
    status,
    CASE 
        WHEN age_seconds < 60 THEN 'Fresh'
        WHEN age_seconds < 300 THEN 'Warning'
        ELSE 'Stale'
    END as freshness_status
FROM data_freshness
ORDER BY age_seconds DESC;

-- ==============================================
-- BINANCE DATABASE SCHEMA
-- ==============================================

\c binance;

-- Ticker data table for storing OHLCV data
CREATE TABLE IF NOT EXISTS ticker_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    min INTEGER NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume_crypto DECIMAL(20, 8) NOT NULL,
    volume_usd DECIMAL(20, 8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

-- Trading signals table
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    signal_type VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    macd_line DECIMAL(20, 8),
    macd_signal DECIMAL(20, 8),
    macd_histogram DECIMAL(20, 8),
    bb_upper DECIMAL(20, 8),
    bb_middle DECIMAL(20, 8),
    bb_lower DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for binance database
CREATE INDEX IF NOT EXISTS idx_ticker_data_symbol_timestamp ON ticker_data(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_ticker_data_timestamp ON ticker_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_ticker_data_symbol ON ticker_data(symbol);
CREATE INDEX IF NOT EXISTS idx_ticker_data_date ON ticker_data(date);
CREATE INDEX IF NOT EXISTS idx_ticker_data_hour ON ticker_data(hour);
CREATE INDEX IF NOT EXISTS idx_ticker_data_min ON ticker_data(min);

CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol_timestamp ON trading_signals(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trading_signals_created_at ON trading_signals(created_at);

-- ==============================================
-- INITIALIZATION COMPLETE
-- ==============================================

-- Switch back to default database
\c postgres;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL initialization complete!';
    RAISE NOTICE 'Created databases: grafana, binance';
    RAISE NOTICE 'Grafana database: monitoring metrics and dashboards';
    RAISE NOTICE 'Binance database: ticker data and trading signals';
    RAISE NOTICE 'Note: Backtesting database runs in separate container';
END $$;
