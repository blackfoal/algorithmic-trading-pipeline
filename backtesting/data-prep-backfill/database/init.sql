-- Backtesting Database Schema
-- This database stores backtesting-specific data including periods, pre-calculated indicators, and results

-- Note: Database is already created by the container

-- Periods table - stores testing periods with tags
CREATE TABLE IF NOT EXISTS periods (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    tags TEXT[], -- Array of tags for filtering
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Historical 1-minute data for backtesting (with 200 candles buffer for indicators)
CREATE TABLE IF NOT EXISTS ticker_data_historical (
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
    period_id VARCHAR(50) REFERENCES periods(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp, period_id)
);

-- Note: Removed ticker_data_seconds as 1-second historical data not available via REST API

-- Note: Removed indicator_base table - all indicators now read directly from ticker_data_historical

-- MACD indicators table
CREATE TABLE IF NOT EXISTS macd_indicators (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    close DECIMAL(20, 8) NOT NULL,
    
    -- MACD components
    ema_12 DECIMAL(20, 8),
    ema_26 DECIMAL(20, 8),
    macd_line DECIMAL(20, 8),
    macd_signal DECIMAL(20, 8),
    macd_histogram DECIMAL(20, 8),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ts, period_id)
);

-- Bollinger Bands indicators table
CREATE TABLE IF NOT EXISTS bb_indicators (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    
    -- Bollinger Bands components
    bb_middle DECIMAL(20, 8),
    bb_std DECIMAL(20, 8),
    bb_upper DECIMAL(20, 8),
    bb_lower DECIMAL(20, 8),
    bb_z DECIMAL(20, 8),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ts, period_id)
);

-- RSI indicators table
CREATE TABLE IF NOT EXISTS rsi_indicators (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    
    -- RSI components
    rsi_7 DECIMAL(20, 8),
    rsi_14 DECIMAL(20, 8),
    rsi_30 DECIMAL(20, 8),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ts, period_id)
);

-- State history table (final combined table)
CREATE TABLE IF NOT EXISTS state_history (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    period_id VARCHAR(50) REFERENCES periods(id),
    
    -- Price data
    close DECIMAL(20, 8) NOT NULL,
    
    -- MACD indicators
    macd_line DECIMAL(20, 8),
    macd_signal DECIMAL(20, 8),
    macd_histogram DECIMAL(20, 8),
    
    -- Bollinger Bands
    bb_upper DECIMAL(20, 8),
    bb_middle DECIMAL(20, 8),
    bb_lower DECIMAL(20, 8),
    bb_z DECIMAL(20, 8),
    
    -- RSI indicators
    rsi_7 DECIMAL(20, 8),
    rsi_14 DECIMAL(20, 8),
    rsi_30 DECIMAL(20, 8),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ts, period_id)
);

-- Strategy results
CREATE TABLE IF NOT EXISTS strategy_results (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    period_id VARCHAR(50) REFERENCES periods(id),
    symbol VARCHAR(20) NOT NULL,
    
    -- Performance metrics
    total_pnl DECIMAL(20, 8) NOT NULL DEFAULT 0,
    total_trades INTEGER NOT NULL DEFAULT 0,
    winning_trades INTEGER NOT NULL DEFAULT 0,
    losing_trades INTEGER NOT NULL DEFAULT 0,
    win_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    avg_win DECIMAL(20, 8) NOT NULL DEFAULT 0,
    avg_loss DECIMAL(20, 8) NOT NULL DEFAULT 0,
    max_drawdown DECIMAL(20, 8) NOT NULL DEFAULT 0,
    sharpe_ratio DECIMAL(10, 4) NOT NULL DEFAULT 0,
    
    -- Trade details
    trade_history JSONB, -- Array of all trades
    
    -- Simulation metadata
    simulation_start TIMESTAMP NOT NULL,
    simulation_end TIMESTAMP NOT NULL,
    simulation_duration_seconds INTEGER NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Strategy comparison results
CREATE TABLE IF NOT EXISTS strategy_comparison (
    id SERIAL PRIMARY KEY,
    comparison_id VARCHAR(50) NOT NULL,
    period_id VARCHAR(50) REFERENCES periods(id),
    strategy_ids TEXT[], -- Array of strategy IDs compared
    comparison_metrics JSONB, -- Comparison results
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ticker_historical_symbol_timestamp ON ticker_data_historical(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_ticker_historical_period ON ticker_data_historical(period_id);
CREATE INDEX IF NOT EXISTS idx_indicators_symbol_timestamp ON state_history(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_indicators_period ON state_history(period_id);
CREATE INDEX IF NOT EXISTS idx_strategy_results_strategy ON strategy_results(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_results_period ON strategy_results(period_id);
CREATE INDEX IF NOT EXISTS idx_strategy_results_symbol ON strategy_results(symbol);

-- Functions for updating timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for automatic timestamp updates
CREATE TRIGGER update_periods_updated_at BEFORE UPDATE ON periods
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ========================================
-- BACKTESTING REPORTING TABLES
-- ========================================

-- 1. Individual Trade History
-- Records every position opened and closed during backtesting
CREATE TABLE IF NOT EXISTS backfill_order_history (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    
    -- Trade timing
    entry_timestamp TIMESTAMPTZ NOT NULL,
    exit_timestamp TIMESTAMPTZ NOT NULL,
    duration_minutes INTEGER NOT NULL, -- Calculated field: exit - entry in minutes
    
    -- Trade outcome
    take_profit BOOLEAN NOT NULL DEFAULT FALSE,
    stop_loss BOOLEAN NOT NULL DEFAULT FALSE,
    pnl_percentage DECIMAL(10,4) NOT NULL, -- PnL as percentage
    
    -- Trade details
    position_size DECIMAL(20,8) NOT NULL,
    entry_price DECIMAL(20,8) NOT NULL,
    exit_price DECIMAL(20,8) NOT NULL,
    max_drawdown DECIMAL(10,4), -- Max drawdown during position
    
    -- Trigger conditions (stored as JSON)
    entry_trigger JSONB, -- State variables that triggered entry
    exit_trigger JSONB,  -- State variables that triggered exit
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Strategy Performance Summary
-- Aggregated performance metrics for each strategy/symbol/period combination
CREATE TABLE IF NOT EXISTS backfill_performance_summary (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    market_regime VARCHAR(100), -- From period tags (e.g., "bull_market", "high_volatility")
    
    -- Trade statistics
    number_of_positions_opened INTEGER NOT NULL DEFAULT 0,
    total_trades INTEGER NOT NULL DEFAULT 0,
    winning_trades INTEGER NOT NULL DEFAULT 0,
    losing_trades INTEGER NOT NULL DEFAULT 0,
    
    -- Performance metrics
    win_rate DECIMAL(5,2) NOT NULL DEFAULT 0, -- Percentage
    pnl_percentage DECIMAL(10,4) NOT NULL DEFAULT 0, -- Final PnL
    avg_win_pct DECIMAL(10,4) DEFAULT 0, -- Average win percentage
    avg_loss_pct DECIMAL(10,4) DEFAULT 0, -- Average loss percentage
    avg_position_holding_time DECIMAL(10,2) DEFAULT 0, -- Average holding time in minutes
    
    -- Risk metrics
    max_drawdown DECIMAL(10,4) DEFAULT 0, -- Maximum drawdown
    sharpe_ratio DECIMAL(10,4) DEFAULT 0, -- Risk-adjusted return
    profit_factor DECIMAL(10,4) DEFAULT 0, -- Gross profit / Gross loss
    
    -- Period timing
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE(strategy_id, symbol, period_id)
);

-- 3. Strategy Comparison (Optional - for cross-strategy analysis)
CREATE TABLE IF NOT EXISTS backfill_strategy_comparison (
    id SERIAL PRIMARY KEY,
    period_id VARCHAR(50) NOT NULL REFERENCES periods(id),
    market_regime VARCHAR(100),
    
    -- Best performing strategy for this period
    best_strategy_id VARCHAR(50),
    best_pnl DECIMAL(10,4),
    best_win_rate DECIMAL(5,2),
    
    -- Worst performing strategy for this period
    worst_strategy_id VARCHAR(50),
    worst_pnl DECIMAL(10,4),
    worst_win_rate DECIMAL(5,2),
    
    -- Overall statistics
    total_strategies_tested INTEGER,
    avg_pnl DECIMAL(10,4),
    avg_win_rate DECIMAL(5,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Market Regime Analysis (Optional - for regime-based insights)
CREATE TABLE IF NOT EXISTS backfill_market_regime_analysis (
    id SERIAL PRIMARY KEY,
    market_regime VARCHAR(100) NOT NULL,
    
    -- Performance by regime
    total_periods INTEGER DEFAULT 0,
    avg_pnl DECIMAL(10,4) DEFAULT 0,
    avg_win_rate DECIMAL(5,2) DEFAULT 0,
    best_strategy VARCHAR(50),
    worst_strategy VARCHAR(50),
    
    -- Risk metrics by regime
    avg_max_drawdown DECIMAL(10,4) DEFAULT 0,
    avg_sharpe_ratio DECIMAL(10,4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(market_regime)
);

-- 5. Symbol Performance Analysis (Optional - for symbol-based insights)
CREATE TABLE IF NOT EXISTS backfill_symbol_performance (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    
    -- Performance by symbol
    total_periods INTEGER DEFAULT 0,
    avg_pnl DECIMAL(10,4) DEFAULT 0,
    avg_win_rate DECIMAL(5,2) DEFAULT 0,
    best_strategy VARCHAR(50),
    worst_strategy VARCHAR(50),
    
    -- Risk metrics by symbol
    avg_max_drawdown DECIMAL(10,4) DEFAULT 0,
    avg_sharpe_ratio DECIMAL(10,4) DEFAULT 0,
    avg_holding_time DECIMAL(10,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol)
);

-- Indexes for reporting tables
CREATE INDEX IF NOT EXISTS idx_order_history_strategy ON backfill_order_history(strategy_id);
CREATE INDEX IF NOT EXISTS idx_order_history_symbol ON backfill_order_history(symbol);
CREATE INDEX IF NOT EXISTS idx_order_history_period ON backfill_order_history(period_id);
CREATE INDEX IF NOT EXISTS idx_order_history_timestamps ON backfill_order_history(entry_timestamp, exit_timestamp);

CREATE INDEX IF NOT EXISTS idx_performance_strategy ON backfill_performance_summary(strategy_id);
CREATE INDEX IF NOT EXISTS idx_performance_symbol ON backfill_performance_summary(symbol);
CREATE INDEX IF NOT EXISTS idx_performance_period ON backfill_performance_summary(period_id);
CREATE INDEX IF NOT EXISTS idx_performance_regime ON backfill_performance_summary(market_regime);
CREATE INDEX IF NOT EXISTS idx_performance_pnl ON backfill_performance_summary(pnl_percentage);

CREATE INDEX IF NOT EXISTS idx_comparison_period ON backfill_strategy_comparison(period_id);
CREATE INDEX IF NOT EXISTS idx_comparison_regime ON backfill_strategy_comparison(market_regime);

CREATE INDEX IF NOT EXISTS idx_regime_analysis ON backfill_market_regime_analysis(market_regime);
CREATE INDEX IF NOT EXISTS idx_symbol_performance ON backfill_symbol_performance(symbol);

-- Additional triggers for reporting tables
CREATE TRIGGER update_backfill_performance_summary_updated_at 
    BEFORE UPDATE ON backfill_performance_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_backfill_market_regime_analysis_updated_at 
    BEFORE UPDATE ON backfill_market_regime_analysis 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_backfill_symbol_performance_updated_at 
    BEFORE UPDATE ON backfill_symbol_performance 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
