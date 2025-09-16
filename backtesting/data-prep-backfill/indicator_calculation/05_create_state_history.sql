-- Drop and recreate state_history table with all indicators
-- This is the single source of truth for backtesting
DROP TABLE IF EXISTS state_history;

CREATE TABLE state_history AS
SELECT 
    m.symbol,
    m.ts,
    m.period_id,
    m.close,
    
    -- MACD indicators
    m.macd_line,
    m.macd_signal,
    m.macd_histogram,
    
    -- Bollinger Bands
    bb.bb_upper,
    bb.bb_middle,
    bb.bb_lower,
    bb.bb_z,
    
    -- RSI indicators
    r.rsi_7,
    r.rsi_14,
    r.rsi_30
    
FROM macd_indicators m
JOIN periods p ON m.period_id = p.id
LEFT JOIN bb_indicators bb ON (
    m.symbol = bb.symbol 
    AND m.ts = bb.ts
    AND m.period_id = bb.period_id
)
LEFT JOIN rsi_indicators r ON (
    m.symbol = r.symbol 
    AND m.ts = r.ts
    AND m.period_id = r.period_id
)
WHERE m.ts >= p.start_time AND m.ts <= p.end_time
ORDER BY m.symbol, m.ts;

-- Add primary key and indexes after table creation
ALTER TABLE state_history ADD PRIMARY KEY (symbol, ts, period_id);
CREATE INDEX idx_state_history_symbol ON state_history(symbol);
CREATE INDEX idx_state_history_ts ON state_history(ts);
CREATE INDEX idx_state_history_period_id ON state_history(period_id);
CREATE INDEX idx_state_history_symbol_ts ON state_history(symbol, ts);
