-- Create unified state history table with all indicators per frequency
-- This is the single source of truth for backtesting

-- Remove existing rows to ensure idempotency
DELETE FROM state_history;

INSERT INTO state_history (
    symbol, ts, period_id, frequency, close,
    macd_line, macd_signal, macd_histogram,
    bb_upper, bb_middle, bb_lower, bb_z,
    rsi_7, rsi_14, rsi_30
)
SELECT 
    m.symbol,
    m.ts,
    m.period_id,
    m.frequency,
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
    AND m.frequency = bb.frequency
)
LEFT JOIN rsi_indicators r ON (
    m.symbol = r.symbol 
    AND m.ts = r.ts
    AND m.period_id = r.period_id
    AND m.frequency = r.frequency
)
WHERE m.ts >= p.start_time AND m.ts <= p.end_time
ORDER BY m.symbol, m.frequency, m.ts;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_state_history_symbol_freq ON state_history(symbol, frequency);
CREATE INDEX IF NOT EXISTS idx_state_history_ts ON state_history(ts);
CREATE INDEX IF NOT EXISTS idx_state_history_period_id ON state_history(period_id);
CREATE INDEX IF NOT EXISTS idx_state_history_symbol_ts ON state_history(symbol, ts);