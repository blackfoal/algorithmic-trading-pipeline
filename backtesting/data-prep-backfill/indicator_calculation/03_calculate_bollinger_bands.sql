-- Calculate Bollinger Bands indicators for all frequencies
-- This script calculates BB upper, middle, lower bands and z-score

-- Remove existing rows to ensure idempotency
DELETE FROM bb_indicators;

WITH bb_calculations AS (
    SELECT 
        symbol,
        ts,
        close,
        frequency,
        period_id,
        AVG(close) OVER (
            PARTITION BY symbol, frequency, period_id 
            ORDER BY ts 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as bb_middle,
        STDDEV_POP(close) OVER (
            PARTITION BY symbol, frequency, period_id 
            ORDER BY ts 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as bb_std
    FROM ticker_data t
    JOIN periods p ON t.period_id = p.id
    WHERE close IS NOT NULL 
        AND t.ts >= COALESCE(p.buffer_start_time, p.start_time)
        AND t.ts <= p.end_time
    ORDER BY symbol, frequency, ts
)
INSERT INTO bb_indicators (symbol, ts, period_id, frequency, bb_middle, bb_std, bb_upper, bb_lower, bb_z)
SELECT 
    symbol,
    ts,
    period_id,
    frequency,
    bb_middle,
    bb_std,
    (bb_middle + (2.0 * bb_std)) as bb_upper,
    (bb_middle - (2.0 * bb_std)) as bb_lower,
    CASE 
        WHEN bb_std = 0 THEN 0
        ELSE (close - bb_middle) / bb_std
    END as bb_z
FROM bb_calculations bc
JOIN periods p ON bc.period_id = p.id
WHERE bb_middle IS NOT NULL AND bb_std IS NOT NULL
    AND bc.ts >= p.start_time;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_bb_symbol_freq ON bb_indicators(symbol, frequency);
CREATE INDEX IF NOT EXISTS idx_bb_ts ON bb_indicators(ts);
CREATE INDEX IF NOT EXISTS idx_bb_period_id ON bb_indicators(period_id);
CREATE INDEX IF NOT EXISTS idx_bb_symbol_ts ON bb_indicators(symbol, ts);