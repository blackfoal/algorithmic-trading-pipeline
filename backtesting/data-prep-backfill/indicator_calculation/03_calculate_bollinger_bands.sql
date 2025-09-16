-- Calculate Bollinger Bands indicators for backtesting
-- This script calculates BB upper, middle, lower bands and z-score

-- Drop and recreate bb_indicators table
DROP TABLE IF EXISTS bb_indicators;

CREATE TABLE bb_indicators AS
WITH bb_calculations AS (
    SELECT 
        symbol,
        ts,
        close,
        period_id,
        
        -- Calculate BB Middle (20-period SMA)
        AVG(close) OVER (
            PARTITION BY symbol, period_id 
            ORDER BY ts 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as bb_middle,
        
        -- Calculate BB Standard Deviation
        STDDEV_POP(close) OVER (
            PARTITION BY symbol, period_id 
            ORDER BY ts 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as bb_std
        
    FROM ticker_data_historical t
    JOIN periods p ON t.period_id = p.id
    WHERE close IS NOT NULL 
        AND t.ts >= COALESCE(p.buffer_start_time, p.start_time)
        AND t.ts <= p.end_time
    ORDER BY symbol, ts
)
SELECT 
    symbol,
    ts,
    period_id,
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
    -- Only store results for the actual period (exclude buffer)
    AND bc.ts >= p.start_time;

-- Add primary key and indexes after table creation
ALTER TABLE bb_indicators ADD PRIMARY KEY (symbol, ts, period_id);
CREATE INDEX idx_bb_symbol ON bb_indicators(symbol);
CREATE INDEX idx_bb_ts ON bb_indicators(ts);
CREATE INDEX idx_bb_period_id ON bb_indicators(period_id);
CREATE INDEX idx_bb_symbol_ts ON bb_indicators(symbol, ts);