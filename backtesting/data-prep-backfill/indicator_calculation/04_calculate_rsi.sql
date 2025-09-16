-- Calculate RSI indicators for backtesting
-- This script calculates RSI(7), RSI(14), and RSI(30)

-- Drop and recreate rsi_indicators table
DROP TABLE IF EXISTS rsi_indicators;

CREATE TABLE rsi_indicators AS
WITH price_changes AS (
    SELECT 
        symbol,
        ts,
        close,
        period_id,
        close - LAG(close) OVER (PARTITION BY symbol, period_id ORDER BY ts) as price_change
    FROM ticker_data_historical t
    JOIN periods p ON t.period_id = p.id
    WHERE close IS NOT NULL
        AND t.ts >= COALESCE(p.buffer_start_time, p.start_time)
        AND t.ts <= p.end_time
    ORDER BY symbol, ts
),

gains_losses AS (
    SELECT 
        symbol,
        ts,
        close,
        period_id,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
    FROM price_changes
    WHERE price_change IS NOT NULL
),

rsi_calculations AS (
    SELECT 
        symbol,
        ts,
        close,
        period_id,
        
        -- RSI(7) calculation
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY symbol, period_id ORDER BY ts) < 7 THEN NULL
            ELSE 100 - (100 / (1 + (
                AVG(gain) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) / NULLIF(AVG(loss) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 0)
            )))
        END as rsi_7,
        
        -- RSI(14) calculation
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY symbol, period_id ORDER BY ts) < 14 THEN NULL
            ELSE 100 - (100 / (1 + (
                AVG(gain) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / NULLIF(AVG(loss) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ), 0)
            )))
        END as rsi_14,
        
        -- RSI(30) calculation
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY symbol, period_id ORDER BY ts) < 30 THEN NULL
            ELSE 100 - (100 / (1 + (
                AVG(gain) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) / NULLIF(AVG(loss) OVER (
                    PARTITION BY symbol, period_id 
                    ORDER BY ts 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 0)
            )))
        END as rsi_30
        
    FROM gains_losses
    WHERE ts >= (
        SELECT MIN(ts) + INTERVAL '30 minutes' 
        FROM gains_losses 
        WHERE symbol = gains_losses.symbol
    )
)
SELECT 
    symbol,
    ts,
    period_id,
    rsi_7,
    rsi_14,
    rsi_30
FROM rsi_calculations rc
JOIN periods p ON rc.period_id = p.id
WHERE rsi_7 IS NOT NULL AND rsi_14 IS NOT NULL AND rsi_30 IS NOT NULL
    -- Only store results for the actual period (exclude buffer)
    AND rc.ts >= p.start_time;

-- Add primary key and indexes after table creation
ALTER TABLE rsi_indicators ADD PRIMARY KEY (symbol, ts, period_id);
CREATE INDEX idx_rsi_symbol ON rsi_indicators(symbol);
CREATE INDEX idx_rsi_ts ON rsi_indicators(ts);
CREATE INDEX idx_rsi_period_id ON rsi_indicators(period_id);
CREATE INDEX idx_rsi_symbol_ts ON rsi_indicators(symbol, ts);