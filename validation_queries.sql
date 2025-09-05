-- Price Buffer & Indicator Validation Queries
-- Run these queries to get all the data you need for manual validation

-- 1. Get complete price buffer (200 candles) for BTC/USDT
SELECT 
    ROW_NUMBER() OVER (ORDER BY ts ASC) as candle_number,
    symbol,
    ts as timestamp,
    to_timestamp(ts/1000) as readable_timestamp,
    open,
    high,
    low,
    close,
    volume_crypto,
    volume_usd
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts DESC 
LIMIT 200
ORDER BY ts ASC;

-- 2. Get current candle data (most recent)
SELECT 
    symbol,
    ts as timestamp,
    to_timestamp(ts/1000) as readable_timestamp,
    open,
    high,
    low,
    close,
    volume_crypto,
    'CURRENT CANDLE' as status
FROM ticker_data 
WHERE symbol = 'BTC/USDT'
ORDER BY ts DESC 
LIMIT 1;

-- 3. Get last 20 candles for Bollinger Bands calculation
SELECT 
    ROW_NUMBER() OVER (ORDER BY ts ASC) as candle_number,
    ts as timestamp,
    to_timestamp(ts/1000) as readable_timestamp,
    close as price
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts DESC 
LIMIT 20
ORDER BY ts ASC;

-- 4. Get last 26 candles for MACD calculation (minimum required)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ts ASC) as candle_number,
    ts as timestamp,
    to_timestamp(ts/1000) as readable_timestamp,
    close as price
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts DESC 
LIMIT 26
ORDER BY ts ASC;

-- 5. Check data freshness for all symbols
SELECT 
    symbol,
    MAX(ts) as latest_timestamp,
    to_timestamp(MAX(ts)/1000) as latest_readable,
    NOW() - to_timestamp(MAX(ts)/1000) as age,
    COUNT(*) as total_candles
FROM ticker_data 
GROUP BY symbol
ORDER BY latest_timestamp DESC;

-- 6. Get price buffer for all symbols (last 50 candles each)
SELECT 
    symbol,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts ASC) as candle_number,
    ts as timestamp,
    to_timestamp(ts/1000) as readable_timestamp,
    close as price
FROM ticker_data 
WHERE symbol IN ('BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'DOT/USDT', 'BNB/USDT', 'XRP/USDT')
ORDER BY symbol, ts DESC
LIMIT 350;  -- 50 candles * 7 symbols

-- 7. Check for any missing candles (gaps in data)
WITH candle_gaps AS (
    SELECT 
        symbol,
        ts,
        to_timestamp(ts/1000) as readable_timestamp,
        LAG(ts) OVER (PARTITION BY symbol ORDER BY ts) as prev_ts,
        ts - LAG(ts) OVER (PARTITION BY symbol ORDER BY ts) as gap_ms
    FROM ticker_data 
    WHERE symbol = 'BTC/USDT'
    ORDER BY ts DESC
    LIMIT 200
)
SELECT 
    symbol,
    readable_timestamp,
    gap_ms,
    CASE 
        WHEN gap_ms > 60000 THEN 'GAP DETECTED'
        ELSE 'OK'
    END as status
FROM candle_gaps 
WHERE gap_ms > 60000  -- More than 1 minute gap
ORDER BY ts DESC;

-- 8. Get closing prices as simple list for easy copy-paste
SELECT close 
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts ASC 
LIMIT 200;

-- 9. Get last 20 closing prices for Bollinger Bands (simple list)
SELECT close 
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts DESC 
LIMIT 20
ORDER BY ts ASC;

-- 10. Export data for external validation (CSV format)
SELECT 
    symbol,
    to_timestamp(ts/1000) as timestamp,
    open,
    high,
    low,
    close,
    volume_crypto
FROM ticker_data 
WHERE symbol = 'BTC/USDT' 
ORDER BY ts ASC 
LIMIT 200;
