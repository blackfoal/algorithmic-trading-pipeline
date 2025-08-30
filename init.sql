-- Create the database schema for Binance OHLCV data
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create unique constraint to prevent duplicate data
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_data_symbol_timestamp 
ON ticker_data(symbol, timestamp);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ticker_data_symbol_date 
ON ticker_data(symbol, date);

CREATE INDEX IF NOT EXISTS idx_ticker_data_timestamp 
ON ticker_data(timestamp);

-- Create index for time-based queries
CREATE INDEX IF NOT EXISTS idx_ticker_data_ts 
ON ticker_data(ts);
