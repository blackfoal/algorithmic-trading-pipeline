import os
import time
import json
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
import psycopg2
import ccxt

# Get configuration from environment variables
symbols = os.getenv('SYMBOLS').split(',')
kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
topic_name = os.getenv('OHLCV_TOPIC', 'binance-ohlcv')

binance_api_key = os.getenv('BINANCE_API_KEY')
binance_secret_key = os.getenv('BINANCE_SECRET_KEY')
backfill_days = int(os.getenv('BACKFILL_DAYS', '7'))
pg_user = os.getenv('POSTGRES_USER')
pg_password = os.getenv('POSTGRES_PASSWORD')
pg_db = os.getenv('POSTGRES_DB', 'binance')
pg_host = os.getenv('POSTGRES_HOST', 'postgres')

# Initialize Binance API
binance = ccxt.binance({
    'apiKey': binance_api_key,
    'secret': binance_secret_key,
    'enableRateLimit': True
})

def get_kafka_producer():
    """Get Kafka producer with retry logic"""
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("✅ Kafka producer initialized successfully")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Failed to initialize Kafka producer (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Failed to initialize Kafka producer after {max_retries} attempts: {e}")
                raise

def create_db_connection():
    """Create a connection to PostgreSQL database"""
    return psycopg2.connect(
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port="5432"
    )

def ensure_table_exists():
    """Ensure the ticker_data table exists with the correct schema"""
    conn = create_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_data (
            symbol TEXT,
            timestamp BIGINT,
            ts TIMESTAMPTZ,
            date DATE,
            hour SMALLINT,
            min SMALLINT,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume_crypto DOUBLE PRECISION,
            volume_usd DOUBLE PRECISION,
            UNIQUE(symbol, timestamp)
        );
    """)
    
    # Create indexes
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol ON ticker_data(symbol);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ticker_data(timestamp);
        CREATE INDEX IF NOT EXISTS idx_date ON ticker_data(date);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Ensured ticker_data table and indexes exist")

def truncate_table():
    """Truncate the ticker_data table for fresh start"""
    conn = create_db_connection()
    cur = conn.cursor()
    
    print("🗑️  Truncating ticker_data table for fresh start...")
    cur.execute("TRUNCATE TABLE ticker_data")
    conn.commit()
    
    cur.close()
    conn.close()
    print("✅ Table truncated successfully")

def insert_ohlcv_directly(data, conn):
    """Insert OHLCV data directly into PostgreSQL (for backfill)"""
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO ticker_data 
            (symbol, timestamp, ts, date, hour, min, open, high, low, close, volume_crypto, volume_usd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
        """, (
            data['symbol'],
            data['timestamp'],
            data['ts'],
            data['date'],
            data['hour'],
            data['min'],
            data['open'],
            data['high'],
            data['low'],
            data['close'],
            data['volume_crypto'],
            data['volume_usd']
        ))
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Error inserting OHLCV data: {e}")
        conn.rollback()
    
    finally:
        cur.close()

def backfill_historical_data():
    """Backfill historical data directly to database with clear boundaries"""
    print("🚀 === STARTING HISTORICAL BACKFILL ===")
    
    # Calculate exact boundaries
    now = datetime.now(timezone.utc)
    current_minute = now.replace(second=0, microsecond=0)
    end_time = int(current_minute.timestamp() * 1000)  # Current minute boundary
    start_time = end_time - (backfill_days * 24 * 60 * 60 * 1000)  # 7 days ago
    
    print(f"📊 Backfill boundaries:")
    print(f"  🕐 Start: {datetime.fromtimestamp(start_time/1000, tz=timezone.utc)}")
    print(f"  🕐 End: {current_minute}")
    print(f"  📅 Duration: {backfill_days} days")
    print(f"  🏷️  Symbols: {symbols}")
    
    # Create database connection for backfill
    conn = create_db_connection()
    total_inserted = 0
    
    try:
        for symbol in symbols:
            print(f"\n🔄 --- Backfilling {symbol} ---")
            symbol_inserted = 0
            current_time = start_time
            retry_count = 0
            max_retries = 3
            
            while current_time < end_time and retry_count < max_retries:
                try:
                    print(f"  📥 Fetching data from {datetime.fromtimestamp(current_time/1000, tz=timezone.utc)}")
                    ohlcv = binance.fetch_ohlcv(symbol, timeframe='1m', since=current_time, limit=1000)
                    
                    if not ohlcv:
                        print(f"  ⚠️  No more data available for {symbol}")
                        break
                    
                    batch_inserted = 0
                    for entry in ohlcv:
                        timestamp = entry[0]
                        
                        # Stop if we've reached the end boundary
                        if timestamp >= end_time:
                            print(f"  🎯 Reached end boundary for {symbol}")
                            break
                        
                        ts_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                        date = ts_utc.date()
                        hour = ts_utc.hour
                        minute = ts_utc.minute
                        
                        data = {
                            'symbol': symbol,
                            'timestamp': timestamp,
                            'ts': ts_utc,
                            'date': date,
                            'hour': hour,
                            'min': minute,
                            'open': float(entry[1]),
                            'high': float(entry[2]),
                            'low': float(entry[3]),
                            'close': float(entry[4]),
                            'volume_crypto': float(entry[5]),
                            'volume_usd': float(entry[5]) * float(entry[1])
                        }
                        
                        insert_ohlcv_directly(data, conn)
                        batch_inserted += 1
                    
                    symbol_inserted += batch_inserted
                    print(f"  ✅ Inserted {batch_inserted} records for {symbol}")
                    
                    # Update current time to next minute after last entry
                    if ohlcv:
                        current_time = ohlcv[-1][0] + 60000
                    
                    # Respect rate limits
                    time.sleep(binance.rateLimit / 1000)
                    retry_count = 0  # Reset retry count on success
                    
                except Exception as e:
                    retry_count += 1
                    print(f"  ❌ Error fetching backfill data for {symbol} (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        time.sleep(60)
                    else:
                        print(f"  ⚠️  Max retries reached for {symbol}, moving to next symbol")
                        break
            
            total_inserted += symbol_inserted
            print(f"  🎉 Completed {symbol}: {symbol_inserted} records")
            
    finally:
        conn.close()
    
    print(f"\n🎯 === HISTORICAL BACKFILL COMPLETED ===")
    print(f"📊 Total records inserted: {total_inserted}")
    print(f"🏷️  All symbols processed: {symbols}")
    print(f"📅 Backfill duration: {backfill_days} days")
    
    # Send completion marker (optional control topic)
    try:
        print("📡 Preparing completion marker...")
        completion_marker = {
            'type': 'backfill_complete',
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
            'message': 'Historical backfill completed - safe to start metrics calculation',
            'total_records': total_inserted,
            'start_time': start_time,
            'end_time': end_time,
            'symbols_processed': symbols
        }
        
        print("ℹ️ Backfill completion marker publishing removed")
    except Exception as e:
        print(f"❌ Failed to handle completion marker: {e}")
    
    print("🎯 Historical producer completed successfully!")
    return total_inserted

def main():
    print("📚 Historical Producer - OHLCV Data Backfiller")
    print("=" * 60)
    
    # Ensure table exists and truncate for fresh start
    ensure_table_exists()
    truncate_table()
    
    # Run backfill
    total_inserted = backfill_historical_data()
    
    print(f"\n🎉 SUCCESS: Historical backfill completed!")
    print(f"📊 Total records inserted: {total_inserted}")
    print(f"✅ Historical producer exiting...")
    
    return 0

if __name__ == "__main__":
    exit(main())
