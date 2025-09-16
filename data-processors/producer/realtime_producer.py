import os
import time
import json
import ccxt
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
topic_name = os.getenv('OHLCV_TOPIC', 'binance-ohlcv')
symbols_str = os.getenv('SYMBOLS')
symbols = [s.strip() for s in symbols_str.split(',')]
realtime_delay_seconds = int(os.getenv('REALTIME_DELAY_SECONDS', '1'))

# Initialize Binance exchange
binance = ccxt.binance({
    'enableRateLimit': True,
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_SECRET_KEY'),
    'options': {
        'defaultType': 'spot'
    }
})

# Initialize Kafka producer
producer = None

def get_kafka_producer():
    """Get Kafka producer with retry logic"""
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                request_timeout_ms=30000
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    return producer

def send_to_kafka(data, producer, topic, symbol):
    """Send data to Kafka topic"""
    try:
        key = symbol.replace('/', '_')  # Use symbol as key for partitioning
        logger.info(f"DEBUG: Attempting to send {symbol} data to Kafka topic '{topic}'")
        future = producer.send(topic, key=key, value=data)
        record_metadata = future.get(timeout=10)
        logger.info(f"DEBUG: Successfully sent {symbol} data to Kafka: partition={record_metadata.partition}, offset={record_metadata.offset}")
        logger.info(f"Sent {symbol} data to Kafka: partition={record_metadata.partition}, offset={record_metadata.offset}")
        return True
    except Exception as e:
        logger.error(f"Failed to send {symbol} data to Kafka: {e}")
        return False

def fetch_and_send_symbol_data(symbol, target_minute, producer, topic_name):
    """Fetch and send data for a specific minute for a single symbol"""
    try:
        # Calculate the exact timestamp for the target minute
        start_ts = int(target_minute.timestamp() * 1000)
        
        # Fetch the specific candle for this minute
        ohlcvs = binance.fetch_ohlcv(symbol, timeframe='1m', since=start_ts, limit=1)
        
        if ohlcvs and len(ohlcvs) > 0:
            ohlcv = ohlcvs[0]
            
            # Prepare data payload
            ohlcv_time = datetime.fromtimestamp(ohlcv[0] / 1000)
            data = {
                'symbol': symbol,
                'timestamp': ohlcv[0],
                'open': float(ohlcv[1]),
                'high': float(ohlcv[2]),
                'low': float(ohlcv[3]),
                'close': float(ohlcv[4]),
                'volume': float(ohlcv[5]),
                'ts': ohlcv_time.isoformat(),
                'date': ohlcv_time.isoformat(),
                'hour': ohlcv_time.hour,
                'min': ohlcv_time.minute,
                'volume_crypto': float(ohlcv[5]),
                'volume_usd': float(ohlcv[5] * ohlcv[4])
            }
            
            # DEBUG: Print the data being sent
            logger.info(f"DEBUG: Data to be sent for {symbol}: {json.dumps(data, indent=2)}")
            
            # Send to Kafka
            success = send_to_kafka(data, producer, topic_name, symbol)
            if success:
                logger.info(f"Processed {symbol} at {ohlcv_time}")
            return success
        else:
            logger.warning(f"No OHLCV data found for {symbol} at {current_minute}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        return False



def calculate_next_fetch_time():
    """Calculate time until next candle completion with buffer"""
    now = datetime.utcnow()
    next_candle = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    wait_seconds = (next_candle - now).total_seconds() + 5  # 5 second buffer
    return max(wait_seconds, 1)  # Minimum 1 second

def stream_data():
    """Dynamically fetch OHLCV data based on candle completion timing"""
    producer = get_kafka_producer()
    
    logger.info(f"Starting dynamic real-time streaming for symbols: {symbols}")
    
    while True:
        try:
            # Calculate the last completed minute (the one we want to fetch)
            now = datetime.utcnow()
            last_completed_minute = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
            
            logger.info(f"Fetching data for completed minute: {last_completed_minute}")
            
            # Process all symbols in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
                # Submit all symbol processing tasks
                futures = [
                    executor.submit(fetch_and_send_symbol_data, symbol, last_completed_minute, producer, topic_name)
                    for symbol in symbols
                ]
                
                # Wait for all tasks to complete
                concurrent.futures.wait(futures)
                
                # Check for any exceptions
                for future in futures:
                    if future.exception():
                        logger.error(f"Symbol processing failed: {future.exception()}")
            
            logger.info(f"Completed processing all symbols for minute: {last_completed_minute}")
            
            # Calculate dynamic wait time until next candle completion
            wait_time = calculate_next_fetch_time()
            logger.info(f"Waiting {wait_time:.1f} seconds until next candle completion...")
            time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error in stream_data: {e}")
            time.sleep(5)  # Wait before retrying

def main():
    """Main function"""
    logger.info("Starting dynamic candle-based data fetcher...")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(30)
    
    try:
        stream_data()
    except KeyboardInterrupt:
        logger.info("Shutting down real-time producer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()