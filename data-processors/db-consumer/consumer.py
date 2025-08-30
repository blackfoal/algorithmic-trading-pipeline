import json
import os
import time
import logging
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get configuration from environment variables
kafka_broker = os.getenv('KAFKA_BROKER')
topic_name = os.getenv('TOPIC_NAME')
pg_user = os.getenv('POSTGRES_USER')
pg_password = os.getenv('POSTGRES_PASSWORD')
pg_db = os.getenv('POSTGRES_DB')
pg_host = os.getenv('POSTGRES_HOST')

def create_db_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port="5432",
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise



def insert_ohlcv_to_db(data):
    """Insert OHLCV data into PostgreSQL"""
    conn = None
    cur = None
    
    try:
        conn = create_db_connection()
        cur = conn.cursor()
        
        # Convert string dates back to proper types
        ts_utc = datetime.fromisoformat(data['ts'])
        date = datetime.fromisoformat(data['date']).date()
        
        sql = (
            """
            INSERT INTO ticker_data 
            (symbol, timestamp, ts, date, hour, min, open, high, low, close, volume_crypto, volume_usd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
            """
        )
        params = (
            data['symbol'],
            data['timestamp'],
            ts_utc,
            date,
            data['hour'],
            data['min'],
            data['open'],
            data['high'],
            data['low'],
            data['close'],
            data['volume_crypto'],
            data['volume_usd']
        )

        cur.execute(sql, params)
        conn.commit()
        logger.info(f"Inserted OHLCV data for {data['symbol']} at {ts_utc}")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting OHLCV data: {e}")
        if conn:
            conn.rollback()
        return False
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_kafka_consumer():
    """Get Kafka consumer with retry logic"""
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to create Kafka consumer (attempt {retry_count + 1})...")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='earliest',  # Get all messages to catch up on missed data
                enable_auto_commit=True,
                group_id='db-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=60000,  # 60 second timeout
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=40000,  # 40 second request timeout
                connections_max_idle_ms=50000  # Close idle connections after 50 seconds
            )
            logger.info("Kafka consumer initialized successfully")
            return consumer
        except Exception as e:
            retry_count += 1
            logger.error(f"Failed to initialize Kafka consumer (attempt {retry_count}): {e}")
            if retry_count < max_retries:
                time.sleep(5)
            else:
                raise

def main():
    logger.info("Starting Database consumer for real-time data...")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    logger.info("About to create Kafka consumer...")
    consumer = None
    
    try:
        consumer = get_kafka_consumer()
        logger.info(f"Listening for messages on topic: {topic_name}")
        
        for message in consumer:
            try:
                ohlcv_data = message.value
                logger.info(f"Received message for {ohlcv_data.get('symbol', 'unknown')} at {ohlcv_data.get('ts', 'unknown')}")
                
                success = insert_ohlcv_to_db(ohlcv_data)
                if not success:
                    logger.warning(f"Failed to insert data for {ohlcv_data.get('symbol', 'unknown')}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
            
    except KeyboardInterrupt:
        logger.info("Stopping Database consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if consumer:
            consumer.close()
        logger.info("Database consumer stopped")

if __name__ == "__main__":
    main()