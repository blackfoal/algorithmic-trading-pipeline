#!/usr/bin/env python3

import os
import json
import time
import logging
from typing import Dict
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
kafka_wait_seconds = int(os.getenv("KAFKA_WAIT_SECONDS"))

# Topics to monitor
TOPICS_TO_MONITOR = [
    "market-stream",
    "binance-ohlcv", 
    "trading-signals"
]

def wait_for_kafka(max_wait_seconds: int = kafka_wait_seconds) -> bool:
    """Wait for Kafka to be ready"""
    logger.info(f"Waiting for Kafka to be ready (max {max_wait_seconds} seconds)...")
    for attempt in range(max_wait_seconds):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BROKER],
                api_version_auto_timeout_ms=3000,
                consumer_timeout_ms=1000,
            )
            consumer.close()
            logger.info("Kafka is ready (connectivity check succeeded)")
            return True
        except Exception as e:
            logger.info(f"Waiting for Kafka to be ready (attempt {attempt}): {e}")
            time.sleep(3)
    logger.error("Kafka did not become ready within the allotted wait time")
    return False

class MetricsCollector:
    def __init__(self):
        self.db_connection = None
        self.consumers = {}
        self.message_counts = {}

        self.last_message_times = {}
        
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            self.db_connection.autocommit = True
            logger.info(f"Connected to PostgreSQL database: {POSTGRES_DB}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    

    
    def create_consumers(self):
        """Create Kafka consumers for each topic"""
        for topic in TOPICS_TO_MONITOR:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=[KAFKA_BROKER],
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='latest',
                    consumer_timeout_ms=1000,
                    group_id=f'metrics-collector-{topic}'
                )
                self.consumers[topic] = consumer
                self.message_counts[topic] = 0
                self.last_message_times[topic] = None
                logger.info(f"Created consumer for topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to create consumer for topic {topic}: {e}")
    
    def record_latency_metric(self, service_name: str, metric_name: str, latency_ms: float, symbol: str = None, additional_data: Dict = None):
        """Record a latency metric to the database"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                INSERT INTO latency_metrics (service_name, metric_name, latency_ms, symbol, additional_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (service_name, metric_name, latency_ms, symbol, json.dumps(additional_data) if additional_data else None))
            self.db_connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to record latency metric: {e}")
            try:
                if self.db_connection:
                    self.db_connection.rollback()
            except Exception:
                pass
    
    def record_event_metric(self, service_name: str, event_type: str, count: int = 1, symbol: str = None, additional_data: Dict = None):
        """Record an event metric to the database"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                INSERT INTO event_metrics (service_name, event_type, count, symbol, additional_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (service_name, event_type, count, symbol, json.dumps(additional_data) if additional_data else None))
            self.db_connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to record event metric: {e}")
            try:
                if self.db_connection:
                    self.db_connection.rollback()
            except Exception:
                pass
    
    def record_data_freshness(self, data_source: str, symbol: str, latest_timestamp: int):
        """Record data freshness metric"""
        try:
            cursor = self.db_connection.cursor()
            # Convert timestamp to datetime
            from datetime import datetime
            timestamp_dt = datetime.fromtimestamp(latest_timestamp / 1000)
            
            cursor.execute("""
                INSERT INTO data_freshness (data_source, symbol, latest_timestamp)
                VALUES (%s, %s, %s)
                ON CONFLICT (data_source, symbol) 
                DO UPDATE SET latest_timestamp = EXCLUDED.latest_timestamp, received_at = CURRENT_TIMESTAMP
            """, (data_source, symbol, timestamp_dt))
            self.db_connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to record data freshness: {e}")
            try:
                if self.db_connection:
                    self.db_connection.rollback()
            except Exception:
                pass
    
    def process_message(self, topic: str, message):
        """Process a message and extract metrics"""
        try:
            data = message.value
            current_time = time.time() * 1000  # Current time in milliseconds
        
            # Count messages
            self.message_counts[topic] += 1
            self.last_message_times[topic] = current_time
            
            # Record event metric
            self.record_event_metric(
                service_name=f"kafka-{topic}",
                event_type="message_received",
                count=1,
                symbol=data.get('symbol') if isinstance(data, dict) else None
            )
            
            # Extract timestamp and calculate latency if available
            if isinstance(data, dict):
                message_timestamp = data.get('timestamp')
                if message_timestamp:
                    # For minute candles, the candle completes 1 minute after its timestamp
                    # So we should measure latency from when it actually completes, not when it opens
                    if topic == 'binance-ohlcv':
                        candle_completion_time = message_timestamp + 60000  # Add 1 minute (60000ms)
                        latency_ms = current_time - candle_completion_time
                    else:
                        latency_ms = current_time - message_timestamp
                    
                    # Record latency metric
                    self.record_latency_metric(
                        service_name=f"kafka-{topic}",
                        metric_name="message_latency",
                        latency_ms=latency_ms,
                        symbol=data.get('symbol'),
                        additional_data={
                            "topic": topic,
                            "message_size": len(str(data))
                        }
                    )
                    
                    # Record data freshness
                    if data.get('symbol'):
                        self.record_data_freshness(
                            data_source=topic,
                            symbol=data.get('symbol'),
                            latest_timestamp=message_timestamp
                        )
                
                # Extract specific latency metrics from services
                if 'latency_ms' in data:
                    # From ws-realtime-producer
                    self.record_latency_metric(
                        service_name=data.get('service', 'unknown'),
                        metric_name="binance_to_producer_latency",
                        latency_ms=data['latency_ms'],
                        symbol=data.get('symbol'),
                        additional_data={
                            "event_type": data.get('type'),
                            "topic": topic
                        }
                    )
                
                if 'processing_latency_ms' in data:
                    # From signals-consumer
                    self.record_latency_metric(
                        service_name=data.get('service', 'unknown'),
                        metric_name="data_to_signal_latency",
                        latency_ms=data['processing_latency_ms'],
                        symbol=data.get('symbol'),
                        additional_data={
                            "update_type": data.get('update_type'),
                            "data_source": data.get('data_source'),
                            "topic": topic
                        }
                    )
                
                # Record signal generation events
                if data.get('action') == 'indicators_updated':
                    self.record_event_metric(
                        service_name=data.get('service', 'unknown'),
                        event_type="signal_generated",
                        count=1,
                        symbol=data.get('symbol'),
                        additional_data={
                            "update_type": data.get('update_type'),
                            "data_source": data.get('data_source')
                        }
                    )
            
        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}")
    
    def collect_metrics(self):
        """Main metrics collection loop"""
        logger.info("Starting metrics collection...")
        
        while True:
            try:
                # Poll all consumers
                for topic, consumer in self.consumers.items():
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self.process_message(topic, message)
                
                # Record periodic metrics
                self.record_periodic_metrics()
                
                time.sleep(1)  # Small delay to prevent excessive CPU usage
                
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(5)  # Wait before retrying
    
    def record_periodic_metrics(self):
        """Record periodic system metrics"""
        try:
            # Record message rates
            for topic, count in self.message_counts.items():
                if count > 0:
                    self.record_event_metric(
                        service_name="metrics-collector",
                        event_type=f"{topic}_message_rate",
                        count=count,
                        additional_data={"topic": topic}
                    )
                    self.message_counts[topic] = 0  # Reset counter
            
            # Record time since last message for each topic
            current_time = time.time() * 1000
            for topic, last_time in self.last_message_times.items():
                if last_time:
                    time_since_last = current_time - last_time
                    self.record_latency_metric(
                        service_name="metrics-collector",
                        metric_name=f"{topic}_time_since_last_message",
                        latency_ms=time_since_last,
                        additional_data={"topic": topic}
                    )
                    
        except Exception as e:
            logger.error(f"Error recording periodic metrics: {e}")

def main():
    """Main function"""
    logger.info("Starting Metrics Collector...")
    
    # Wait for Kafka to be ready
    if not wait_for_kafka():
        logger.error("Failed to connect to Kafka. Exiting.")
        return
    
    # Initialize metrics collector
    collector = MetricsCollector()
    
    # Connect to database
    collector.connect_database()
    
    # Create consumers
    collector.create_consumers()
    
    # Start collecting metrics
    collector.collect_metrics()

if __name__ == "__main__":
    main()