import os
import json
import logging
import psycopg2
from collections import defaultdict, deque
from typing import Deque, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import numpy as np
import threading
import time

from kafka import KafkaConsumer, KafkaProducer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
OHLCV_TOPIC = os.getenv("OHLCV_TOPIC", "binance-ohlcv")
MARKET_TOPIC = os.getenv("MARKET_STREAM_TOPIC", "market-stream")
SIGNALS_TOPIC = os.getenv("SIGNALS_TOPIC", "trading-signals")
WINDOW = int(os.getenv("INDICATOR_WINDOW", "200"))

# Add a sleep at the start of the script
logger.info("Sleeping for 30 seconds to ensure Kafka is ready...")
time.sleep(30)

# Remove BACKFILL_TOPIC reference
# BACKFILL_TOPIC = os.getenv("BACKFILL_EVENTS_TOPIC", "backfill-events")

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}


class IndicatorCalculator:
    def __init__(self, window_size: int = 200):
        self.window_size = window_size
        self.price_buffers: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window_size))
        self.current_candles: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'open': None, 'high': None, 'low': None, 'close': None, 'volume': None, 'timestamp': None
        })
        self.backfill_complete = False
        
    def load_historical_data(self, symbol: str) -> bool:
        """Load historical closing prices from database"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # Get last N closing prices for the symbol
            query = """
                SELECT close FROM ticker_data 
                WHERE symbol = %s 
                ORDER BY timestamp DESC 
                LIMIT %s
            """
            cursor.execute(query, (symbol, self.window_size))
            results = cursor.fetchall()
            
            if len(results) >= 26:  # Minimum for MACD (12, 26 periods)
                # Store in reverse order (oldest first)
                for close_price, in reversed(results):
                    self.price_buffers[symbol].append(float(close_price))
                
                cursor.close()
                conn.close()
                return True
            else:
                cursor.close()
                conn.close()
                return False
                
        except Exception as e:
            logger.error(f"Error loading historical data for {symbol}: {e}")
            return False
    
    def update_minute_candle(self, symbol: str, candle_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update price buffer with completed minute candle from REST API"""
        if not self.backfill_complete:
            return {}
            
        close_price = float(candle_data.get('close', 0))
        timestamp = candle_data.get('timestamp', 0)
        
        # Add completed candle to price buffer (oldest automatically removed due to maxlen)
        self.price_buffers[symbol].append(close_price)
        
        # Calculate indicators with updated buffer
        return self.calculate_indicators(symbol, close_price, timestamp, "minute_update")
    
    def update_live_candle(self, symbol: str, live_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update current candle with live WebSocket data"""
        if not self.backfill_complete:
            return {}
            
        # Update current candle data
        if live_data.get('type') == 'kline':
            k = live_data
            self.current_candles[symbol].update({
                'open': float(k.get('o', 0)),
                'high': float(k.get('h', 0)),
                'low': float(k.get('l', 0)),
                'close': float(k.get('c', 0)),
                'volume': float(k.get('v', 0)),
                'timestamp': k.get('ts', 0)
            })
            
            close_price = float(k.get('c', 0))
            timestamp = k.get('ts', 0)
            
            # Calculate indicators with current live data
            return self.calculate_indicators(symbol, close_price, timestamp, "live_update")
        
        return {}
    
    def calculate_indicators(self, symbol: str, close_price: float, timestamp: int, update_type: str) -> Dict[str, Any]:
        """Calculate MACD and Bollinger Bands indicators"""
        if len(self.price_buffers[symbol]) < 26:  # Minimum for MACD
            return {}
            
        # Create extended price list including current price for live calculations
        extended_prices = list(self.price_buffers[symbol]) + [close_price]
        
        # Calculate indicators with current price included (like Binance does)
        macd_line, signal_line, histogram = self.calculate_macd(extended_prices)
        bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(extended_prices)
        
        indicators = {
            'symbol': symbol,
            'timestamp': timestamp,
            'close_price': close_price,
            'update_type': update_type,
            'macd': {
                'line': round(macd_line, 6),
                'signal': round(signal_line, 6),
                'histogram': round(histogram, 6)
            },
            'bollinger_bands': {
                'upper': round(bb_upper, 6),
                'middle': round(bb_middle, 6),
                'lower': round(bb_lower, 6)
            },
            'buffer_size': len(self.price_buffers[symbol]),
            'current_candle': self.current_candles[symbol]
        }
        
        print(f"🎯 BREAKING POINT 3: Indicators are calculated for {symbol} - MACD: {indicators['macd']['line']:.6f}, BB_Upper: {indicators['bollinger_bands']['upper']:.6f}")
        
        return indicators
    
    def calculate_macd(self, prices: Deque[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
        """Calculate MACD: MACD line, Signal line, Histogram"""
        if len(prices) < slow:
            return 0.0, 0.0, 0.0
            
        prices_array = np.array(list(prices))
        
        # Calculate EMA arrays
        ema_fast = self._calculate_ema(prices_array, fast)
        ema_slow = self._calculate_ema(prices_array, slow)
        
        # MACD line (final value)
        macd_line = ema_fast[-1] - ema_slow[-1]
        
        # Signal line (EMA of MACD)
        macd_values = []
        for i in range(len(prices_array)):
            if i < slow - 1:
                macd_values.append(0)
            else:
                macd_values.append(ema_fast[i] - ema_slow[i])
        
        # Calculate signal line from MACD values
        macd_array = np.array(macd_values)
        signal_line_array = self._calculate_ema(macd_array, signal)
        signal_line = signal_line_array[-1] if len(signal_line_array) > 0 else 0.0
        
        # Histogram
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    def calculate_bollinger_bands(self, prices: Deque[float], period: int = 20, std_dev: float = 2.0) -> Tuple[float, float, float]:
        """Calculate Bollinger Bands: Upper, Middle (SMA), Lower"""
        if len(prices) < period:
            return 0.0, 0.0, 0.0
            
        prices_array = np.array(list(prices))
        
        # Middle band (SMA)
        middle = np.mean(prices_array[-period:])
        
        # Standard deviation
        std = np.std(prices_array[-period:])
        
        # Upper and lower bands
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return upper, middle, lower
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """Calculate Exponential Moving Average for each price point"""
        if len(prices) < period:
            return np.full(len(prices), prices[-1] if len(prices) > 0 else 0.0)
            
        # Initialize EMA array
        ema_values = np.zeros(len(prices))
        
        # Use simple average for first period
        sma = np.mean(prices[:period])
        ema_values[:period] = sma
        
        # Calculate multiplier
        multiplier = 2.0 / (period + 1)
        
        # Calculate EMA for each subsequent price
        ema = sma
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
            ema_values[i] = ema
            
        return ema_values
    
    def set_backfill_complete(self):
        """Mark backfill as complete"""
        self.backfill_complete = True
        logger.info("Backfill marked as complete - indicators are now active")


def get_consumer(topic: str, group_id: Optional[str]) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000,  # Increased from 1000ms to 30 seconds
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        request_timeout_ms=15000,
        connections_max_idle_ms=30000
    )


def get_anonymous_consumer() -> KafkaConsumer:
    """Create an anonymous consumer for manual partition assignment"""
    return KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Disable auto-commit for anonymous consumer
        group_id=None,  # Anonymous consumer
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        request_timeout_ms=15000,
        connections_max_idle_ms=30000
    )


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        request_timeout_ms=5000
    )


def process_minute_data(calculator: IndicatorCalculator, producer: KafkaProducer, symbols: list):
    """Process minute OHLCV data from binance-ohlcv topic (reliable, on-time updates)"""
    minute_consumer = get_consumer(OHLCV_TOPIC, 'signals-minute-processor')
    
    try:
        for msg in minute_consumer:
            try:
                if not calculator.backfill_complete:
                    continue
                    
                data = msg.value
                
                # Handle both string and dict message formats
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                
                symbol = data.get('symbol')
                if not symbol:
                    continue
                
                # Normalize symbol format (remove / and ensure consistency)
                normalized_symbol = symbol.replace('/', '')
                db_symbol = None
                
                # Find matching symbol in our monitored list
                for monitored_symbol in symbols:
                    if monitored_symbol.replace('/', '') == normalized_symbol:
                        db_symbol = monitored_symbol
                        break
                
                if not db_symbol:
                    continue
                    
                # Update price buffer with completed minute candle
                indicators = calculator.update_minute_candle(db_symbol, data)
                
                if indicators:
                    # Calculate latency from original data timestamp
                    current_time = int(time.time() * 1000)
                    data_timestamp = data.get('timestamp', current_time)
                    
                    # For minute candles, the candle completes 1 minute after its timestamp
                    # So we should measure latency from when it actually completes, not when it opens
                    candle_completion_time = data_timestamp + 60000  # Add 1 minute (60000ms)
                    processing_latency = current_time - candle_completion_time
                    
                    # Publish indicators to signals topic
                    signal = {
                        'symbol': db_symbol,
                        'action': 'indicators_updated',
                        'update_type': 'minute_candle',
                        'timestamp': indicators['timestamp'],
                        'indicators': indicators,
                        'ts': datetime.utcnow().isoformat(),
                        'processing_latency_ms': processing_latency,
                        'service': 'signals-consumer',
                        'data_source': 'binance-ohlcv'
                    }
                    
                    producer.send(SIGNALS_TOPIC, key=db_symbol, value=signal)
                    logger.info(f"Published signal for {db_symbol} - Processing latency: {processing_latency}ms")
                    
            except Exception as e:
                logger.error(f"Error processing minute message: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in minute data processor: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        logger.info("Minute data processor shutting down")
        minute_consumer.close()


def process_live_data(calculator: IndicatorCalculator, producer: KafkaProducer, symbols: list):
    """Process live WebSocket data (real-time updates for current candle only)"""
    live_consumer = get_consumer(MARKET_TOPIC, 'signals-live-processor')
    
    try:
        for msg in live_consumer:
            try:
                if not calculator.backfill_complete:
                    continue
                    
                event = msg.value
                
                event_type = event.get("type")
                ws_symbol = event.get("symbol")
                
                if not ws_symbol:
                    continue
                
                # Map WebSocket symbol to database symbol format
                db_symbol = None
                for monitored_symbol in symbols:
                    # Remove / from monitored symbol and compare with WebSocket symbol
                    if monitored_symbol.replace('/', '') == ws_symbol:
                        db_symbol = monitored_symbol
                        break
                
                if not db_symbol:
                    continue
                
                if event_type == "kline":
                    # Update current candle with live data (don't update price buffer)
                    indicators = calculator.update_live_candle(db_symbol, event)
                    
                    if indicators:
                        # Calculate latency from original data timestamp
                        current_time = int(time.time() * 1000)
                        data_timestamp = event.get('ts', current_time)
                        processing_latency = current_time - data_timestamp
                        
                        # Publish indicators to signals topic
                        signal = {
                            'symbol': db_symbol,
                            'action': 'indicators_updated',
                            'update_type': 'live_update',
                            'timestamp': indicators['timestamp'],
                            'indicators': indicators,
                            'ts': datetime.utcnow().isoformat(),
                            'processing_latency_ms': processing_latency,
                            'service': 'signals-consumer',
                            'data_source': 'market-stream'
                        }
                        
                        producer.send(SIGNALS_TOPIC, key=db_symbol, value=signal)
                        logger.info(f"Published live signal for {db_symbol} - Processing latency: {processing_latency}ms")
                        
                elif event_type == "aggTrade":
                    # Could use for additional real-time price monitoring
                    # but don't update price buffer - only current candle
                    pass
                    
            except Exception as e:
                logger.error(f"Error processing live message: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in live data processor: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        logger.info("Live data processor shutting down")
        live_consumer.close()


def main():
    # Initialize indicator calculator
    calculator = IndicatorCalculator(WINDOW)
    
    # Get producer for publishing signals
    producer = get_producer()
    
    # Get symbols from environment
    symbols_str = os.getenv('SYMBOLS', 'BTC/USDT')
    symbols = [s.strip() for s in symbols_str.split(',')]
    
    # Assume backfill is complete (simplest approach)
    print("🎯 BREAKING POINT 1: Backfill assumed complete - starting indicators")
    calculator.set_backfill_complete()
    
    # Load historical data for all symbols
    print("🎯 BREAKING POINT 2: We read the data from db and formed the price buffer")
    for symbol in symbols:
        calculator.load_historical_data(symbol)
    
    # Now start both processors in separate threads
    logger.info("Starting hybrid data processors...")
    logger.info(f"Backfill complete status before starting threads: {calculator.backfill_complete}")
    
    # Start minute data processor (REST API data)
    minute_thread = threading.Thread(
        target=process_minute_data, 
        args=(calculator, producer, symbols),
        daemon=True
    )
    minute_thread.start()
    
    # Start live data processor (WebSocket data)
    live_thread = threading.Thread(
        target=process_live_data, 
        args=(calculator, producer, symbols),
        daemon=True
    )
    live_thread.start()
    
    logger.info(f"Backfill complete status after starting threads: {calculator.backfill_complete}")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
            if not minute_thread.is_alive() or not live_thread.is_alive():
                logger.error("One of the data processors has stopped!")
                logger.error(f"Minute thread alive: {minute_thread.is_alive()}")
                logger.error(f"Live thread alive: {live_thread.is_alive()}")
                break
            logger.debug(f"Main loop - Minute thread: {minute_thread.is_alive()}, Live thread: {live_thread.is_alive()}")
    except KeyboardInterrupt:
        logger.info("Shutting down signals consumer...")
    
    logger.info("Signals consumer stopped.")


if __name__ == "__main__":
    main()


