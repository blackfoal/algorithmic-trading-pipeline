import os
import json
import time
import threading
import logging
from typing import List

from kafka import KafkaProducer
from binance import ThreadedWebsocketManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KAFKA_BROKER = os.getenv("KAFKA_BROKER", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
TOPIC = os.getenv("MARKET_STREAM_TOPIC", "market-stream")
SYMBOLS = [s.strip().upper().replace("/", "") for s in os.getenv("SYMBOLS", "BTCUSDT").split(",")]


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )


def main():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_SECRET_KEY")

    producer = get_producer()

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()

    def publish(event: dict):
        try:
            # Add latency tracking
            current_time = int(time.time() * 1000)
            event["producer_timestamp"] = current_time
            
            # Calculate latency from Binance timestamp
            binance_ts = event.get("ts", current_time)
            latency_ms = current_time - binance_ts
            
            # Add latency info to event
            event["latency_ms"] = latency_ms
            event["service"] = "ws-realtime-producer"
            
            key = event.get("symbol", "").lower()
            producer.send(TOPIC, key=key, value=event)
            
            # Log latency for monitoring
            logger.info(f"Published {event.get('type')} for {event.get('symbol')} - Latency: {latency_ms}ms")
            
        except Exception as e:
            logger.error(f"Error publishing event: {e}")

    # Kline (1m) stream callback
    def handle_kline(msg):
        # Example payload keys: e (event), s (symbol), k (kline dict)
        k = msg.get("k", {})
        event = {
            "type": "kline",
            "symbol": msg.get("s", "").upper(),
            "ts": msg.get("E"),
            "interval": k.get("i"),
            "open_time": k.get("t"),
            "close_time": k.get("T"),
            "o": k.get("o"),
            "h": k.get("h"),
            "l": k.get("l"),
            "c": k.get("c"),
            "v": k.get("v"),
            "is_closed": k.get("x"),
        }
        publish(event)

    # AggTrade callback (optional tick price)
    def handle_aggtrade(msg):
        event = {
            "type": "aggTrade",
            "symbol": msg.get("s", "").upper(),
            "ts": msg.get("E"),
            "price": msg.get("p"),
            "qty": msg.get("q"),
            "trade_time": msg.get("T"),
        }
        publish(event)

    # Subscribe per symbol
    for sym in SYMBOLS:
        twm.start_kline_socket(callback=handle_kline, symbol=sym, interval="1m")
        twm.start_aggtrade_socket(callback=handle_aggtrade, symbol=sym)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        twm.stop()


if __name__ == "__main__":
    main()


