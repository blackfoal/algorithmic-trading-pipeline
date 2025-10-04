#!/usr/bin/env python3
"""
Data Backfill for Backtesting
Fetches historical data for all periods defined in periods.yaml
"""

import os
import psycopg2
from psycopg2.extras import execute_values
import ccxt
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import gc
from period_manager import PeriodManager
from config import get_db_config, get_symbols, to_db_symbol

class DataBackfill:
    def __init__(self, db_config: Dict[str, Any], config_path: str = "config/periods.yaml"):
        self.db_config = db_config
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
        
        # Initialize period manager
        self.period_manager = PeriodManager(db_config, config_path)
        
        # Initialize Binance exchange using ccxt (public API only)
        self.binance = ccxt.binance({
            'enableRateLimit': True,
            'sandbox': False,  # Use production API
            'apiKey': '',  # Empty API key
            'secret': '',  # Empty secret
            'options': {
                'defaultType': 'spot'
            }
        })
        
    def load_periods_from_config(self) -> List[Dict[str, Any]]:
        """Load periods from YAML config file and sync to database"""
        if self.period_manager.sync_periods_to_database():
            return self.period_manager.get_periods()
        return []
    
    
    def get_symbols_from_env(self) -> List[str]:
        """Get symbols strictly from environment variables"""
        try:
            return get_symbols(strict=True)
        except Exception as e:
            self.logger.error(str(e))
            return []
    
    def fetch_and_store_symbol_data(self, symbol: str, interval: str, start_time: datetime, end_time: datetime, period_id: str):
        """Fetch and store data for a symbol in a time range using streaming + batch inserts to reduce memory/connection pressure."""
        try:
            # Map our interval format to ccxt timeframe
            timeframe_map = {
                '15m': '15m',
                '30m': '30m',
                '1h': '1h'
            }
            
            if interval not in timeframe_map:
                self.logger.error(f"Unsupported interval: {interval}")
                return 0
            
            timeframe = timeframe_map[interval]

            since_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)

            records_inserted = 0
            conn = psycopg2.connect(**self.db_config)
            try:
                with conn.cursor() as cursor:
                    while since_ms < end_ms:
                        ohlcv_list = self.binance.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=1000)
                        if not ohlcv_list:
                            break

                        # Keep candles within end boundary, build rows for batch insert
                        rows: List[tuple] = []
                        for ohlcv in ohlcv_list:
                            ts_ms = ohlcv[0]
                            if ts_ms > end_ms:
                                break
                            candle_time = datetime.fromtimestamp(ts_ms / 1000)
                            # Convert BTC/USDT to BTCUSDT for database
                            db_symbol = to_db_symbol(symbol)
                            rows.append(
                                (
                                    db_symbol,
                                    interval,  # frequency column
                                    ts_ms,
                                    candle_time,
                                    candle_time.date(),
                                    candle_time.hour,
                                    candle_time.minute,
                                    float(ohlcv[1]),
                                    float(ohlcv[2]),
                                    float(ohlcv[3]),
                                    float(ohlcv[4]),
                                    float(ohlcv[5]),
                                    float(ohlcv[5] * ohlcv[1]),
                                    period_id,
                                )
                            )

                        if rows:
                            execute_values(
                                cursor,
                                """
                                INSERT INTO ticker_data (
                                    symbol, frequency, timestamp, ts, date, hour, min,
                                    open, high, low, close, volume_crypto, volume_usd, period_id
                                ) VALUES %s
                                ON CONFLICT (symbol, frequency, timestamp, period_id) DO NOTHING
                                """,
                                rows,
                                page_size=1000,
                            )
                            conn.commit()
                            records_inserted += len(rows)

                        # Advance since to the last candle time + 1 minute
                        since_ms = ohlcv_list[-1][0] + 60000
                        time.sleep(0.1)

                self.logger.info(f"Fetched and stored {records_inserted} {interval} candles for {symbol}")
                return records_inserted
            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"Error fetching {interval} data for {symbol}: {str(e)}")
            return 0
    
    # Removed unused _store_ohlcv_data to simplify module
    
    
    def backfill_period_data(self, period: Dict[str, Any], symbols: List[str]) -> bool:
        """Backfill data for a specific period"""
        period_id = period['id']
        start_time = period['start_time']
        end_time = period['end_time']
        
        # Add 200 candles before start_time for historical context
        buffer_times = {
            '15m': timedelta(minutes=15 * 200),
            '30m': timedelta(minutes=30 * 200),
            '1h': timedelta(hours=200)
        }
        
        self.logger.info(f"Backfilling data for period {period_id}: {start_time} to {end_time}")
        
        total_records = 0
        for symbol in symbols:
            self.logger.info(f"Processing {symbol} for period {period_id}")
            
            # Fetch data for each timeframe
            for interval in ['15m', '30m', '1h']:
                extended_start = start_time - buffer_times[interval]
                self.logger.info(f"Fetching {interval} data for {symbol}")
                records = self.fetch_and_store_symbol_data(symbol, interval, extended_start, end_time, period_id)
                total_records += records
            
            # Release resources between symbols to avoid memory growth
            try:
                self.binance.close()
            except Exception:
                pass
            self.binance = ccxt.binance({
                'apiKey': os.getenv('BINANCE_API_KEY', ''),
                'secret': os.getenv('BINANCE_SECRET_KEY', ''),
                'enableRateLimit': True
            })
            gc.collect()
            
            # Rate limiting
            time.sleep(0.2)
        
        self.logger.info(f"Successfully backfilled {total_records} total records for period {period_id}")
        return total_records > 0
    
    def backfill_all_periods(self) -> bool:
        """Backfill data for all periods"""
        periods = self.load_periods_from_config()
        if not periods:
            self.logger.error("No periods found in config")
            return False
        
        symbols = self.get_symbols_from_env()
        self.logger.info(f"Backfilling data for {len(periods)} periods and {len(symbols)} symbols")
        
        success_count = 0
        for period in periods:
            if self.backfill_period_data(period, symbols):
                success_count += 1
            else:
                self.logger.error(f"Failed to backfill data for period {period['id']}")
        
        self.logger.info(f"Backfill completed: {success_count}/{len(periods)} periods successful")
        return success_count == len(periods)
    
    def backfill_specific_period(self, period_id: str, symbols_override: Optional[List[str]] = None) -> bool:
        """Backfill data for a specific period. Optionally override symbols list."""
        periods = self.load_periods_from_config()
        period = next((p for p in periods if p['id'] == period_id), None)
        
        if not period:
            self.logger.error(f"Period {period_id} not found in config")
            return False
        
        symbols = symbols_override if symbols_override is not None else self.get_symbols_from_env()
        # Cleanup existing historical data for this period to ensure overwrite
        self._cleanup_historical_data_for_period(period_id)
        return self.backfill_period_data(period, symbols)

    def _cleanup_historical_data_for_period(self, period_id: str) -> None:
        """Delete existing historical data rows for the given period to avoid partial overlaps."""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                self.logger.info(f"Cleaning existing historical data for period {period_id}")
                cursor.execute("DELETE FROM ticker_data WHERE period_id = %s", (period_id,))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error cleaning historical data for period {period_id}: {str(e)}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_backfill_status(self) -> Dict[str, Any]:
        """Get status of backfilled data"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Check data for each frequency
                cursor.execute("""
                    SELECT period_id, symbol, frequency, COUNT(*) as record_count,
                           MIN(ts) as earliest, MAX(ts) as latest
                    FROM ticker_data
                    GROUP BY period_id, symbol, frequency
                    ORDER BY period_id, symbol, frequency
                """)
                data = cursor.fetchall()
                
                # Group by frequency for display
                status = {}
                for row in data:
                    freq = row[2]  # frequency column
                    if freq not in status:
                        status[freq] = []
                    status[freq].append((row[0], row[1], row[3], row[4], row[5]))
                
                return status
        finally:
            conn.close()

def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Backfill for Backtesting')
    parser.add_argument('--period', help='Specific period ID to backfill')
    parser.add_argument('--all', action='store_true', help='Backfill all periods')
    parser.add_argument('--status', action='store_true', help='Show backfill status')
    parser.add_argument('--symbols', help='Comma-separated symbols to backfill (e.g. BTCUSDT,ETHUSDT). Overrides .env SYMBOLS')
    parser.add_argument('--exec1m', action='store_true', help='Also backfill 1m execution feed into execution_data_1m')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get database config strictly from environment
    db_config = get_db_config(strict=True)
    
    backfill = DataBackfill(db_config)
    
    if args.status:
        status = backfill.get_backfill_status()
        print("\n📊 Backfill Status:")
        print("=" * 50)
        for freq, rows in status.items():
            print(f"\n{freq} Data:")
            for row in rows:
                print(f"  {row[0]} - {row[1]}: {row[2]:,} records ({row[3]} to {row[4]})")
    elif args.period:
        symbols_override = None
        if args.symbols:
            parts = [s.strip() for s in args.symbols.split(',') if s.strip()]
            # Normalize to exchange format without slash, e.g., BTCUSDT
            symbols_override = [p.upper().replace('/', '') for p in parts]
        success = backfill.backfill_specific_period(args.period, symbols_override)
        if success:
            print(f"✅ Successfully backfilled data for period: {args.period}")
        else:
            print(f"❌ Failed to backfill data for period: {args.period}")
    elif args.all:
        # If requested, first backfill the 1m execution feed
        if args.exec1m:
            symbols = backfill.get_symbols_from_env()
            periods = backfill.load_periods_from_config()
            for period in periods:
                period_id = period['id']
                start_time = period['start_time']
                end_time = period['end_time']
                for symbol in symbols:
                    # stream 1m OHLCV into execution_data_1m
                    try:
                        since_ms = int(start_time.timestamp() * 1000)
                        end_ms = int(end_time.timestamp() * 1000)
                        conn = psycopg2.connect(**backfill.db_config)
                        try:
                            with conn.cursor() as cursor:
                                while since_ms < end_ms:
                                    ohlcv_1m = backfill.binance.fetch_ohlcv(symbol, '1m', since=since_ms, limit=1000)
                                    if not ohlcv_1m:
                                        break
                                    rows = []
                                    for o in ohlcv_1m:
                                        ts_ms = o[0]
                                        if ts_ms > end_ms:
                                            break
                                        ct = datetime.fromtimestamp(ts_ms / 1000)
                                        db_symbol = to_db_symbol(symbol)
                                        rows.append((
                                            db_symbol,
                                            ts_ms,
                                            ct,
                                            ct.date(),
                                            ct.hour,
                                            ct.minute,
                                            float(o[1]), float(o[2]), float(o[3]), float(o[4]),
                                            float(o[5]), float(o[5] * o[1]),
                                            period_id,
                                        ))
                                    if rows:
                                        execute_values(
                                            cursor,
                                            """
                                            INSERT INTO execution_data_1m (
                                                symbol, timestamp, ts, date, hour, min,
                                                open, high, low, close, volume_crypto, volume_usd, period_id
                                            ) VALUES %s
                                            ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
                                            """,
                                            rows,
                                            page_size=1000,
                                        )
                                        conn.commit()
                                    since_ms = ohlcv_1m[-1][0] + 60000
                                    time.sleep(0.05)
                        finally:
                            conn.close()
                    except Exception as e:
                        backfill.logger.error(f"Exec1m backfill error for {symbol} in {period_id}: {e}")
        success = backfill.backfill_all_periods()
        if success:
            print("✅ Successfully backfilled data for all periods")
        else:
            print("❌ Failed to backfill data for some periods")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
