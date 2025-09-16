#!/usr/bin/env python3
"""
Data Backfill for Backtesting
Fetches historical data for all periods defined in periods.yaml
"""

import os
import yaml
import psycopg2
from psycopg2.extras import execute_values
import ccxt
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import gc

class DataBackfill:
    def __init__(self, db_config: Dict[str, Any], config_path: str = "config/periods.yaml"):
        self.db_config = db_config
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
        
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
        """Load periods from YAML config file with variable substitution"""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
                periods = config.get('periods', [])
                
                # Process periods and substitute variables
                processed_periods = []
                for period in periods:
                    processed_period = self._substitute_variables(period)
                    processed_periods.append(processed_period)
                
                self.logger.info(f"Loaded {len(processed_periods)} periods from config")
                return processed_periods
        except Exception as e:
            self.logger.error(f"Error loading periods from config: {str(e)}")
            return []
    
    def _substitute_variables(self, period: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute variables in period configuration"""
        processed_period = period.copy()
        
        # Handle dynamic last week variables
        if period.get('id') == 'dynamic_last_week':
            last_week_start, last_week_end = self._calculate_last_week_dates()
            
            # Substitute start_time variable
            if 'start_time' in processed_period and processed_period['start_time'] == '${LAST_WEEK_START}':
                processed_period['start_time'] = last_week_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Substitute end_time variable
            if 'end_time' in processed_period and processed_period['end_time'] == '${LAST_WEEK_END}':
                processed_period['end_time'] = last_week_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # Update name with actual dates
            processed_period['name'] = f"Last Week ({last_week_start.strftime('%Y-%m-%d')} to {last_week_end.strftime('%Y-%m-%d')})"
            
            # Update description with actual dates
            processed_period['description'] = f"Dynamic period representing the previous week: {last_week_start.strftime('%Y-%m-%d')} to {last_week_end.strftime('%Y-%m-%d')}. Automatically updated each time the system runs."
        
        return processed_period
    
    def _calculate_last_week_dates(self) -> tuple[datetime, datetime]:
        """Calculate last week's start and end dates (today - 7 days to current time)"""
        now = datetime.now()
        
        # Start: 7 days ago at 00:00:00
        start_date = now - timedelta(days=7)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # End: current time (not end of day)
        end_date = now
        
        return start_date, end_date
    
    def get_symbols_from_env(self) -> List[str]:
        """Get symbols from environment variables"""
        symbols_str = os.getenv('SYMBOLS', 'BTC/USDT,ETH/USDT,ADA/USDT')
        # Keep original BTC/USDT format for CCXT
        symbols = []
        for s in symbols_str.split(','):
            symbol = s.strip().upper()
            symbols.append(symbol)
        return symbols
    
    def fetch_and_store_symbol_data(self, symbol: str, interval: str, start_time: datetime, end_time: datetime, period_id: str):
        """Fetch and store data for a symbol in a time range using streaming + batch inserts to reduce memory/connection pressure."""
        try:
            # Convert interval format for ccxt
            if interval == '1s':
                self.logger.warning(f"1-second intervals not supported by Binance REST API for historical data. Skipping {symbol}")
                return 0
            elif interval == '1m':
                timeframe = '1m'
            else:
                self.logger.error(f"Unsupported interval: {interval}")
                return 0

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
                            rows.append(
                                (
                                    symbol.replace('/', ''),  # Convert BTC/USDT to BTCUSDT for database
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
                            if interval == '1m':
                                execute_values(
                                    cursor,
                                    """
                                    INSERT INTO ticker_data_historical (
                                        symbol, timestamp, ts, date, hour, min,
                                        open, high, low, close, volume_crypto, volume_usd, period_id
                                    ) VALUES %s
                                    ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
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
    
    def _store_ohlcv_data(self, data: dict, interval: str) -> bool:
        """Store OHLCV data in the appropriate table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                if interval == '1s':
                    cursor.execute("""
                        INSERT INTO ticker_data_seconds (
                            symbol, timestamp, ts, date, hour, min,
                            open, high, low, close, volume_crypto, volume_usd, period_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
                    """, (
                        data['symbol'], data['timestamp'], data['ts'], data['date'], 
                        data['hour'], data['min'], data['open'], data['high'], 
                        data['low'], data['close'], data['volume_crypto'], data['volume_usd'], data['period_id']
                    ))
                elif interval == '1m':
                    cursor.execute("""
                        INSERT INTO ticker_data_historical (
                            symbol, timestamp, ts, date, hour, min,
                            open, high, low, close, volume_crypto, volume_usd, period_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
                    """, (
                        data['symbol'], data['timestamp'], data['ts'], data['date'], 
                        data['hour'], data['min'], data['open'], data['high'], 
                        data['low'], data['close'], data['volume_crypto'], data['volume_usd'], data['period_id']
                    ))
                conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Error storing OHLCV data: {str(e)}")
            return False
    
    
    def backfill_period_data(self, period: Dict[str, Any], symbols: List[str]) -> bool:
        """Backfill data for a specific period"""
        period_id = period['id']
        start_time = datetime.strptime(period['start_time'], '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(period['end_time'], '%Y-%m-%d %H:%M:%S')
        
        # Add 200 minutes before start_time for historical context
        extended_start = start_time - timedelta(minutes=200)
        
        self.logger.info(f"Backfilling data for period {period_id}: {start_time} to {end_time}")
        
        total_records = 0
        for symbol in symbols:
            self.logger.info(f"Processing {symbol} for period {period_id}")
            
            # Note: 1-second data not available via Binance REST API for historical data
            # We'll use 1-minute data for backtesting (can be enhanced later with 1s simulation)
            
            # Fetch and store 1-minute data (with extended range for historical context)
            self.logger.info(f"Fetching 1-minute data for {symbol}")
            minutes_count = self.fetch_and_store_symbol_data(symbol, '1m', extended_start, end_time, period_id)
            total_records += minutes_count
            
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
                cursor.execute("DELETE FROM ticker_data_historical WHERE period_id = %s", (period_id,))
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
                # Check historical data (1-minute candles)
                cursor.execute("""
                    SELECT period_id, symbol, COUNT(*) as record_count,
                           MIN(ts) as earliest, MAX(ts) as latest
                    FROM ticker_data_historical
                    GROUP BY period_id, symbol
                    ORDER BY period_id, symbol
                """)
                historical_data = cursor.fetchall()
                
                return {
                    'seconds_data': [],  # Not available via REST API
                    'minutes_data': historical_data  # Using historical_data for consistency
                }
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
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get database config
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': 'backtesting',
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    backfill = DataBackfill(db_config)
    
    if args.status:
        status = backfill.get_backfill_status()
        print("\n📊 Backfill Status:")
        print("=" * 50)
        print("\n1-Second Data:")
        for row in status['seconds_data']:
            print(f"  {row[0]} - {row[1]}: {row[2]:,} records ({row[3]} to {row[4]})")
        print("\n1-Minute Data:")
        for row in status['minutes_data']:
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
        success = backfill.backfill_all_periods()
        if success:
            print("✅ Successfully backfilled data for all periods")
        else:
            print("❌ Failed to backfill data for some periods")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
