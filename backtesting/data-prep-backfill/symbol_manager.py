"""
Symbol Manager for Backtesting
Handles symbol management, backfill operations, and data synchronization
"""

import os
import yaml
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Any
import ccxt
import time
import logging

class SymbolManager:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY', ''),
            'secret': os.getenv('BINANCE_SECRET_KEY', ''),
            'sandbox': False,
            'enableRateLimit': True,
        })
        self.logger = logging.getLogger(__name__)
        
    def get_symbols_from_env(self) -> List[str]:
        """Get symbols from .env file"""
        symbols_str = os.getenv('SYMBOLS', '')
        if not symbols_str:
            raise ValueError("No symbols found in .env file. Please set SYMBOLS environment variable.")
        
        symbols = [s.strip().upper() for s in symbols_str.split(',')]
        self.logger.info(f"Loaded {len(symbols)} symbols from .env: {symbols}")
        return symbols
    
    def add_symbol(self, symbol: str) -> bool:
        """Add new symbol to .env and backfill for all existing periods"""
        try:
            # 1. Add symbol to .env file
            self._add_symbol_to_env(symbol)
            
            # 2. Get all existing periods
            periods = self._get_all_periods()
            
            # 3. Backfill data for all periods
            for period in periods:
                self.logger.info(f"Backfilling {symbol} for period: {period['id']}")
                self._backfill_symbol_for_period(symbol, period)
            
            self.logger.info(f"Successfully added symbol {symbol} and backfilled for {len(periods)} periods")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding symbol {symbol}: {str(e)}")
            return False
    
    def _add_symbol_to_env(self, symbol: str):
        """Add symbol to .env file"""
        env_file = '.env'
        symbol = symbol.upper()
        
        # Read current .env file
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                lines = f.readlines()
        else:
            lines = []
        
        # Find SYMBOLS line and update it
        symbols_line_index = None
        for i, line in enumerate(lines):
            if line.startswith('SYMBOLS='):
                symbols_line_index = i
                break
        
        if symbols_line_index is not None:
            # Update existing SYMBOLS line
            current_symbols = lines[symbols_line_index].split('=')[1].strip()
            if symbol not in current_symbols:
                new_symbols = f"{current_symbols},{symbol}" if current_symbols else symbol
                lines[symbols_line_index] = f"SYMBOLS={new_symbols}\n"
        else:
            # Add new SYMBOLS line
            lines.append(f"SYMBOLS={symbol}\n")
        
        # Write back to .env file
        with open(env_file, 'w') as f:
            f.writelines(lines)
        
        self.logger.info(f"Added {symbol} to .env file")
    
    def _get_all_periods(self) -> List[Dict[str, Any]]:
        """Get all periods from database"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, start_time, end_time, tags, description
                    FROM periods
                    ORDER BY start_time
                """)
                periods = []
                for row in cursor.fetchall():
                    periods.append({
                        'id': row[0],
                        'name': row[1],
                        'start_time': row[2],
                        'end_time': row[3],
                        'tags': row[4],
                        'description': row[5]
                    })
                return periods
        finally:
            conn.close()
    
    def _backfill_symbol_for_period(self, symbol: str, period: Dict[str, Any]):
        """Backfill 1-minute and 1-second data for a symbol and period"""
        start_time = period['start_time']
        end_time = period['end_time']
        period_id = period['id']
        
        # Backfill 1-minute data (for price buffer)
        self._backfill_minute_data(symbol, start_time, end_time, period_id)
        
        # Backfill 1-second data (for current price simulation)
        self._backfill_second_data(symbol, start_time, end_time, period_id)
        
        self.logger.info(f"Completed backfill for {symbol} in period {period_id}")
    
    def _backfill_minute_data(self, symbol: str, start_time: datetime, end_time: datetime, period_id: str):
        """Backfill 1-minute OHLCV data"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Get existing data to avoid duplicates
                cursor.execute("""
                    SELECT MAX(timestamp) FROM ticker_data_minutes 
                    WHERE symbol = %s AND period_id = %s
                """, (symbol, period_id))
                last_timestamp = cursor.fetchone()[0]
                
                if last_timestamp:
                    start_time = max(start_time, last_timestamp + timedelta(minutes=1))
                
                if start_time >= end_time:
                    self.logger.info(f"Minute data for {symbol} in period {period_id} is already up to date")
                    return
                
                # Fetch data from Binance
                since = int(start_time.timestamp() * 1000)
                until = int(end_time.timestamp() * 1000)
                
                ohlcv_data = self.exchange.fetch_ohlcv(symbol, '1m', since=since, limit=1000)
                
                # Insert data
                for candle in ohlcv_data:
                    if since <= candle[0] <= until:
                        cursor.execute("""
                            INSERT INTO ticker_data_minutes 
                            (symbol, timestamp, open, high, low, close, volume_crypto, volume_usd, period_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
                        """, (
                            symbol,
                            datetime.fromtimestamp(candle[0] / 1000),
                            candle[1],  # open
                            candle[2],  # high
                            candle[3],  # low
                            candle[4],  # close
                            candle[5],  # volume
                            candle[5] * candle[4],  # volume_usd (approximate)
                            period_id
                        ))
                
                conn.commit()
                self.logger.info(f"Backfilled minute data for {symbol} in period {period_id}")
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error backfilling minute data for {symbol}: {str(e)}")
            raise
        finally:
            conn.close()
    
    def _backfill_second_data(self, symbol: str, start_time: datetime, end_time: datetime, period_id: str):
        """Backfill 1-second OHLCV data"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Get existing data to avoid duplicates
                cursor.execute("""
                    SELECT MAX(timestamp) FROM ticker_data_seconds 
                    WHERE symbol = %s AND period_id = %s
                """, (symbol, period_id))
                last_timestamp = cursor.fetchone()[0]
                
                if last_timestamp:
                    start_time = max(start_time, last_timestamp + timedelta(seconds=1))
                
                if start_time >= end_time:
                    self.logger.info(f"Second data for {symbol} in period {period_id} is already up to date")
                    return
                
                # For 1-second data, we'll use tick data and aggregate
                # This is a simplified approach - in production you might want to use WebSocket data
                current_time = start_time
                batch_size = 1000
                
                while current_time < end_time:
                    batch_end = min(current_time + timedelta(seconds=batch_size), end_time)
                    
                    # Fetch tick data (simplified - using 1-second intervals)
                    since = int(current_time.timestamp() * 1000)
                    until = int(batch_end.timestamp() * 1000)
                    
                    # Use 1-second intervals from Binance
                    ohlcv_data = self.exchange.fetch_ohlcv(symbol, '1s', since=since, limit=1000)
                    
                    for candle in ohlcv_data:
                        if since <= candle[0] <= until:
                            cursor.execute("""
                                INSERT INTO ticker_data_seconds 
                                (symbol, timestamp, open, high, low, close, volume_crypto, volume_usd, period_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (symbol, timestamp, period_id) DO NOTHING
                            """, (
                                symbol,
                                datetime.fromtimestamp(candle[0] / 1000),
                                candle[1],  # open
                                candle[2],  # high
                                candle[3],  # low
                                candle[4],  # close
                                candle[5],  # volume
                                candle[5] * candle[4],  # volume_usd
                                period_id
                            ))
                    
                    current_time = batch_end
                    time.sleep(0.1)  # Rate limiting
                
                conn.commit()
                self.logger.info(f"Backfilled second data for {symbol} in period {period_id}")
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error backfilling second data for {symbol}: {str(e)}")
            raise
        finally:
            conn.close()
    
    def get_symbols(self) -> List[str]:
        """Get current symbols from .env"""
        return self.get_symbols_from_env()
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists on Binance"""
        try:
            markets = self.exchange.load_markets()
            return symbol in markets
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {str(e)}")
            return False
