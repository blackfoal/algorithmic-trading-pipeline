#!/usr/bin/env python3
"""
Data Preparation for Backtesting
Orchestrates the creation of indicator base table and calculation of all indicators
"""

import os
import sys
import psycopg2
import logging
from typing import List, Dict, Any
from pathlib import Path
from config import get_db_config

class DataPreparation:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        self.indicator_dir = Path(__file__).parent / "indicator_calculation"
        
    def prepare_all_data(self, frequencies: List[str] = None) -> bool:
        """Prepare all indicator data for all periods and frequencies"""
        try:
            if frequencies is None:
                frequencies = ['15m', '30m', '1h']
            
            self.logger.info(f"Starting data preparation for all periods with frequencies: {frequencies}")
            
            for freq in frequencies:
                self.logger.info(f"\nProcessing {freq} frequency:")
                
                # Step 1: Calculate MACD indicators using Python
                self.logger.info(f"Step 1: Calculating MACD indicators for {freq}...")
                self._run_python_macd(frequency=freq)
                
                # Step 2: Calculate Bollinger Bands
                self.logger.info(f"Step 2: Calculating Bollinger Bands for {freq}...")
                self._execute_sql_file("03_calculate_bollinger_bands.sql")
                
                # Step 3: Calculate RSI indicators
                self.logger.info(f"Step 3: Calculating RSI indicators for {freq}...")
                self._execute_sql_file("04_calculate_rsi.sql")
                
                # Step 4: Create state history table
                self.logger.info(f"Step 4: Creating state history table for {freq}...")
                self._execute_sql_file("05_create_state_history.sql")
            
            self.logger.info("Data preparation completed for all periods and frequencies")
            return True
            
        except Exception as e:
            self.logger.error(f"Error preparing data: {str(e)}")
            return False
    
    def prepare_all_periods(self, frequencies: List[str] = None) -> bool:
        """Prepare data for all periods - delegates to prepare_all_data with optional frequencies"""
        return self.prepare_all_data(frequencies=frequencies)
    
    def prepare_period_data(self, period_id: str, frequencies: List[str] = None) -> bool:
        """Prepare indicator data for a specific period and frequencies"""
        try:
            if frequencies is None:
                frequencies = ['15m', '30m', '1h']
            
            self.logger.info(f"Starting data preparation for period {period_id} with frequencies: {frequencies}")
            
            for freq in frequencies:
                self.logger.info(f"\nProcessing {freq} frequency for period {period_id}:")
                
                # Step 1: Calculate MACD indicators for specific period using Python
                self.logger.info(f"Step 1: Calculating MACD indicators for period using Python ({freq})...")
                self._run_python_macd_period(period_id, frequency=freq)
                
                # Step 2: Calculate Bollinger Bands for specific period
                self.logger.info(f"Step 2: Calculating Bollinger Bands for period ({freq})...")
                self._execute_sql_file_with_period("03_calculate_bollinger_bands.sql", period_id)
                
                # Step 3: Calculate RSI indicators for specific period
                self.logger.info(f"Step 3: Calculating RSI indicators for period ({freq})...")
                self._execute_sql_file_with_period("04_calculate_rsi.sql", period_id)
                
                # Step 4: Create state history table for specific period
                self.logger.info(f"Step 4: Creating state history table for period ({freq})...")
                self._execute_sql_file_with_period("05_create_state_history.sql", period_id)
            
            self.logger.info(f"Data preparation completed for period {period_id} and all frequencies")
            return True
            
        except Exception as e:
            self.logger.error(f"Error preparing data for period {period_id}: {str(e)}")
            return False
    
    def add_new_indicator(self, indicator_name: str, period_id: str) -> bool:
        """Add a new indicator calculation for a specific period"""
        try:
            self.logger.info(f"Adding new indicator {indicator_name} for period {period_id}")
            
            # This would be implemented based on the specific indicator
            # For now, just log the request
            self.logger.info(f"Indicator {indicator_name} calculation not yet implemented")
            return False
            
        except Exception as e:
            self.logger.error(f"Error adding indicator {indicator_name}: {str(e)}")
            return False
    
    def _run_python_macd(self, frequency: str = None):
        """Run Python MACD calculation for all periods with optional frequency"""
        import subprocess
        import sys
        
        macd_script = self.indicator_dir / "macd_python.py"
        if not macd_script.exists():
            raise FileNotFoundError(f"MACD Python script not found: {macd_script}")
        
        try:
            cmd = [sys.executable, str(macd_script), "--all"]
            if frequency:
                cmd.extend(["--frequency", frequency])
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            self.logger.info(f"Python MACD calculation completed successfully{' for ' + frequency if frequency else ''}")
            if result.stdout:
                self.logger.info(f"MACD output: {result.stdout}")
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Python MACD calculation failed: {e}")
            self.logger.error(f"Error output: {e.stderr}")
            raise

    def _run_python_macd_period(self, period_id: str, frequency: str = None):
        """Run Python MACD calculation for a specific period and optional frequency"""
        import subprocess
        import sys
        
        macd_script = self.indicator_dir / "macd_python.py"
        if not macd_script.exists():
            raise FileNotFoundError(f"MACD Python script not found: {macd_script}")
        
        try:
            cmd = [sys.executable, str(macd_script), "--period", period_id]
            if frequency:
                cmd.extend(["--frequency", frequency])
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            self.logger.info(f"Python MACD calculation completed successfully for period {period_id}{' and ' + frequency if frequency else ''}")
            if result.stdout:
                self.logger.info(f"MACD output: {result.stdout}")
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Python MACD calculation failed for period {period_id}: {e}")
            self.logger.error(f"Error output: {e.stderr}")
            raise

    def _execute_sql_file(self, filename: str):
        """Execute a SQL file"""
        sql_file = self.indicator_dir / filename
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                conn.commit()
        finally:
            conn.close()
    
    def _execute_sql_file_with_period(self, filename: str, period_id: str):
        """Execute a SQL file with period filtering"""
        sql_file = self.indicator_dir / filename
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Add period filtering to the SQL
        if "FROM ticker_data_seconds" in sql_content:
            sql_content = sql_content.replace(
                "FROM ticker_data_seconds s",
                f"FROM ticker_data_seconds s WHERE s.period_id = '{period_id}'"
            )
        
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                conn.commit()
        finally:
            conn.close()
    
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
    
    def get_preparation_status(self) -> Dict[str, Any]:
        """Get status of data preparation for all periods and frequencies"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Check state history table
                cursor.execute("""
                    SELECT period_id, frequency, COUNT(*) as record_count
                    FROM state_history
                    GROUP BY period_id, frequency
                    ORDER BY period_id, frequency
                """)
                state_counts = {}
                for row in cursor.fetchall():
                    period_id, freq, count = row
                    if period_id not in state_counts:
                        state_counts[period_id] = {}
                    state_counts[period_id][freq] = count
                
                # Check individual indicator tables
                cursor.execute("""
                    SELECT period_id, frequency, COUNT(*) as record_count
                    FROM macd_indicators
                    GROUP BY period_id, frequency
                    ORDER BY period_id, frequency
                """)
                macd_counts = {}
                for row in cursor.fetchall():
                    period_id, freq, count = row
                    if period_id not in macd_counts:
                        macd_counts[period_id] = {}
                    macd_counts[period_id][freq] = count
                
                cursor.execute("""
                    SELECT period_id, frequency, COUNT(*) as record_count
                    FROM bb_indicators
                    GROUP BY period_id, frequency
                    ORDER BY period_id, frequency
                """)
                bb_counts = {}
                for row in cursor.fetchall():
                    period_id, freq, count = row
                    if period_id not in bb_counts:
                        bb_counts[period_id] = {}
                    bb_counts[period_id][freq] = count
                
                cursor.execute("""
                    SELECT period_id, frequency, COUNT(*) as record_count
                    FROM rsi_indicators
                    GROUP BY period_id, frequency
                    ORDER BY period_id, frequency
                """)
                rsi_counts = {}
                for row in cursor.fetchall():
                    period_id, freq, count = row
                    if period_id not in rsi_counts:
                        rsi_counts[period_id] = {}
                    rsi_counts[period_id][freq] = count
                
                return {
                    'state_history': state_counts,
                    'macd_indicators': macd_counts,
                    'bb_indicators': bb_counts,
                    'rsi_indicators': rsi_counts
                }
        finally:
            conn.close()

def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Preparation for Backtesting')
    parser.add_argument('--period', help='Specific period ID to prepare')
    parser.add_argument('--all', action='store_true', help='Prepare all periods')
    parser.add_argument('--status', action='store_true', help='Show preparation status')
    parser.add_argument('--frequencies', help='Comma-separated list of frequencies to process (e.g. 15m,30m,1h)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get database config strictly from environment
    db_config = get_db_config(strict=True)
    
    # Parse frequencies if provided
    frequencies = None
    if args.frequencies:
        frequencies = [f.strip() for f in args.frequencies.split(',')]
        valid_freqs = {'15m', '30m', '1h'}
        if not all(f in valid_freqs for f in frequencies):
            print("❌ Invalid frequencies. Must be one or more of: 15m, 30m, 1h")
            sys.exit(1)
    
    prep = DataPreparation(db_config)
    
    if args.status:
        status = prep.get_preparation_status()
        print("\n📊 Data Preparation Status:")
        print("=" * 50)
        for table, period_data in status.items():
            print(f"\n{table}:")
            for period_id, freq_data in period_data.items():
                print(f"  {period_id}:")
                for freq, count in freq_data.items():
                    print(f"    {freq}: {count:,} records")
    elif args.period:
        success = prep.prepare_period_data(args.period, frequencies=frequencies)
        if success:
            print(f"✅ Successfully prepared data for period: {args.period}")
        else:
            print(f"❌ Failed to prepare data for period: {args.period}")
    elif args.all:
        success = prep.prepare_all_periods(frequencies=frequencies)
        if success:
            print("✅ Successfully prepared data for all periods")
        else:
            print("❌ Failed to prepare data for some periods")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
