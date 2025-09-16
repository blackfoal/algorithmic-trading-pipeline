#!/usr/bin/env python3
"""
Data Preparation for Backtesting
Orchestrates the creation of indicator base table and calculation of all indicators
"""

import os
import psycopg2
import logging
from typing import List, Dict, Any
from pathlib import Path

class DataPreparation:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        self.indicator_dir = Path(__file__).parent / "indicator_calculation"
        
    def prepare_all_data(self) -> bool:
        """Prepare all indicator data for all periods"""
        try:
            self.logger.info("Starting data preparation for all periods")
            
            # Step 1: Calculate MACD indicators using Python
            self.logger.info("Step 1: Calculating MACD indicators using Python...")
            self._run_python_macd()
            
            # Step 2: Calculate Bollinger Bands
            self.logger.info("Step 2: Calculating Bollinger Bands...")
            self._execute_sql_file("03_calculate_bollinger_bands.sql")
            
            # Step 3: Calculate RSI indicators
            self.logger.info("Step 3: Calculating RSI indicators...")
            self._execute_sql_file("04_calculate_rsi.sql")
            
            # Step 4: Create state history table
            self.logger.info("Step 4: Creating state history table...")
            self._execute_sql_file("05_create_state_history.sql")
            
            self.logger.info("Data preparation completed for all periods")
            return True
            
        except Exception as e:
            self.logger.error(f"Error preparing data: {str(e)}")
            return False
    
    def prepare_all_periods(self) -> bool:
        """Prepare data for all periods - now just calls prepare_all_data"""
        return self.prepare_all_data()
    
    def prepare_period_data(self, period_id: str) -> bool:
        """Prepare indicator data for a specific period only"""
        try:
            self.logger.info(f"Starting data preparation for period: {period_id}")
            
            # Step 1: Calculate MACD indicators for specific period using Python
            self.logger.info("Step 1: Calculating MACD indicators for period using Python...")
            self._run_python_macd_period(period_id)
            
            # Step 2: Calculate Bollinger Bands for specific period
            self.logger.info("Step 2: Calculating Bollinger Bands for period...")
            self._execute_sql_file_with_period("03_calculate_bollinger_bands.sql", period_id)
            
            # Step 3: Calculate RSI indicators for specific period
            self.logger.info("Step 3: Calculating RSI indicators for period...")
            self._execute_sql_file_with_period("04_calculate_rsi.sql", period_id)
            
            # Step 4: Create state history table for specific period
            self.logger.info("Step 4: Creating state history table for period...")
            self._execute_sql_file_with_period("05_create_state_history.sql", period_id)
            
            self.logger.info(f"Data preparation completed for period: {period_id}")
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
    
    def _run_python_macd(self):
        """Run Python MACD calculation for all periods"""
        import subprocess
        import sys
        
        macd_script = self.indicator_dir / "macd_python.py"
        if not macd_script.exists():
            raise FileNotFoundError(f"MACD Python script not found: {macd_script}")
        
        try:
            result = subprocess.run([
                sys.executable, str(macd_script), "--all"
            ], capture_output=True, text=True, check=True)
            
            self.logger.info("Python MACD calculation completed successfully")
            if result.stdout:
                self.logger.info(f"MACD output: {result.stdout}")
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Python MACD calculation failed: {e}")
            self.logger.error(f"Error output: {e.stderr}")
            raise

    def _run_python_macd_period(self, period_id: str):
        """Run Python MACD calculation for a specific period"""
        import subprocess
        import sys
        
        macd_script = self.indicator_dir / "macd_python.py"
        if not macd_script.exists():
            raise FileNotFoundError(f"MACD Python script not found: {macd_script}")
        
        try:
            result = subprocess.run([
                sys.executable, str(macd_script), "--period", period_id
            ], capture_output=True, text=True, check=True)
            
            self.logger.info(f"Python MACD calculation completed successfully for period {period_id}")
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
        """Get status of data preparation for all periods"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Skip indicator_base table check - no longer needed
                
                # Check state history table
                cursor.execute("""
                    SELECT period_id, COUNT(*) as record_count
                    FROM state_history
                    GROUP BY period_id
                    ORDER BY period_id
                """)
                state_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Check individual indicator tables
                cursor.execute("""
                    SELECT period_id, COUNT(*) as record_count
                    FROM macd_indicators
                    GROUP BY period_id
                    ORDER BY period_id
                """)
                macd_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                cursor.execute("""
                    SELECT period_id, COUNT(*) as record_count
                    FROM bb_indicators
                    GROUP BY period_id
                    ORDER BY period_id
                """)
                bb_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                cursor.execute("""
                    SELECT period_id, COUNT(*) as record_count
                    FROM rsi_indicators
                    GROUP BY period_id
                    ORDER BY period_id
                """)
                rsi_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
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
    
    prep = DataPreparation(db_config)
    
    if args.status:
        status = prep.get_preparation_status()
        print("\n📊 Data Preparation Status:")
        print("=" * 50)
        for table, counts in status.items():
            print(f"\n{table}:")
            for period_id, count in counts.items():
                print(f"  {period_id}: {count:,} records")
    elif args.period:
        success = prep.prepare_period_data(args.period)
        if success:
            print(f"✅ Successfully prepared data for period: {args.period}")
        else:
            print(f"❌ Failed to prepare data for period: {args.period}")
    elif args.all:
        success = prep.prepare_all_periods()
        if success:
            print("✅ Successfully prepared data for all periods")
        else:
            print("❌ Failed to prepare data for some periods")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
