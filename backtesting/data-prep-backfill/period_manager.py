"""
Period Manager for Backtesting
Handles period creation, management, and data loading from config
"""

import yaml
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

class PeriodManager:
    def __init__(self, db_config: Dict[str, Any], config_path: str = "config/periods.yaml"):
        self.db_config = db_config
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
    
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
            buffer_start, actual_start, last_week_end = self._calculate_last_week_dates()
            
            # Substitute buffer_start_time variable
            if 'buffer_start_time' in processed_period and processed_period['buffer_start_time'] == '${LAST_WEEK_BUFFER_START}':
                processed_period['buffer_start_time'] = buffer_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Substitute start_time variable
            if 'start_time' in processed_period and processed_period['start_time'] == '${LAST_WEEK_START}':
                processed_period['start_time'] = actual_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Substitute end_time variable
            if 'end_time' in processed_period and processed_period['end_time'] == '${LAST_WEEK_END}':
                processed_period['end_time'] = last_week_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # Update name with actual dates
            processed_period['name'] = f"Last Week ({actual_start.strftime('%Y-%m-%d')} to {last_week_end.strftime('%Y-%m-%d')})"
            
            # Update description with actual dates
            processed_period['description'] = f"Dynamic period representing the previous week: {actual_start.strftime('%Y-%m-%d')} to {last_week_end.strftime('%Y-%m-%d')}. Automatically updated each time the system runs."
        
        return processed_period
    
    def _calculate_last_week_dates(self) -> tuple[datetime, datetime, datetime]:
        """Calculate last week's start and end dates (today - 7 days to current time) with 48h buffer"""
        now = datetime.now()
        
        # Actual period start: 7 days ago at 00:00:00
        actual_start = now - timedelta(days=7)
        actual_start = actual_start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Buffer start: 48 hours before actual start
        buffer_start = actual_start - timedelta(hours=48)
        
        # End: current time (not end of day)
        end_date = now
        
        return buffer_start, actual_start, end_date
    
    def sync_periods_to_database(self) -> bool:
        """Sync periods from config to database"""
        try:
            periods = self.load_periods_from_config()
            if not periods:
                self.logger.warning("No periods found in config file")
                return False
            
            conn = psycopg2.connect(**self.db_config)
            try:
                with conn.cursor() as cursor:
                    for period in periods:
                        # Enforce a 200-hour warm-up buffer before the start_time
                        start_dt = datetime.fromisoformat(period['start_time'])
                        buffer_start_dt = start_dt - timedelta(hours=200)

                        # Insert or update period with computed buffer_start_time
                        cursor.execute("""
                            INSERT INTO periods (id, name, buffer_start_time, start_time, end_time, tags, description)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                name = EXCLUDED.name,
                                buffer_start_time = EXCLUDED.buffer_start_time,
                                start_time = EXCLUDED.start_time,
                                end_time = EXCLUDED.end_time,
                                tags = EXCLUDED.tags,
                                description = EXCLUDED.description,
                                updated_at = CURRENT_TIMESTAMP
                        """, (
                            period['id'],
                            period['name'],
                            buffer_start_dt,
                            start_dt,
                            datetime.fromisoformat(period['end_time']),
                            period['tags'],
                            period.get('description', '')
                        ))
                    
                    conn.commit()
                    self.logger.info(f"Successfully synced {len(periods)} periods to database")
                    return True
                    
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error syncing periods to database: {str(e)}")
            return False
    
    def get_periods(self, tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get periods from database, optionally filtered by tags"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                if tags:
                    # Filter by tags
                    tag_conditions = " OR ".join(["%s = ANY(tags)" for _ in tags])
                    cursor.execute(f"""
                        SELECT id, name, buffer_start_time, start_time, end_time, tags, description, created_at, updated_at
                        FROM periods
                        WHERE {tag_conditions}
                        ORDER BY start_time
                    """, tags)
                else:
                    # Get all periods
                    cursor.execute("""
                        SELECT id, name, buffer_start_time, start_time, end_time, tags, description, created_at, updated_at
                        FROM periods
                        ORDER BY start_time
                    """)
                
                periods = []
                for row in cursor.fetchall():
                    periods.append({
                        'id': row[0],
                        'name': row[1],
                        'buffer_start_time': row[2],
                        'start_time': row[3],
                        'end_time': row[4],
                        'tags': row[5],
                        'description': row[6],
                        'created_at': row[7],
                        'updated_at': row[8]
                    })
                
                return periods
                
        finally:
            conn.close()
    
    def get_period_by_id(self, period_id: str) -> Optional[Dict[str, Any]]:
        """Get specific period by ID"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, buffer_start_time, start_time, end_time, tags, description, created_at, updated_at
                    FROM periods
                    WHERE id = %s
                """, (period_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'name': row[1],
                        'buffer_start_time': row[2],
                        'start_time': row[3],
                        'end_time': row[4],
                        'tags': row[5],
                        'description': row[6],
                        'created_at': row[7],
                        'updated_at': row[8]
                    }
                return None
                
        finally:
            conn.close()
    
    def get_periods_by_tags(self, tags: List[str]) -> List[Dict[str, Any]]:
        """Get periods that match any of the specified tags"""
        return self.get_periods(tags=tags)
    
    def get_periods_by_regime(self, regime_type: str) -> List[Dict[str, Any]]:
        """Get periods by market regime type"""
        regime_tags = {
            'bull_market': ['bull_market'],
            'bear_market': ['bear_market'],
            'high_volatility': ['high_volatility'],
            'low_volatility': ['low_volatility'],
            'high_volume': ['high_volume'],
            'low_volume': ['low_volume'],
            'event': ['event']
        }
        
        if regime_type not in regime_tags:
            self.logger.warning(f"Unknown regime type: {regime_type}")
            return []
        
        return self.get_periods_by_tags(regime_tags[regime_type])
    
    def create_custom_period(self, period_id: str, name: str, buffer_start_time: str, start_time: str, end_time: str, 
                           tags: List[str], description: str = "") -> bool:
        """Create a custom period programmatically"""
        try:
            conn = psycopg2.connect(**self.db_config)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO periods (id, name, buffer_start_time, start_time, end_time, tags, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            buffer_start_time = EXCLUDED.buffer_start_time,
                            start_time = EXCLUDED.start_time,
                            end_time = EXCLUDED.end_time,
                            tags = EXCLUDED.tags,
                            description = EXCLUDED.description,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        period_id,
                        name,
                        datetime.fromisoformat(buffer_start_time),
                        datetime.fromisoformat(start_time),
                        datetime.fromisoformat(end_time),
                        tags,
                        description
                    ))
                    
                    conn.commit()
                    self.logger.info(f"Created custom period: {period_id}")
                    return True
                    
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error creating custom period {period_id}: {str(e)}")
            return False
    
    def delete_period(self, period_id: str) -> bool:
        """Delete a period and all associated data"""
        try:
            conn = psycopg2.connect(**self.db_config)
            try:
                with conn.cursor() as cursor:
                    # Delete associated data first
                    cursor.execute("DELETE FROM ticker_data WHERE period_id = %s", (period_id,))
                    cursor.execute("DELETE FROM macd_indicators WHERE period_id = %s", (period_id,))
                    cursor.execute("DELETE FROM bb_indicators WHERE period_id = %s", (period_id,))
                    cursor.execute("DELETE FROM rsi_indicators WHERE period_id = %s", (period_id,))
                    cursor.execute("DELETE FROM state_history WHERE period_id = %s", (period_id,))
                    cursor.execute("DELETE FROM strategy_results WHERE period_id = %s", (period_id,))
                    
                    # Delete the period
                    cursor.execute("DELETE FROM periods WHERE id = %s", (period_id,))
                    
                    conn.commit()
                    self.logger.info(f"Deleted period: {period_id}")
                    return True
                    
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error deleting period {period_id}: {str(e)}")
            return False
    
    def get_period_statistics(self) -> Dict[str, Any]:
        """Get statistics about periods in database"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Total periods
                cursor.execute("SELECT COUNT(*) FROM periods")
                total_periods = cursor.fetchone()[0]
                
                # Periods by tag
                cursor.execute("""
                    SELECT unnest(tags) as tag, COUNT(*) as count
                    FROM periods
                    GROUP BY unnest(tags)
                    ORDER BY count DESC
                """)
                tag_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Date range
                cursor.execute("""
                    SELECT MIN(start_time), MAX(end_time)
                    FROM periods
                """)
                date_range = cursor.fetchone()
                
                return {
                    'total_periods': total_periods,
                    'tag_counts': tag_counts,
                    'date_range': {
                        'start': date_range[0],
                        'end': date_range[1]
                    }
                }
                
        finally:
            conn.close()
