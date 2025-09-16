"""
Main Backtesting Engine
Orchestrates the entire backtesting process
"""

import os
import json
import psycopg2
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from collections import deque

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from strategies.base import BaseStrategy
from data_prep_backfill.period_manager import PeriodManager
from data_prep_backfill.symbol_manager import SymbolManager

class BacktestEngine:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.period_manager = PeriodManager(db_config)
        self.symbol_manager = SymbolManager(db_config)
        self.logger = logging.getLogger(__name__)
        
        # Database is already initialized by Docker container
        # No need to run init.sql as the schema is persistent
    
    def _init_database(self):
        """Initialize backtesting database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                # Read and execute init.sql
                with open('database/init.sql', 'r') as f:
                    init_sql = f.read()
                    cursor.execute(init_sql)
                conn.commit()
            conn.close()
            self.logger.info("Backtesting database initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def sync_periods(self) -> bool:
        """Sync periods from config to database"""
        return self.period_manager.sync_periods_to_database()
    
    def add_symbol(self, symbol: str) -> bool:
        """Add new symbol and backfill data for all periods"""
        return self.symbol_manager.add_symbol(symbol)
    
    def run_backtest(self, strategy: BaseStrategy, period_id: str, 
                    symbols: Optional[List[str]] = None, capital_mode: str = "fixed") -> Dict[str, Any]:
        """Run backtest for a single strategy on a specific period"""
        try:
            # Get period information
            period = self.period_manager.get_period_by_id(period_id)
            if not period:
                raise ValueError(f"Period {period_id} not found")
            
            # Get symbols to test
            if symbols is None:
                symbols = self.symbol_manager.get_symbols()
            
            self.logger.info(f"Running backtest for strategy {strategy.strategy_name} on period {period_id}")
            self.logger.info(f"Testing symbols: {symbols}")
            self.logger.info(f"Capital mode: {capital_mode}")
            
            # Create a new strategy instance with the specified capital mode
            strategy_instance = strategy.__class__(capital_mode=capital_mode)
            strategy_instance.reset()
            
            # Run simulation for each symbol and save individual results
            symbol_results = {}
            for symbol in symbols:
                # Reset strategy for each symbol to avoid cross-contamination
                strategy_instance.reset()
                self._simulate_symbol(strategy_instance, symbol, period)
                
                # Get performance metrics for this symbol only
                symbol_metrics = strategy_instance.get_performance_metrics()
                symbol_results[symbol] = symbol_metrics
                
                # Save individual symbol results
                self._save_symbol_results(strategy_instance, period_id, symbol, symbol_metrics, period)
            
            # Calculate overall metrics across all symbols
            overall_metrics = self._calculate_overall_metrics(symbol_results)
            
            self.logger.info(f"Backtest completed. Total PnL: {overall_metrics['total_pnl']:.2f}")
            return overall_metrics
            
        except Exception as e:
            self.logger.error(f"Error running backtest: {str(e)}")
            raise
    
    def run_multiple_strategies(self, strategies: List[BaseStrategy], period_id: str,
                              symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run backtest for multiple strategies"""
        try:
            results = {}
            
            # Run each strategy
            for strategy in strategies:
                self.logger.info(f"Running strategy: {strategy.strategy_name}")
                strategy_results = self.run_backtest(strategy, period_id, symbols)
                results[strategy.strategy_id] = {
                    'strategy_name': strategy.strategy_name,
                    'metrics': strategy_results,
                    'trade_history': [trade.to_dict() for trade in strategy.trade_history]
                }
            
            # Generate comparison metrics
            comparison = self._generate_comparison_metrics(results)
            
            # Save comparison to database
            self._save_comparison_results(period_id, list(results.keys()), comparison)
            
            return {
                'individual_results': results,
                'comparison': comparison
            }
            
        except Exception as e:
            self.logger.error(f"Error running multiple strategies: {str(e)}")
            raise
    
    def _simulate_symbol(self, strategy: BaseStrategy, symbol: str, period: Dict[str, Any]):
        """Simulate trading for a single symbol during a period"""
        start_time = period['start_time']
        end_time = period['end_time']
        period_id = period['id']
        
        # Get 1-minute data for the period
        minute_data = self._get_minute_data(symbol, start_time, end_time, period_id)
        if not minute_data:
            self.logger.warning(f"No minute data found for {symbol} in period {period_id}")
            return
        
        # Get initial price buffer (200 minute candles before start time)
        price_buffer = self._get_price_buffer(symbol, start_time, period_id)
        if len(price_buffer) < 200:
            self.logger.warning(f"Insufficient price buffer for {symbol} in period {period_id}")
            return
        
        # Convert to deque for efficient operations
        price_buffer_deque = deque(price_buffer, maxlen=200)
        
        # Simulate each minute
        for i, (timestamp, price_data) in enumerate(minute_data):
            current_price = price_data['close']
            
            # Update price buffer with current price
            price_buffer_deque.append(current_price)
            
            # Calculate indicators (using same logic as live environment)
            indicators = self._calculate_indicators(list(price_buffer_deque), current_price)
            
            # Create strategy context
            context = StrategyContext(
                symbol=symbol,
                timestamp=timestamp,
                current_price=current_price,
                price_buffer=list(price_buffer_deque),
                indicators=indicators
            )
            
            # Update existing positions
            strategy.update_positions(context)
            
            # Check if we should close existing positions
            if symbol in strategy.positions:
                position = strategy.positions[symbol]
                if strategy.should_close_position(context, position):
                    trade = strategy.close_position(symbol, current_price, timestamp, "Signal-based exit", indicators)
                    if trade:
                        self.logger.debug(f"Closed position: {trade.to_dict()}")
            
            # Check if we should open new positions
            elif strategy.should_open_position(context) is not None:
                result = strategy.should_open_position(context)
                if result is not None:
                    side, quantity, reason, trigger_rule, indicators = result
                    if strategy.open_position(symbol, side, current_price, quantity, timestamp, reason, indicators, trigger_rule):
                        self.logger.debug(f"Opened {side} position for {symbol}: {quantity} @ {current_price} (Rule: {trigger_rule})")
    
    def _get_minute_data(self, symbol: str, start_time: datetime, end_time: datetime, period_id: str) -> List[tuple]:
        """Get 1-minute data with pre-calculated indicators from state history"""
        # Convert symbol from BTC/USDT format to BTCUSDT format for database lookup
        db_symbol = symbol.replace('/', '').upper()
        
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT ts, close,
                           macd_line, macd_signal, macd_histogram,
                           bb_upper, bb_middle, bb_lower, bb_z,
                           rsi_7, rsi_14, rsi_30
                    FROM state_history
                    WHERE symbol = %s AND period_id = %s 
                    AND ts >= %s AND ts <= %s
                    ORDER BY ts
                """, (db_symbol, period_id, start_time, end_time))
                
                data = []
                for row in cursor.fetchall():
                    data.append((
                        row[0],  # timestamp
                        {
                            'close': float(row[1]),
                            'indicators': {
                                'macd_line': float(row[2]) if row[2] else None,
                                'macd_signal': float(row[3]) if row[3] else None,
                                'macd_histogram': float(row[4]) if row[4] else None,
                                'bb_upper': float(row[5]) if row[5] else None,
                                'bb_middle': float(row[6]) if row[6] else None,
                                'bb_lower': float(row[7]) if row[7] else None,
                                'bb_z': float(row[8]) if row[8] else None,
                                'rsi_7': float(row[9]) if row[9] else None,
                                'rsi_14': float(row[10]) if row[10] else None,
                                'rsi_30': float(row[11]) if row[11] else None
                            }
                        }
                    ))
                
                return data
                
        finally:
            conn.close()
    
    def _get_second_data(self, symbol: str, start_time: datetime, end_time: datetime, period_id: str) -> List[tuple]:
        """Get 1-second data with pre-calculated indicators from state history (deprecated - use _get_minute_data)"""
        # For backward compatibility, redirect to minute data
        return self._get_minute_data(symbol, start_time, end_time, period_id)
    
    def _get_price_buffer(self, symbol: str, start_time: datetime, period_id: str) -> List[float]:
        """Get 200 minute candles before start time for price buffer"""
        # Convert symbol from BTC/USDT format to BTCUSDT format for database lookup
        db_symbol = symbol.replace('/', '').upper()
        
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Get 200 minute candles before start_time from historical data
                buffer_start = start_time - timedelta(minutes=200)
                
                cursor.execute("""
                    SELECT close
                    FROM ticker_data_historical
                    WHERE symbol = %s AND period_id = %s 
                    AND ts >= %s AND ts < %s
                    ORDER BY ts DESC
                    LIMIT 200
                """, (db_symbol, period_id, buffer_start, start_time))
                
                prices = [float(row[0]) for row in cursor.fetchall()]
                return prices
                
        finally:
            conn.close()
    
    def _calculate_indicators(self, price_buffer: List[float], current_price: float) -> Dict[str, Any]:
        """Calculate indicators using the same logic as live environment"""
        # This should use the same indicator calculation logic as signals-consumer
        # For now, we'll implement a simplified version
        
        extended_prices = price_buffer + [current_price]
        prices_array = np.array(extended_prices)
        
        # Calculate MACD (simplified)
        ema_fast = self._calculate_ema(prices_array, 12)
        ema_slow = self._calculate_ema(prices_array, 26)
        macd_line = ema_fast[-1] - ema_slow[-1]
        
        # Calculate MACD signal (9-period EMA of MACD)
        macd_values = ema_fast - ema_slow
        macd_signal = self._calculate_ema(macd_values, 9)[-1]
        macd_histogram = macd_line - macd_signal
        
        # Calculate Bollinger Bands
        bb_period = 20
        if len(extended_prices) >= bb_period:
            recent_prices = extended_prices[-bb_period:]
            middle = np.mean(recent_prices)
            std = np.std(recent_prices)
            bb_upper = middle + (2 * std)
            bb_lower = middle - (2 * std)
        else:
            middle = np.mean(extended_prices)
            std = np.std(extended_prices)
            bb_upper = middle + (2 * std)
            bb_lower = middle - (2 * std)
        
        return {
            'macd_line': float(macd_line),
            'macd_signal': float(macd_signal),
            'macd_histogram': float(macd_histogram),
            'bb_upper': float(bb_upper),
            'bb_middle': float(middle),
            'bb_lower': float(bb_lower)
        }
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """Calculate EMA using the same logic as signals-consumer"""
        if len(prices) < period:
            return np.full(len(prices), prices[-1] if len(prices) > 0 else 0.0)
        
        ema_values = np.zeros(len(prices))
        sma = np.mean(prices[:period])
        ema_values[:period] = sma
        
        multiplier = 2.0 / (period + 1)
        ema = sma
        
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
            ema_values[i] = ema
        
        return ema_values
    
    def _save_strategy_results(self, strategy: BaseStrategy, period_id: str, 
                             symbols: List[str], metrics: Dict[str, Any]):
        """Save strategy results to database"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                # Get period info for simulation metadata
                period = self.period_manager.get_period_by_id(period_id)
                
                for symbol in symbols:
                    cursor.execute("""
                        INSERT INTO strategy_results 
                        (strategy_id, strategy_name, period_id, symbol, total_pnl, total_trades,
                         winning_trades, losing_trades, win_rate, avg_win, avg_loss, max_drawdown,
                         sharpe_ratio, trade_history, simulation_start, simulation_end, simulation_duration_seconds)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (strategy_id, period_id, symbol) DO UPDATE SET
                            total_pnl = EXCLUDED.total_pnl,
                            total_trades = EXCLUDED.total_trades,
                            winning_trades = EXCLUDED.winning_trades,
                            losing_trades = EXCLUDED.losing_trades,
                            win_rate = EXCLUDED.win_rate,
                            avg_win = EXCLUDED.avg_win,
                            avg_loss = EXCLUDED.avg_loss,
                            max_drawdown = EXCLUDED.max_drawdown,
                            sharpe_ratio = EXCLUDED.sharpe_ratio,
                            trade_history = EXCLUDED.trade_history,
                            simulation_start = EXCLUDED.simulation_start,
                            simulation_end = EXCLUDED.simulation_end,
                            simulation_duration_seconds = EXCLUDED.simulation_duration_seconds
                    """, (
                        strategy.strategy_id,
                        strategy.strategy_name,
                        period_id,
                        symbol,
                        metrics['total_pnl'],
                        metrics['total_trades'],
                        metrics['winning_trades'],
                        metrics['losing_trades'],
                        metrics['win_rate'],
                        metrics['avg_win'],
                        metrics['avg_loss'],
                        metrics['max_drawdown'],
                        metrics['sharpe_ratio'],
                        json.dumps([trade.to_dict() for trade in strategy.trade_history]),
                        period['start_time'],
                        period['end_time'],
                        int((period['end_time'] - period['start_time']).total_seconds())
                    ))
                
                conn.commit()
                
        finally:
            conn.close()
    
    def _save_symbol_results(self, strategy: BaseStrategy, period_id: str, symbol: str, 
                           metrics: Dict[str, Any], period: Dict[str, Any]):
        """Save individual symbol results to database"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO strategy_results 
                    (strategy_id, strategy_name, period_id, symbol, total_pnl, total_trades,
                     winning_trades, losing_trades, win_rate, avg_win, avg_loss, max_drawdown,
                     sharpe_ratio, trade_history, simulation_start, simulation_end, simulation_duration_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (strategy_id, period_id, symbol) DO UPDATE SET
                        total_pnl = EXCLUDED.total_pnl,
                        total_trades = EXCLUDED.total_trades,
                        winning_trades = EXCLUDED.winning_trades,
                        losing_trades = EXCLUDED.losing_trades,
                        win_rate = EXCLUDED.win_rate,
                        avg_win = EXCLUDED.avg_win,
                        avg_loss = EXCLUDED.avg_loss,
                        max_drawdown = EXCLUDED.max_drawdown,
                        sharpe_ratio = EXCLUDED.sharpe_ratio,
                        trade_history = EXCLUDED.trade_history,
                        simulation_start = EXCLUDED.simulation_start,
                        simulation_end = EXCLUDED.simulation_end,
                        simulation_duration_seconds = EXCLUDED.simulation_duration_seconds
                """, (
                    strategy.strategy_id,
                    strategy.strategy_name,
                    period_id,
                    symbol,
                    metrics['total_pnl'],
                    metrics['total_trades'],
                    metrics['winning_trades'],
                    metrics['losing_trades'],
                    metrics['win_rate'],
                    metrics['avg_win'],
                    metrics['avg_loss'],
                    metrics['max_drawdown'],
                    metrics['sharpe_ratio'],
                    json.dumps([trade.to_dict() for trade in strategy.trade_history]),
                    period['start_time'],
                    period['end_time'],
                    int((period['end_time'] - period['start_time']).total_seconds())
                ))
                
                conn.commit()
                
        finally:
            conn.close()
    
    def _calculate_overall_metrics(self, symbol_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate overall metrics across all symbols"""
        total_pnl = sum(result['total_pnl'] for result in symbol_results.values())
        total_trades = sum(result['total_trades'] for result in symbol_results.values())
        total_wins = sum(result['winning_trades'] for result in symbol_results.values())
        total_losses = sum(result['losing_trades'] for result in symbol_results.values())
        
        avg_win = sum(result['avg_win'] * result['winning_trades'] for result in symbol_results.values()) / total_wins if total_wins > 0 else 0
        avg_loss = sum(result['avg_loss'] * result['losing_trades'] for result in symbol_results.values()) / total_losses if total_losses > 0 else 0
        win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
        
        # Calculate max drawdown across all symbols
        max_drawdown = max(result['max_drawdown'] for result in symbol_results.values()) if symbol_results else 0
        
        # Calculate average Sharpe ratio
        avg_sharpe = sum(result['sharpe_ratio'] for result in symbol_results.values()) / len(symbol_results) if symbol_results else 0
        
        return {
            'total_pnl': total_pnl,
            'total_trades': total_trades,
            'winning_trades': total_wins,
            'losing_trades': total_losses,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': avg_sharpe
        }
    
    def _generate_comparison_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comparison metrics for multiple strategies"""
        comparison = {
            'best_pnl': None,
            'best_win_rate': None,
            'best_sharpe': None,
            'worst_drawdown': None,
            'strategy_rankings': []
        }
        
        best_pnl = float('-inf')
        best_win_rate = 0.0
        best_sharpe = float('-inf')
        worst_drawdown = float('inf')
        
        for strategy_id, data in results.items():
            metrics = data['metrics']
            
            if metrics['total_pnl'] > best_pnl:
                best_pnl = metrics['total_pnl']
                comparison['best_pnl'] = strategy_id
            
            if metrics['win_rate'] > best_win_rate:
                best_win_rate = metrics['win_rate']
                comparison['best_win_rate'] = strategy_id
            
            if metrics['sharpe_ratio'] > best_sharpe:
                best_sharpe = metrics['sharpe_ratio']
                comparison['best_sharpe'] = strategy_id
            
            if metrics['max_drawdown'] < worst_drawdown:
                worst_drawdown = metrics['max_drawdown']
                comparison['worst_drawdown'] = strategy_id
        
        # Create rankings
        strategy_rankings = []
        for strategy_id, data in results.items():
            strategy_rankings.append({
                'strategy_id': strategy_id,
                'strategy_name': data['strategy_name'],
                'total_pnl': data['metrics']['total_pnl'],
                'win_rate': data['metrics']['win_rate'],
                'sharpe_ratio': data['metrics']['sharpe_ratio'],
                'max_drawdown': data['metrics']['max_drawdown']
            })
        
        # Sort by total PnL
        strategy_rankings.sort(key=lambda x: x['total_pnl'], reverse=True)
        comparison['strategy_rankings'] = strategy_rankings
        
        return comparison
    
    def _save_comparison_results(self, period_id: str, strategy_ids: List[str], comparison: Dict[str, Any]):
        """Save comparison results to database"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO strategy_comparison (comparison_id, period_id, strategy_ids, comparison_metrics)
                    VALUES (%s, %s, %s, %s)
                """, (
                    f"comparison_{period_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    period_id,
                    strategy_ids,
                    comparison
                ))
                conn.commit()
                
        finally:
            conn.close()
