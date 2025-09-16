#!/usr/bin/env python3
"""
Detailed Reporting System
Generates comprehensive reports at period_id + strategy + allocation_mode + symbol level
"""

import psycopg2
import json
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd

def get_db_config():
    """Get database configuration"""
    return {
        'host': 'postgres-backtesting',
        'port': '5432',
        'database': 'backtesting',
        'user': 'murat_binance',
        'password': '2112'
    }

def analyze_trade_details(trade_history: List[Dict], symbol: str) -> Dict[str, Any]:
    """Analyze trade details for a specific symbol"""
    if not trade_history:
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'max_win': 0.0,
            'max_loss': 0.0,
            'trigger_rules': {},
            'entry_conditions': [],
            'exit_reasons': {},
            'macd_analysis': {}
        }
    
    # Filter trades for this symbol
    symbol_trades = [t for t in trade_history if t.get('symbol') == symbol]
    
    if not symbol_trades:
        return analyze_trade_details([], symbol)
    
    # Basic metrics
    total_trades = len(symbol_trades)
    winning_trades = [t for t in symbol_trades if t.get('pnl', 0) > 0]
    losing_trades = [t for t in symbol_trades if t.get('pnl', 0) <= 0]
    
    win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0.0
    total_pnl = sum(t.get('pnl', 0) for t in symbol_trades)
    
    avg_win = sum(t.get('pnl', 0) for t in winning_trades) / len(winning_trades) if winning_trades else 0.0
    avg_loss = sum(t.get('pnl', 0) for t in losing_trades) / len(losing_trades) if losing_trades else 0.0
    
    max_win = max((t.get('pnl', 0) for t in winning_trades), default=0.0)
    max_loss = min((t.get('pnl', 0) for t in losing_trades), default=0.0)
    
    # Analyze exit reasons
    exit_reasons = {}
    for trade in symbol_trades:
        reason = trade.get('reason', 'Unknown')
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    
    # Analyze trigger patterns (simplified - would need more data)
    trigger_rules = {
        'long_signals': len([t for t in symbol_trades if t.get('side') == 'buy']),
        'short_signals': len([t for t in symbol_trades if t.get('side') == 'sell'])
    }
    
    # Sample entry conditions (first few trades)
    entry_conditions = symbol_trades[:3] if symbol_trades else []
    
    # MACD analysis (placeholder - would need indicator data)
    macd_analysis = {
        'avg_macd_line': 0.0,  # Would need to calculate from indicators
        'avg_macd_signal': 0.0,
        'macd_crossovers': 0
    }
    
    return {
        'total_trades': total_trades,
        'winning_trades': len(winning_trades),
        'losing_trades': len(losing_trades),
        'win_rate': round(win_rate, 2),
        'total_pnl': round(total_pnl, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'max_win': round(max_win, 2),
        'max_loss': round(max_loss, 2),
        'trigger_rules': trigger_rules,
        'entry_conditions': entry_conditions,
        'exit_reasons': exit_reasons,
        'macd_analysis': macd_analysis
    }

def generate_detailed_report(period_id: str = None, strategy_id: str = None, allocation_mode: str = None):
    """Generate detailed report for specified dimensions"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        with conn.cursor() as cur:
            # Build query based on filters
            where_conditions = []
            params = []
            
            if period_id:
                where_conditions.append("period_id = %s")
                params.append(period_id)
            
            if strategy_id:
                where_conditions.append("strategy_id = %s")
                params.append(strategy_id)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get strategy results with trade history
            query = f"""
                SELECT 
                    period_id,
                    strategy_id,
                    strategy_name,
                    symbol,
                    total_pnl,
                    total_trades,
                    winning_trades,
                    losing_trades,
                    win_rate,
                    avg_win,
                    avg_loss,
                    max_drawdown,
                    sharpe_ratio,
                    trade_history,
                    simulation_start,
                    simulation_end
                FROM strategy_results 
                {where_clause}
                ORDER BY period_id, strategy_id, symbol
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            if not results:
                print("No data found for the specified criteria.")
                return
            
            # Group by dimensions
            report_data = {}
            
            for row in results:
                period_id, strategy_id, strategy_name, symbol, total_pnl, total_trades, winning_trades, losing_trades, win_rate, avg_win, avg_loss, max_drawdown, sharpe_ratio, trade_history_json, sim_start, sim_end = row
                
                # Parse trade history
                if trade_history_json:
                    if isinstance(trade_history_json, str):
                        trade_history = json.loads(trade_history_json)
                    else:
                        trade_history = trade_history_json
                else:
                    trade_history = []
                
                # Create dimension key
                dimension_key = f"{period_id}|{strategy_id}|{symbol}"
                
                # Analyze trade details
                trade_analysis = analyze_trade_details(trade_history, symbol)
                
                # Store report data
                report_data[dimension_key] = {
                    'period_id': period_id,
                    'strategy_id': strategy_id,
                    'strategy_name': strategy_name,
                    'symbol': symbol,
                    'allocation_mode': 'unknown',  # Would need to track this separately
                    'basic_metrics': {
                        'total_pnl': float(total_pnl),
                        'total_trades': total_trades,
                        'winning_trades': winning_trades,
                        'losing_trades': losing_trades,
                        'win_rate': float(win_rate),
                        'avg_win': float(avg_win),
                        'avg_loss': float(avg_loss),
                        'max_drawdown': float(max_drawdown),
                        'sharpe_ratio': float(sharpe_ratio)
                    },
                    'trade_analysis': trade_analysis,
                    'simulation_period': {
                        'start': sim_start.isoformat() if sim_start else None,
                        'end': sim_end.isoformat() if sim_end else None
                    }
                }
            
            # Display report
            print("=" * 100)
            print("DETAILED TRADING REPORT")
            print("=" * 100)
            print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Total Dimension Slices: {len(report_data)}")
            print()
            
            for dimension_key, data in report_data.items():
                print(f"📊 DIMENSION SLICE: {data['period_id']} | {data['strategy_id']} | {data['symbol']}")
                print("-" * 80)
                
                # Basic metrics
                metrics = data['basic_metrics']
                print(f"💰 PnL: ${metrics['total_pnl']:.2f} | Trades: {metrics['total_trades']} | Win Rate: {metrics['win_rate']:.1f}%")
                print(f"📈 Avg Win: ${metrics['avg_win']:.2f} | Avg Loss: ${metrics['avg_loss']:.2f} | Max DD: ${metrics['max_drawdown']:.2f}")
                print(f"⚡ Sharpe: {metrics['sharpe_ratio']:.3f}")
                
                # Trade analysis
                analysis = data['trade_analysis']
                print(f"🎯 Trigger Rules: Long={analysis['trigger_rules']['long_signals']}, Short={analysis['trigger_rules']['short_signals']}")
                
                # Exit reasons
                if analysis['exit_reasons']:
                    print(f"🚪 Exit Reasons: {', '.join([f'{k}({v})' for k, v in analysis['exit_reasons'].items()])}")
                
                # Sample trades
                if analysis['entry_conditions']:
                    print("📋 Sample Trades:")
                    for i, trade in enumerate(analysis['entry_conditions'][:3], 1):
                        print(f"  {i}. {trade.get('side', 'N/A')} @ ${trade.get('price', 0):.2f} | PnL: ${trade.get('pnl', 0):.2f} | Reason: {trade.get('reason', 'N/A')}")
                
                print()
            
            # Summary statistics
            print("=" * 100)
            print("SUMMARY STATISTICS")
            print("=" * 100)
            
            total_pnl = sum(data['basic_metrics']['total_pnl'] for data in report_data.values())
            total_trades = sum(data['basic_metrics']['total_trades'] for data in report_data.values())
            avg_win_rate = sum(data['basic_metrics']['win_rate'] for data in report_data.values()) / len(report_data)
            
            print(f"Total PnL Across All Slices: ${total_pnl:.2f}")
            print(f"Total Trades Across All Slices: {total_trades}")
            print(f"Average Win Rate: {avg_win_rate:.1f}%")
            print(f"Best Performing Slice: {max(report_data.items(), key=lambda x: x[1]['basic_metrics']['total_pnl'])[0]}")
            print(f"Worst Performing Slice: {min(report_data.items(), key=lambda x: x[1]['basic_metrics']['total_pnl'])[0]}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    period_id = sys.argv[1] if len(sys.argv) > 1 else None
    strategy_id = sys.argv[2] if len(sys.argv) > 2 else None
    allocation_mode = sys.argv[3] if len(sys.argv) > 3 else None
    
    generate_detailed_report(period_id, strategy_id, allocation_mode)
