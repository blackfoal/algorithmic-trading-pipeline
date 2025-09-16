# Backtesting Module

A comprehensive backtesting system for algorithmic trading strategies that mirrors your live trading environment.

## 🎯 **Key Features**

- **Mirrors Live Environment**: Uses same 200-minute price buffer + current second price logic
- **Period-Based Testing**: Test strategies on specific market regimes and events
- **Multiple Strategy Support**: Run and compare multiple strategies
- **Comprehensive Metrics**: PnL, win rate, Sharpe ratio, max drawdown, etc.
- **Flexible Configuration**: YAML-based period configuration
- **Automatic Data Management**: Symbol addition with automatic backfill

## 🏗️ **Architecture**

```
backtesting/
├── data-prep-backfill/          # Data preparation module
│   ├── config/
│   │   └── periods.yaml         # Period definitions
│   ├── database/
│   │   └── init.sql             # Database schema
│   ├── indicator_calculation/
│   │   ├── 03_calculate_bollinger_bands.sql
│   │   ├── 04_calculate_rsi.sql
│   │   ├── 05_create_state_history.sql
│   │   └── macd_python.py
│   ├── backfill_data.py         # Historical data backfill
│   ├── data_preparation.py      # Indicator calculation orchestration
│   ├── period_manager.py        # Period management
│   └── symbol_manager.py        # Symbol management & backfill
├── strategy_interface.py        # Base strategy class
├── backtest_engine.py           # Main backtesting engine
├── run_backtest.py             # CLI interface
└── requirements.txt             # Dependencies
```

## 📊 **Data Preparation Architecture**

### **1. Base Data Structure**
- **`ticker_data_seconds`**: 1-second OHLCV data for all periods
- **`ticker_data_minutes`**: 1-minute OHLCV data + 200 periods before
- **`indicator_base`**: Joined seconds + minutes data for calculations

### **2. Indicator Calculations**
- **Individual SQL tables** for each indicator (MACD, Bollinger Bands, RSI)
- **SQL-based calculations** using joined base data
- **Incremental updates** - only calculate missing indicators

### **3. State History Table**
- **Single source of truth** for backtesting
- **Pre-calculated indicators** at second frequency
- **Instant access** during simulation

## 📊 **Data Structure**

- **1-Minute Data**: 200-candle price buffer (same as live environment)
- **1-Second Data**: Current price simulation
- **Pre-calculated Indicators**: MACD, Bollinger Bands, etc.
- **Strategy Results**: Performance metrics and trade history

## 🚀 **Quick Start**

### 1. Setup Environment

```bash
# Install dependencies
pip install -r backtesting/requirements.txt

# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_USER=your_user
export POSTGRES_PASSWORD=your_password
export SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT
```

### 2. Initialize Database

```bash
# Sync periods from config
python backtesting/run_backtest.py sync-periods
```

### 3. Backfill Historical Data

```bash
# Check backfill status
python backtesting/run_backtest.py backfill-data --status

# Backfill data for all periods
python backtesting/run_backtest.py backfill-data --all

# Backfill data for specific period
python backtesting/run_backtest.py backfill-data --period pre_etf_approval_grind

# Add new symbols (automatically backfills data)
python backtesting/run_backtest.py add-symbol SOLUSDT
```

### 4. Prepare Indicator Data

```bash
# Check data preparation status
python backtesting/run_backtest.py prepare-data --status

# Prepare all indicator data (calculates for all periods at once)
python backtesting/run_backtest.py prepare-data

# Prepare data for specific period only (efficient for new periods)
python backtesting/run_backtest.py prepare-data --period pre_etf_approval_grind
```

### 5. Run Backtests

```bash
# List available periods
python backtesting/run_backtest.py list-periods

# Run single strategy on specific period
python backtesting/run_backtest.py run-strategy bollinger_macd pre_etf_approval_grind

# Compare multiple strategies on one period
python backtesting/run_backtest.py compare-strategies pre_etf_approval_grind
```

## 📝 **Configuration**

### Periods Configuration (`config/periods.yaml`)

```yaml
periods:
  - id: "bull_market_q1_2024"
    name: "Bull Market Q1 2024"
    start_time: "2024-01-01 00:00:00"
    end_time: "2024-03-31 23:59:59"
    tags:
      - "bull_market"
      - "high_volume"
      - "low_volatility"
    description: "Strong bullish trend with high volume and low volatility"
```

### Market Regime Tags

- **Trend**: `bull_market`, `bear_market`, `sideways`
- **Volatility**: `high_volatility`, `low_volatility`, `medium_volatility`
- **Volume**: `high_volume`, `low_volume`, `medium_volume`
- **Events**: `event`, `fed_announcement`, `crash_event`, etc.

## 🔧 **Creating Custom Strategies**

```python
from backtesting.strategy_interface import BaseStrategy, StrategyContext

class MyStrategy(BaseStrategy):
    def __init__(self, **kwargs):
        super().__init__("my_strategy", "My Custom Strategy", **kwargs)
    
    def should_open_position(self, context: StrategyContext):
        # Your logic here
        if some_condition:
            return ('long', quantity, 'reason')
        return None
    
    def should_close_position(self, context: StrategyContext, position):
        # Your exit logic here
        return some_exit_condition
```

## 📊 **Performance Metrics**

- **Total PnL**: Total profit/loss
- **Win Rate**: Percentage of winning trades
- **Average Win/Loss**: Average profit/loss per trade
- **Max Drawdown**: Maximum peak-to-trough decline
- **Sharpe Ratio**: Risk-adjusted returns
- **Trade History**: Complete trade log with timestamps

## 🗄️ **Database Schema**

### Key Tables

- **`periods`**: Testing periods with tags
- **`ticker_data_minutes`**: 1-minute OHLCV data
- **`ticker_data_seconds`**: 1-second OHLCV data
- **`strategy_results`**: Performance metrics per strategy
- **`strategy_comparison`**: Cross-strategy comparisons

## 🔄 **Workflow**

### Data Preparation Phase (data-prep-backfill/)
1. **Define Periods**: Create periods in `data-prep-backfill/config/periods.yaml`
2. **Sync Periods**: Load periods into database
3. **Add Symbols**: Add trading symbols with automatic backfill
4. **Prepare Data**: Calculate indicators and create state_history table

### Backtesting Phase (main backtesting/)
5. **Run Backtests**: Test strategies on specific periods using state_history table
6. **Analyze Results**: Review performance metrics and trade history (via separate Streamlit module)

## 🎯 **Best Practices**

- **Test Multiple Regimes**: Test strategies across different market conditions
- **Use Realistic Data**: Ensure sufficient historical data for indicators
- **Monitor Performance**: Track key metrics over time
- **Validate Results**: Cross-check with live trading performance

## 🚨 **Important Notes**

- **Data Requirements**: Ensure sufficient historical data for 200-minute price buffer
- **Symbol Validation**: New symbols are validated against Binance before backfill
- **Rate Limiting**: Automatic rate limiting to respect exchange API limits
- **Error Handling**: Comprehensive error handling and logging

## 📈 **Example Usage**

```python
from backtesting.backtest_engine import BacktestEngine
from backtesting.strategy_interface import BollingerMACDStrategy

# Initialize engine
engine = BacktestEngine(db_config)

# Create strategy
strategy = BollingerMACDStrategy()

# Run backtest
results = engine.run_backtest(strategy, "bull_market_q1_2024", ["BTCUSDT"])

print(f"Total PnL: ${results['total_pnl']:.2f}")
print(f"Win Rate: {results['win_rate']:.1f}%")
```

## 🔧 **Troubleshooting**

### Common Issues

1. **Database Connection**: Ensure PostgreSQL is running and accessible
2. **Symbol Validation**: Check if symbol exists on Binance
3. **Data Availability**: Verify sufficient historical data for periods
4. **Rate Limiting**: Wait if hitting API rate limits

### Logs

Check `backtesting.log` for detailed execution logs and error messages.

## 📚 **API Reference**

See individual module files for detailed implementation:

- `BacktestEngine`: Main orchestration class
- `BaseStrategy`: Strategy base class
- `PeriodManager`: Period management
- `SymbolManager`: Symbol management and backfill
