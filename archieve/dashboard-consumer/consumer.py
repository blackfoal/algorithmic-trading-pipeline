import os
import json
import logging
from kafka import KafkaConsumer
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SIGNALS_TOPIC = os.getenv("SIGNALS_TOPIC", "trading-signals")

class TradingSignalsDashboard:
    def __init__(self):
        self.consumer = None
        self.signals_data = []
        self.candle_data = {}  # Store OHLCV data for candlesticks
        self.price_history = {}  # Store price history for indicators
        self.indicator_history = {}  # Store indicator history
        
    def get_consumer(self):
        """Get Kafka consumer for trading signals"""
        if not self.consumer:
            self.consumer = KafkaConsumer(
                SIGNALS_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
        return self.consumer
    
    def get_latest_signals(self, limit=50):
        """Get latest trading signals"""
        consumer = self.get_consumer()
        signals = []
        
        try:
            # Poll for new messages
            message_batch = consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    signal = message.value
                    signal['received_at'] = datetime.now().isoformat()
                    signals.append(signal)
                    
                    # Extract data from the signal structure
                    symbol = signal['symbol']
                    indicators = signal.get('indicators', {})
                    
                    if not indicators:
                        continue
                    
                    # Initialize storage for symbol if not exists
                    if symbol not in self.candle_data:
                        self.candle_data[symbol] = []
                    if symbol not in self.price_history:
                        self.price_history[symbol] = []
                    if symbol not in self.indicator_history:
                        self.indicator_history[symbol] = []
                    
                    # Store price data
                    close_price = indicators.get('close_price', 0)
                    timestamp = indicators.get('timestamp', 0)
                    
                    if close_price > 0:
                        price_data = {
                            'timestamp': timestamp,
                            'price': close_price,
                            'update_type': indicators.get('update_type', 'unknown')
                        }
                        self.price_history[symbol].append(price_data)
                        
                        # Keep only last 200 prices
                        if len(self.price_history[symbol]) > 200:
                            self.price_history[symbol] = self.price_history[symbol][-200:]
                    
                    # Store indicator data
                    if 'macd' in indicators and 'bollinger_bands' in indicators:
                        indicator_data = {
                            'timestamp': timestamp,
                            'close_price': close_price,
                            'macd': indicators['macd'],
                            'bollinger_bands': indicators['bollinger_bands'],
                            'update_type': indicators.get('update_type', 'unknown')
                        }
                        self.indicator_history[symbol].append(indicator_data)
                        
                        # Keep only last 100 indicator readings
                        if len(self.indicator_history[symbol]) > 100:
                            self.indicator_history[symbol] = self.indicator_history[symbol][-100:]
                    
                    # Create synthetic OHLCV data for candlestick chart
                    # For now, we'll create simple candles from price data
                    if len(self.price_history[symbol]) >= 2:
                        # Create a simple OHLCV from price data
                        current_price = close_price
                        prev_price = self.price_history[symbol][-2]['price'] if len(self.price_history[symbol]) > 1 else current_price
                        
                        candle = {
                            'timestamp': timestamp,
                            'open': prev_price,
                            'high': max(current_price, prev_price),
                            'low': min(current_price, prev_price),
                            'close': current_price,
                            'volume': 1000  # Synthetic volume
                        }
                        
                        # Only add if it's a new timestamp (avoid duplicates)
                        if not self.candle_data[symbol] or self.candle_data[symbol][-1]['timestamp'] != timestamp:
                            self.candle_data[symbol].append(candle)
                            
                            # Keep only last 100 candles
                            if len(self.candle_data[symbol]) > 100:
                                self.candle_data[symbol] = self.candle_data[symbol][-100:]
                    
        except Exception as e:
            logger.error(f"Error consuming signals: {e}")
            
        return signals[-limit:] if signals else []
    
    def create_binance_style_chart(self, symbol, selected_indicator="Bollinger Bands"):
        """Create Binance-style candlestick chart with indicators"""
        if symbol not in self.candle_data or not self.candle_data[symbol]:
            return go.Figure()
        
        candles = self.candle_data[symbol]
        indicator_data = self.indicator_history.get(symbol, [])
        
        # Create subplots: Main chart + Volume + Indicator
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.6, 0.2, 0.2],  # Main chart, Volume, Indicator
            subplot_titles=[f"{symbol} Price Chart", "Volume", f"{selected_indicator}"]
        )
        
        # Convert timestamps to datetime
        timestamps = [datetime.fromtimestamp(c['timestamp']/1000) for c in candles]
        
        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=timestamps,
                open=[c['open'] for c in candles],
                high=[c['high'] for c in candles],
                low=[c['low'] for c in candles],
                close=[c['close'] for c in candles],
                name="Price",
                increasing_line_color='#00ff88',
                decreasing_line_color='#ff4444'
            ),
            row=1, col=1
        )
        
        # Add selected indicator to main chart
        if selected_indicator == "Bollinger Bands":
            self._add_bollinger_bands_from_history(fig, indicator_data, row=1)
        elif selected_indicator == "EMA":
            self._add_ema(fig, candles, timestamps, row=1)
        elif selected_indicator == "SMA":
            self._add_sma(fig, candles, timestamps, row=1)
        
        # Volume chart
        fig.add_trace(
            go.Bar(
                x=timestamps,
                y=[c['volume'] for c in candles],
                name="Volume",
                marker_color='rgba(158,202,225,0.6)'
            ),
            row=2, col=1
        )
        
        # Indicator subchart
        if selected_indicator == "MACD":
            self._add_macd_subchart_from_history(fig, indicator_data, row=3)
        elif selected_indicator == "RSI":
            self._add_rsi_subchart(fig, candles, timestamps, row=3)
        elif selected_indicator == "Stochastic":
            self._add_stochastic_subchart(fig, candles, timestamps, row=3)
        
        # Update layout
        fig.update_layout(
            title=f"{symbol} - {selected_indicator}",
            xaxis_rangeslider_visible=False,
            height=800,
            showlegend=True,
            template="plotly_dark"
        )
        
        # Update axes
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
        
        return fig
    
    def _add_bollinger_bands_from_history(self, fig, indicator_data, row=1):
        """Add Bollinger Bands from stored indicator history"""
        if not indicator_data:
            return
            
        timestamps = [datetime.fromtimestamp(d['timestamp']/1000) for d in indicator_data]
        upper_bands = [d['bollinger_bands']['upper'] for d in indicator_data]
        middle_bands = [d['bollinger_bands']['middle'] for d in indicator_data]
        lower_bands = [d['bollinger_bands']['lower'] for d in indicator_data]
        
        fig.add_trace(
            go.Scatter(x=timestamps, y=upper_bands, name="BB Upper", 
                      line=dict(color='rgba(255,255,255,0.5)', width=1)),
            row=row, col=1
        )
        fig.add_trace(
            go.Scatter(x=timestamps, y=middle_bands, name="BB Middle", 
                      line=dict(color='rgba(255,255,255,0.3)', width=1)),
            row=row, col=1
        )
        fig.add_trace(
            go.Scatter(x=timestamps, y=lower_bands, name="BB Lower", 
                      line=dict(color='rgba(255,255,255,0.5)', width=1)),
            row=row, col=1
        )
    
    def _add_bollinger_bands(self, fig, candles, timestamps, row=1):
        """Add Bollinger Bands to chart"""
        closes = [c['close'] for c in candles]
        
        # Calculate Bollinger Bands (20 period, 2 std dev)
        if len(closes) >= 20:
            sma_20 = pd.Series(closes).rolling(window=20).mean()
            std_20 = pd.Series(closes).rolling(window=20).std()
            
            upper_band = sma_20 + (std_20 * 2)
            lower_band = sma_20 - (std_20 * 2)
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=upper_band, name="BB Upper", 
                          line=dict(color='rgba(255,255,255,0.5)', width=1)),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=timestamps, y=sma_20, name="BB Middle", 
                          line=dict(color='rgba(255,255,255,0.3)', width=1)),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=timestamps, y=lower_band, name="BB Lower", 
                          line=dict(color='rgba(255,255,255,0.5)', width=1)),
                row=row, col=1
            )
    
    def _add_ema(self, fig, candles, timestamps, row=1):
        """Add EMA to chart"""
        closes = [c['close'] for c in candles]
        
        # Calculate EMA (12 and 26 periods)
        if len(closes) >= 26:
            ema_12 = pd.Series(closes).ewm(span=12).mean()
            ema_26 = pd.Series(closes).ewm(span=26).mean()
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=ema_12, name="EMA 12", 
                          line=dict(color='#ff6b6b', width=2)),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=timestamps, y=ema_26, name="EMA 26", 
                          line=dict(color='#4ecdc4', width=2)),
                row=row, col=1
            )
    
    def _add_sma(self, fig, candles, timestamps, row=1):
        """Add SMA to chart"""
        closes = [c['close'] for c in candles]
        
        # Calculate SMA (20 and 50 periods)
        if len(closes) >= 50:
            sma_20 = pd.Series(closes).rolling(window=20).mean()
            sma_50 = pd.Series(closes).rolling(window=50).mean()
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=sma_20, name="SMA 20", 
                          line=dict(color='#ff6b6b', width=2)),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=timestamps, y=sma_50, name="SMA 50", 
                          line=dict(color='#4ecdc4', width=2)),
                row=row, col=1
            )
    
    def _add_macd_subchart_from_history(self, fig, indicator_data, row=3):
        """Add MACD from stored indicator history"""
        if not indicator_data:
            return
            
        timestamps = [datetime.fromtimestamp(d['timestamp']/1000) for d in indicator_data]
        macd_lines = [d['macd']['line'] for d in indicator_data]
        signal_lines = [d['macd']['signal'] for d in indicator_data]
        histograms = [d['macd']['histogram'] for d in indicator_data]
        
        fig.add_trace(
            go.Scatter(x=timestamps, y=macd_lines, name="MACD", 
                      line=dict(color='#00ff88', width=2)),
            row=row, col=1
        )
        fig.add_trace(
            go.Scatter(x=timestamps, y=signal_lines, name="Signal", 
                      line=dict(color='#ff4444', width=2)),
            row=row, col=1
        )
        fig.add_trace(
            go.Bar(x=timestamps, y=histograms, name="Histogram", 
                  marker_color=['#00ff88' if h >= 0 else '#ff4444' for h in histograms]),
            row=row, col=1
        )
    
    def _add_macd_subchart(self, fig, candles, timestamps, row=3):
        """Add MACD to subchart"""
        closes = [c['close'] for c in candles]
        
        if len(closes) >= 26:
            # Calculate MACD
            ema_12 = pd.Series(closes).ewm(span=12).mean()
            ema_26 = pd.Series(closes).ewm(span=26).mean()
            macd_line = ema_12 - ema_26
            signal_line = macd_line.ewm(span=9).mean()
            histogram = macd_line - signal_line
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=macd_line, name="MACD", 
                          line=dict(color='#00ff88', width=2)),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=timestamps, y=signal_line, name="Signal", 
                          line=dict(color='#ff4444', width=2)),
                row=row, col=1
            )
            fig.add_trace(
                go.Bar(x=timestamps, y=histogram, name="Histogram", 
                      marker_color=['#00ff88' if h >= 0 else '#ff4444' for h in histogram]),
                row=row, col=1
            )
    
    def _add_rsi_subchart(self, fig, candles, timestamps, row=3):
        """Add RSI to subchart"""
        closes = [c['close'] for c in candles]
        
        if len(closes) >= 14:
            # Calculate RSI
            delta = pd.Series(closes).diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=rsi, name="RSI", 
                          line=dict(color='#ff6b6b', width=2)),
                row=row, col=1
            )
            
            # Add RSI levels
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=row, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=row, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=row, col=1)
    
    def _add_stochastic_subchart(self, fig, candles, timestamps, row=3):
        """Add Stochastic to subchart"""
        if len(candles) >= 14:
            # Calculate Stochastic
            highs = [c['high'] for c in candles]
            lows = [c['low'] for c in candles]
            closes = [c['close'] for c in candles]
            
            k_percent = []
            for i in range(13, len(candles)):
                highest_high = max(highs[i-13:i+1])
                lowest_low = min(lows[i-13:i+1])
                k = ((closes[i] - lowest_low) / (highest_high - lowest_low)) * 100
                k_percent.append(k)
            
            # Pad with zeros for alignment
            k_percent = [0] * 13 + k_percent
            
            fig.add_trace(
                go.Scatter(x=timestamps, y=k_percent, name="%K", 
                          line=dict(color='#00ff88', width=2)),
                row=row, col=1
            )
            
            # Add Stochastic levels
            fig.add_hline(y=80, line_dash="dash", line_color="red", row=row, col=1)
            fig.add_hline(y=20, line_dash="dash", line_color="green", row=row, col=1)

def main():
    st.set_page_config(
        page_title="🚀 Binance-Style Trading Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for Binance-style dark theme
    st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stSelectbox > div > div {
        background-color: #1e1e1e;
        color: white;
    }
    .stSelectbox label {
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Initialize dashboard
    if 'dashboard' not in st.session_state:
        st.session_state.dashboard = TradingSignalsDashboard()
    
    dashboard = st.session_state.dashboard
    
    # Sidebar controls (Binance-style)
    with st.sidebar:
        st.title("🎛️ Trading Controls")
        
        # Symbol selection
        available_symbols = list(dashboard.indicator_history.keys()) if dashboard.indicator_history else []
        if not available_symbols:
            # Get symbols from latest signals
            signals_data = dashboard.get_latest_signals(limit=10)
            available_symbols = list(set([s['symbol'] for s in signals_data])) if signals_data else ['BTC/USDT']
        
        # Initialize selected symbol in session state
        if 'selected_symbol' not in st.session_state:
            st.session_state.selected_symbol = available_symbols[0] if available_symbols else 'BTC/USDT'
        
        selected_symbol = st.selectbox(
            "📊 Select Symbol",
            available_symbols,
            index=available_symbols.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in available_symbols else 0,
            key="sidebar_symbol_selector"
        )
        
        # Update session state when sidebar selection changes
        if selected_symbol != st.session_state.selected_symbol:
            st.session_state.selected_symbol = selected_symbol
        
        # Indicator selection
        indicators = [
            "Bollinger Bands",
            "EMA", 
            "SMA",
            "MACD",
            "RSI",
            "Stochastic"
        ]
        
        selected_indicator = st.selectbox(
            "📈 Select Indicator",
            indicators,
            index=0
        )
        
        # Timeframe selection
        timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
        selected_timeframe = st.selectbox(
            "⏰ Timeframe",
            timeframes,
            index=0
        )
        
        # Auto refresh controls
        st.markdown("---")
        st.subheader("🔄 Refresh Settings")
        
        auto_refresh = st.checkbox("Auto Refresh", value=True)
        refresh_interval = st.selectbox("Interval (seconds)", [1, 2, 5, 10], index=1)
        
        if st.button("🔄 Manual Refresh"):
            st.rerun()
        
        # Market info
        st.markdown("---")
        st.subheader("📊 Market Info")
        
        if st.session_state.selected_symbol in dashboard.indicator_history and dashboard.indicator_history[st.session_state.selected_symbol]:
            latest_indicator = dashboard.indicator_history[st.session_state.selected_symbol][-1]
            current_price = latest_indicator['close_price']
            
            st.metric("Current Price", f"${current_price:.2f}")
            
            # Price change (if we have previous indicator reading)
            if len(dashboard.indicator_history[st.session_state.selected_symbol]) > 1:
                prev_price = dashboard.indicator_history[st.session_state.selected_symbol][-2]['close_price']
                price_change = current_price - prev_price
                price_change_pct = (price_change / prev_price) * 100
                
                st.metric(
                    "24h Change", 
                    f"${price_change:.2f}",
                    f"{price_change_pct:.2f}%"
                )
    
    # Top symbol selector (Binance-style)
    st.markdown("---")
    
    # Get available symbols for top selector
    available_symbols_top = list(dashboard.indicator_history.keys()) if dashboard.indicator_history else []
    if not available_symbols_top:
        signals_data = dashboard.get_latest_signals(limit=10)
        available_symbols_top = list(set([s['symbol'] for s in signals_data])) if signals_data else ['BTC/USDT']
    
    # Create columns for symbol buttons
    num_cols = min(len(available_symbols_top), 6)  # Max 6 symbols per row
    cols = st.columns(num_cols)
    
    # Display symbol buttons
    for i, symbol in enumerate(available_symbols_top):
        with cols[i % num_cols]:
            if st.button(
                f"📊 {symbol}",
                key=f"symbol_btn_{symbol}",
                use_container_width=True,
                type="primary" if symbol == st.session_state.selected_symbol else "secondary"
            ):
                # Update selected symbol in session state
                st.session_state.selected_symbol = symbol
                st.rerun()
    
    # Main chart area
    st.title(f"📈 {st.session_state.selected_symbol} - {selected_timeframe}")
    
    # Get latest signals
    signals_data = dashboard.get_latest_signals(limit=100)
    
    if not signals_data:
        st.warning("⚠️ No signals data available. Make sure the signals-consumer is running.")
        st.info("💡 The dashboard will automatically update when data becomes available.")
        return
    
    # Create Binance-style chart
    if st.session_state.selected_symbol in dashboard.indicator_history and dashboard.indicator_history[st.session_state.selected_symbol]:
        chart = dashboard.create_binance_style_chart(st.session_state.selected_symbol, selected_indicator)
        st.plotly_chart(chart, use_container_width=True, height=800)
    else:
        st.info(f"📊 Loading data for {st.session_state.selected_symbol}...")
    
    # Market overview table
    st.subheader("📊 Market Overview")
    
    if signals_data:
        # Create market overview from indicator history
        overview_data = []
        
        for symbol in dashboard.indicator_history:
            if dashboard.indicator_history[symbol]:
                latest = dashboard.indicator_history[symbol][-1]
                overview_data.append({
                    'Symbol': symbol,
                    'Price': f"${latest['close_price']:.2f}",
                    'MACD': f"{latest['macd']['line']:.4f}",
                    'BB Upper': f"${latest['bollinger_bands']['upper']:.2f}",
                    'BB Lower': f"${latest['bollinger_bands']['lower']:.2f}",
                    'Status': '🟢 Live' if latest['update_type'] == 'live_update' else '📊 Minute',
                    'Last Update': datetime.fromtimestamp(latest['timestamp']/1000).strftime('%H:%M:%S')
                })
        
        if overview_data:
            df = pd.DataFrame(overview_data)
            st.dataframe(df, use_container_width=True, height=300)
        else:
            st.info("📊 No indicator data available yet. Waiting for signals...")
    
    # Auto refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
