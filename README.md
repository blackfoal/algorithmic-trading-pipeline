# Algorithmic Trading Data Pipeline

A real-time cryptocurrency data streaming system that continuously fetches Binance OHLCV data, processes it through Kafka, and stores it in PostgreSQL for algorithmic trading analysis.

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐                           ┌─────────────────┐
│   Binance API   │───▶│  Historical     │─────────────────────────▶ │   PostgreSQL    │
│                 │    │  Producer       │                           │   Database      │
└─────────────────┘    └─────────────────┘                           └─────────────────┘
       │                                                                      ▲
       │                                                                      │
       │                                                                      │
       │               ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
       ───────────────▶│  Real-time      │───▶│     Kafka       │───▶│  DB Consumer    │
                       │  Producer       │    │                 │    │                 │
                       └─────────────────┘    └─────────────────┘    └─────────────────┘
 ```                                                       

## 🚀 Core Components

### 1. Historical Producer
- **Purpose**: Backfills historical OHLCV data for specified symbols
- **Data Source**: Binance API (1-minute candles)
- **Coverage**: Configurable number of days (default: 7 days)
- **Boundary Logic**: Backfills up to but **NOT INCLUDING** the current minute
- **Output**: Direct database insertion (bypasses Kafka for performance)

### 2. Real-time Producer
- **Purpose**: Continuously fetches the latest available OHLCV data
- **Data Source**: Binance API (1-minute candles)
- **Timing**: Dynamic, event-driven based on candle completion
- **Output**: Kafka messages for real-time consumption

### 3. Kafka Message Broker
- **Topic**: `binance-ohlcv`
- **Partitioning**: By symbol (ensures ordered processing per symbol)
- **Message Format**: JSON with OHLCV data + metadata
- **Retention**: Configurable based on requirements

### 4. DB Consumer
- **Purpose**: Consumes Kafka messages and inserts into PostgreSQL
- **Processing**: Real-time message consumption
- **Database**: PostgreSQL with optimized schema and indexes
- **Error Handling**: Robust retry logic and connection management

## 📊 Data Flow

### Historical Backfill Process
1. **Startup**: Historical producer begins at system startup
2. **Boundary Calculation**: 
   - End time: Current minute boundary (exclusive)
   - Start time: End time - BACKFILL_DAYS
3. **Data Fetching**: Sequential fetching with rate limiting
4. **Database Insertion**: Direct insertion for performance (bypasses Kafka)
5. **Completion**: Exits after backfill is complete

### Real-time Streaming Process
1. **Parallel Startup**: Runs independently and in parallel with historical producer
2. **Immediate Start**: Begins processing as soon as Kafka is ready
3. **Smart Timing**: Calculates last completed minute dynamically
4. **Data Fetching**: Fetches specific completed candles by timestamp
5. **Dynamic Waiting**: Calculates optimal wait time until next candle completion
6. **Continuous Cycle**: Always fetches the most recent available data

### Independence & Coordination
- **Historical Producer**: Runs once at startup, completes backfill, then exits
- **Real-time Producer**: Runs continuously, independent of historical producer
- **No Dependencies**: Both producers can start/stop independently
- **Data Coordination**: Historical ensures no gaps, real-time maintains continuity

## 🎯 Design Choices & Gap Prevention

### Why We Don't Use `since=None, limit=1`
- **Problem**: `since=None` is ambiguous - Binance might return current in-progress candle
- **Solution**: Use precise timestamps to fetch specific completed candles
- **Benefit**: Guaranteed to get the exact minute we want

### Dynamic Timing vs Fixed Frequency
- **Fixed Frequency (❌)**: 
  - Polls every X seconds regardless of data availability
  - May miss data or fetch incomplete candles
  - Inefficient resource usage

- **Dynamic Timing (✅)**:
  - Calculates time until next candle completion
  - Adds buffer time to ensure data availability
  - Always fetches at optimal moments

### Boundary Coordination
- **Historical Producer**: Stops at current minute (exclusive)
- **Real-time Producer**: Starts with last completed minute
- **Result**: Seamless coverage with no gaps

### Example Timeline
```
Time: 14:16:24 (Service Start)
├── Historical Producer: Backfills up to 14:16:00 (exclusive)
├── Real-time Producer: Immediately fetches 14:15:00 (last completed)
└── Next Fetch: Calculates wait until 14:17:05 (14:17:00 + 5 sec buffer)

Time: 14:17:05
├── Real-time Producer: Fetches 14:16:00 (now available)
└── Next Fetch: Calculates wait until 14:18:05

Time: 14:18:05
├── Real-time Producer: Fetches 14:17:00 (now available)
└── Continuous cycle...
```

## 🔧 Configuration

### Environment Variables
```bash
# Binance API Keys
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key

# PostgreSQL Configuration
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
POSTGRES_HOST=host.docker.internal

# Kafka Configuration
KAFKA_BROKER=kafka:9092
TOPIC_NAME=binance-ohlcv
KAFKA_UI_PORT=8090

# Application Configuration
SYMBOLS=BTC/USDT,ETH/USDT,ADA/USDT,SOL/USDT,DOT/USDT,BNB/USDT,XRP/USDT
BACKFILL_DAYS=7
REALTIME_DELAY_SECONDS=1
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Binance API credentials
- PostgreSQL database (external or containerized)

### 1. Clone Repository
```bash
git clone <repository-url>
cd algoritmic_trading
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Start Services
```bash
docker-compose up -d
```

### 4. Monitor Services
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f realtime-producer
docker-compose logs -f db-consumer
```

### 5. Access Kafka UI
- Open http://localhost:8090
- Monitor topics and message flow

## 📈 Monitoring & Debugging

### Key Metrics to Watch
- **Historical Producer**: Backfill completion time, records inserted
- **Real-time Producer**: Fetch timing, data freshness, Kafka offsets
- **DB Consumer**: Message processing rate, database insertion success
- **Kafka**: Topic lag, partition distribution

### Common Issues & Solutions
1. **Data Gaps**: Check boundary coordination between producers
2. **Kafka Connection Issues**: Verify network connectivity and broker status
3. **Database Errors**: Check credentials and connection parameters
4. **Rate Limiting**: Monitor Binance API usage and adjust delays

