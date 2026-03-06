# Binance Ingestor (Crypto Feed High-Load)

A high-performance, asynchronous pipeline for ingesting, storing, and distributing real-time cryptocurrency market data from Binance.

This project connects to the Binance WebSocket API, stores raw order book snapshots into ClickHouse, performs real-time data aggregation using ClickHouse Materialized Views, and streams both real-time data and 1-minute aggregated candles to clients via a FastAPI WebSocket server and Redis Pub/Sub.

## 🚀 Features

- **Real-time Ingestion**: Connects to the Binance WebSocket stream for real-time market data extraction (default symbols: `btcusdt`, `ethusdt`, `solusdt`, `bnbusdt`).
- **High-Performance Storage**: Uses **ClickHouse** to store raw snapshots (`market_data.snapshots`) with an automated TTL (3 days).
- **On-the-fly Aggregation**: Utilizes ClickHouse Materialized Views (`candles_1m_mv`) to aggregate raw data into 1-minute candles (`market_data.candles_1m`).
- **Live Broadcasting**: A background broadcaster service reads the latest minute's aggregated data and publishes it via Redis Pub/Sub.
- **WebSocket Steaming API**: Exposes WebSocket endpoints for clients to subscribe to either raw real-time ticker data or the 1-minute aggregated candles.
- **REST API**: Provides a REST endpoint to retrieve historical aggregated 1-minute candles.
- **Fully Asynchronous**: Built completely asynchronously with `FastAPI`, `aiochclient`, `aiohttp`, and `redis-py`.
- **Structured Logging**: Uses `structlog` for detailed, structured, JSON-friendly logs.

## 🛠️ Tech Stack

- **Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python >= 3.12)
- **Database**: [ClickHouse](https://clickhouse.com/) (Async client `aiochclient`)
- **Message Broker**: [Redis](https://redis.io/)
- **Package Management**: `uv` or `pip` (`pyproject.toml`)
- **Containerization**: Docker & Docker Compose

## 🏗️ Architecture

1. **Ingestor Worker**: A background worker that connects to Binance via WebSocket, fetches live order book snapshots, and inserts them directly into ClickHouse and Redis.
2. **ClickHouse Aggregation**: ClickHouse receives the raw data and uses an inner `SummingMergeTree` and `Materialized View` to automatically aggregate the data into 1-minute candles.
3. **Broadcaster**: A recurring async task checks ClickHouse for the past minute's completed candle and pushes a summarized event to Redis (`ticker:{symbol}_1m_candle`).
4. **API Gateway (FastAPI)**: 
   - HTTP clients can hit the REST endpoint to query ClickHouse for historical data.
   - WebSocket clients can connect to stream real-time prices or 1-minute candle events powered by Redis Pub/Sub.

## ⚙️ Prerequisites

- **Docker** and **Docker Compose**

## 🚦 Getting Started

### 1. Start the Environment using Docker Compose

The easiest way to run the entire stack (ClickHouse, Redis, API, and Ingestor) is via Docker Compose:

```bash
docker-compose up -d --build
```

This will spin up:
- ClickHouse on port `8123` & `9000`
- Redis on port `6380` (mapped to `6379` internally)
- API server on port `8000`
- The Data Ingestor worker in the background

*Note: The ClickHouse instance is automatically initialized using the script at `sql/init.sql`.*

### 2. Verify Services

Check the logs to ensure everything is running smoothly:
```bash
docker-compose logs -f
```

## 📖 API Documentation

Once the API is running, you can explore the interactive OpenAPI documentation at: `http://localhost:8000/docs`.

### REST Endpoints

#### **GET** `/api/v1/history/{symbol}`
Retrieve historical 1-minute aggregated candles for a specific symbol.
- **Parameters:**
  - `symbol` (str): e.g., `btcusdt`
  - `limit` (int, optional): The number of candles to return. Default is `60`.
- **Response**: List of `AggregatedCandle` objects containing open/close bid and ask logic, average mid-price, etc.

### WebSocket Endpoints

#### **WS** `/api/v1/ws/{symbol}/1m_candle`
Subscribe to 1-minute candle summaries. These events are published once every minute for the completed previous minute.
- **Example Client Connection**: `ws://localhost:8000/api/v1/ws/btcusdt/1m_candle`

#### **WS** `/api/v1/ws/{symbol}`
Subscribe to real-time raw ticker events via Redis Pub/Sub.
- **Example Client Connection**: `ws://localhost:8000/api/v1/ws/btcusdt`

## 📁 Project Structure

```text
.
├── app
│   ├── api         # FastAPI routers and websocket endpoints
│   ├── core        # CLI commands, settings, configuration mappings
│   ├── db          # Asynchronous database clients (ClickHouse, Redis)
│   ├── models      # Pydantic data models for snapshot and aggregation
│   ├── services    # Background tasks (e.g. candle broadcaster)
│   └── main.py     # FastAPI application lifecycle and startup
├── sql
│   └── init.sql    # ClickHouse schema initialization triggers / Materialized Views
├── docker-compose.yml 
├── pyproject.toml  # Project dependencies and configuration
└── README.md
```
