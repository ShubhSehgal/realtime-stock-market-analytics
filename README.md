# realtime-stock-market-analytics

# What it does

- Streams Alpha Vantage intraday market data into Kafka, with a simulated data fallback
- Processes market events with Spark Structured Streaming
- Computes rolling price metrics and anomaly signals
- Stores results in PostgreSQL
- Exposes analytics through a FastAPI backend
- Displays live market metrics in a React dashboard

## Tech Stack

Kafka, Spark Structured Streaming, Python, PostgreSQL, FastAPI, React, Docker, Alpha Vantage

## Architecture

```
Alpha Vantage / Simulated Stock Producer
        |
        v
Apache Kafka topic: stock_ticks
        |
        v
Spark Structured Streaming
        |
        v
PostgreSQL tables
        |
        v
FastAPI REST API
        |
        v
React Dashboard
```

## Instructions to Run Locally

### 1. Configure Environment

```bash
cp .env.example .env
```

```text
ALPHA_VANTAGE_API_KEY=your_key_here
MARKET_DATA_MODE=alpha_vantage
```

To run without an API key, use:

```text
MARKET_DATA_MODE=simulated
```

### 2. Start Infrastructure

```bash
docker compose up --build
```

### 3. Start the Stock Tick Producer

```bash
docker compose exec producer python producer.py
```

### 4. Start the Spark Streaming Job

```bash
docker compose exec spark bash -lc "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /app/streaming_job.py"
```

### 5. Open the Dashboard

Frontend:

```text
http://localhost:3000
```

Backend API docs:

```text
http://localhost:8000/docs
```

## Alpha Vantage Mode

The producer supports two modes:

```text
MARKET_DATA_MODE=alpha_vantage
MARKET_DATA_MODE=simulated
```

In Alpha Vantage mode, the producer calls the `TIME_SERIES_INTRADAY` endpoint with `interval=1min`, extracts the latest candle close price and volume, then publishes it to Kafka as a market event.

Note: Alpha Vantage returns 1-minute OHLCV candles, not true exchange tick-by-tick data.