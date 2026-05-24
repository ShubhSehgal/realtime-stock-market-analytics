import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")

MARKET_DATA_MODE = os.getenv("MARKET_DATA_MODE", "simulated").lower()
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
ALPHA_VANTAGE_POLL_SECONDS = int(os.getenv("ALPHA_VANTAGE_POLL_SECONDS", "15"))

SYMBOLS = [
    symbol.strip().upper()
    for symbol in os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,GOOGL,TSLA").split(",")
    if symbol.strip()
]

SIMULATED_STARTING_PRICES = {
    "AAPL": 190.00,
    "MSFT": 420.00,
    "NVDA": 950.00,
    "AMZN": 180.00,
    "GOOGL": 170.00,
    "TSLA": 180.00,
}

current_prices = {
    symbol: SIMULATED_STARTING_PRICES.get(symbol, random.uniform(50, 500))
    for symbol in SYMBOLS
}


def create_simulated_tick(symbol: str) -> dict:
    previous_price = current_prices[symbol]
    shock = random.choice([0, 0, 0, 0, random.uniform(-0.05, 0.05)])
    drift = random.uniform(-0.003, 0.003)
    new_price = max(1, previous_price * (1 + drift + shock))
    current_prices[symbol] = new_price

    return {
        "symbol": symbol,
        "price": round(new_price, 4),
        "volume": random.randint(100, 10_000),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "simulated",
    }


def fetch_alpha_vantage_tick(symbol: str) -> Optional[dict]:
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "outputsize": "compact",
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()

    if "Note" in payload:
        print(f"Alpha Vantage rate-limit note for {symbol}: {payload['Note']}")
        return None

    if "Error Message" in payload:
        print(f"Alpha Vantage error for {symbol}: {payload['Error Message']}")
        return None

    time_series = payload.get("Time Series (1min)")
    if not time_series:
        print(f"No intraday data returned for {symbol}. Response keys: {list(payload.keys())}")
        return None

    latest_timestamp = sorted(time_series.keys())[-1]
    latest_bar = time_series[latest_timestamp]

    return {
        "symbol": symbol,
        "price": round(float(latest_bar["4. close"]), 4),
        "volume": int(float(latest_bar["5. volume"])),
        "event_time": datetime.fromisoformat(latest_timestamp).replace(tzinfo=timezone.utc).isoformat(),
        "source": "alpha_vantage",
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def publish_tick(producer: KafkaProducer, tick: dict) -> None:
    producer.send(TOPIC, key=tick["symbol"], value=tick)
    producer.flush()
    print(f"Published tick: {tick}")


def run_simulated_mode(producer: KafkaProducer) -> None:
    print("Running producer in simulated mode.")
    while True:
        symbol = random.choice(SYMBOLS)
        publish_tick(producer, create_simulated_tick(symbol))
        time.sleep(0.5)


def run_alpha_vantage_mode(producer: KafkaProducer) -> None:
    print("Running producer in Alpha Vantage mode.")
    print("Free API keys have strict rate limits. Increase ALPHA_VANTAGE_POLL_SECONDS if needed.")

    while True:
        for symbol in SYMBOLS:
            try:
                tick = fetch_alpha_vantage_tick(symbol)
                if tick:
                    publish_tick(producer, tick)
            except Exception as exc:
                print(f"Failed to fetch/publish {symbol}: {exc}")

            time.sleep(ALPHA_VANTAGE_POLL_SECONDS)


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    print(f"Publishing to Kafka topic: {TOPIC}")
    print(f"Configured symbols: {', '.join(SYMBOLS)}")

    producer = build_producer()

    if MARKET_DATA_MODE == "alpha_vantage":
        run_alpha_vantage_mode(producer)
    else:
        run_simulated_mode(producer)


if __name__ == "__main__":
    main()
