import os
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://market_user:changeme@localhost:5432/market",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="Real-Time Financial Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fetch_all(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(text(query), params or {}).mappings().all()
        return [dict(row) for row in rows]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ticks/latest")
def latest_ticks(limit: int = 50) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT symbol, price, volume, event_time
        FROM stock_ticks
        ORDER BY event_time DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


@app.get("/metrics/latest")
def latest_metrics() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT DISTINCT ON (symbol)
            symbol,
            window_start,
            window_end,
            avg_price,
            min_price,
            max_price,
            total_volume,
            tick_count
        FROM stock_metrics
        ORDER BY symbol, window_end DESC
        """
    )


@app.get("/metrics/{symbol}")
def metrics_by_symbol(symbol: str, limit: int = 30) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT symbol, window_start, window_end, avg_price, min_price, max_price, total_volume, tick_count
        FROM stock_metrics
        WHERE symbol = :symbol
        ORDER BY window_end DESC
        LIMIT :limit
        """,
        {"symbol": symbol.upper(), "limit": limit},
    )


@app.get("/anomalies/latest")
def latest_anomalies(limit: int = 25) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT symbol, price, avg_price, pct_deviation, event_time
        FROM stock_anomalies
        ORDER BY event_time DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
