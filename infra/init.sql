CREATE TABLE IF NOT EXISTS stock_ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    volume INTEGER NOT NULL,
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_metrics (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_price NUMERIC(12, 4) NOT NULL,
    min_price NUMERIC(12, 4) NOT NULL,
    max_price NUMERIC(12, 4) NOT NULL,
    total_volume BIGINT NOT NULL,
    tick_count BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_anomalies (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    avg_price NUMERIC(12, 4) NOT NULL,
    pct_deviation NUMERIC(8, 4) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
