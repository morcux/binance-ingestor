CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.snapshots (
    timestamp DateTime64(3),
    symbol String,
    bid_price Float64,
    ask_price Float64,
    bid_qty Float64,
    ask_qty Float64
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 3 DAY;

CREATE TABLE IF NOT EXISTS market_data.candles_1m (
    minute DateTime,
    symbol String,
    avg_mid_price Float64,
    open_bid_qty Float64,
    close_bid_qty Float64,
    open_ask_qty Float64,
    close_ask_qty Float64
) ENGINE = SummingMergeTree()
ORDER BY (symbol, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.candles_1m_mv TO market_data.candles_1m AS
SELECT
    toStartOfMinute(timestamp) as minute,
    symbol,
    avg((bid_price + ask_price) / 2) as avg_mid_price,
    argMin(bid_qty, timestamp) as open_bid_qty,
    argMax(bid_qty, timestamp) as close_bid_qty,
    argMin(ask_qty, timestamp) as open_ask_qty,
    argMax(ask_qty, timestamp) as close_ask_qty
FROM market_data.snapshots
GROUP BY minute, symbol;