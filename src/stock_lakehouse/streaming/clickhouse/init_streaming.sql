-- =============================================================
-- Stock Lakehouse Streaming — ClickHouse Schema
-- Idempotent: all tables use CREATE TABLE IF NOT EXISTS
-- Run via streaming/clickhouse/init.py or manually in ClickHouse
-- All tables live in database 'lakehouse'
-- =============================================================

SET allow_experimental_lightweight_delete = 1;

-- ---------------------------------------------------------------
-- 1. rt_hose_ohlcv_1m — OHLCV 1-minute candles from Kafka
--    Source: MV from kafka_ohlc
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.rt_hose_ohlcv_1m
(
    received_at   DateTime64(3, 'Asia/Ho_Chi_Minh'),
    candle_time   DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol        LowCardinality(String),
    resolution    String,
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Int64,
    lastUpdated   Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(candle_time)
ORDER BY (symbol, candle_time)
TTL toDate(candle_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------
-- 2. kafka_ohlc — Kafka Engine table, consumes dnse.ohlc topic
--    ClickHouse polls Kafka automatically; no Python consumer needed
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.kafka_ohlc
(
    received_at  String,
    symbol       String,
    resolution   String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Int64,
    type         String,
    time         UInt32,
    lastUpdated  UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'kafka:9092',
    kafka_topic_list          = 'dnse.ohlc',
    kafka_group_name          = 'clickhouse_lakehouse_streaming',
    kafka_format              = 'JSONEachRow',
    kafka_num_consumers       = 1,
    kafka_max_block_size      = 65536,
    kafka_skip_broken_messages = 10;

-- ---------------------------------------------------------------
-- 3. Materialized View: kafka_ohlc → rt_hose_ohlcv_1m
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.mv_rt_ohlcv_1m
TO lakehouse.rt_hose_ohlcv_1m
AS
SELECT
    parseDateTime64BestEffort(received_at, 3, 'Asia/Ho_Chi_Minh') AS received_at,
    toDateTime64(toDateTime(time), 3, 'Asia/Ho_Chi_Minh')        AS candle_time,
    symbol,
    resolution,
    open,
    high,
    low,
    close,
    volume,
    lastUpdated
FROM lakehouse.kafka_ohlc;

-- ---------------------------------------------------------------
-- 4. rt_hose_latest_price — Latest price per symbol (upsert)
--    Source: MV from rt_hose_ohlcv_1m using argMax
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.rt_hose_latest_price
(
    symbol           LowCardinality(String),
    latest_price     Float64,
    latest_quantity  Int64,
    last_trade_time  DateTime64(3, 'Asia/Ho_Chi_Minh')
)
ENGINE = ReplacingMergeTree(last_trade_time)
ORDER BY (symbol);

-- ---------------------------------------------------------------
-- 5. Materialized View: rt_hose_ohlcv_1m → rt_hose_latest_price
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.mv_rt_latest_price
TO lakehouse.rt_hose_latest_price
AS
SELECT
    symbol,
    close            AS latest_price,
    volume           AS latest_quantity,
    candle_time      AS last_trade_time
FROM lakehouse.rt_hose_ohlcv_1m;

-- ---------------------------------------------------------------
-- 6. rt_hose_intraday_vwap — Session VWAP accumulator
--    Formula: VWAP = Σ(tp × vol) / Σ(vol),  tp = (H+L+C)/3
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.rt_hose_intraday_vwap
(
    symbol       LowCardinality(String),
    trading_date Date,
    sum_pv       AggregateFunction(sum, Float64),   -- Σ(tp × vol)
    sum_vol      AggregateFunction(sum, Int64)       -- Σ(vol)
)
ENGINE = AggregatingMergeTree()
ORDER BY (symbol, trading_date)
TTL trading_date + INTERVAL 30 DAY;

-- ---------------------------------------------------------------
-- 7. Materialized View: rt_hose_ohlcv_1m → rt_hose_intraday_vwap
--    typical_price = (high + low + close) / 3
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.mv_rt_intraday_vwap
TO lakehouse.rt_hose_intraday_vwap
AS
SELECT
    symbol,
    toDate(candle_time)                        AS trading_date,
    sumState((high + low + close) / 3 * volume) AS sum_pv,
    sumState(volume)                            AS sum_vol
FROM lakehouse.rt_hose_ohlcv_1m
GROUP BY symbol, trading_date;

-- ---------------------------------------------------------------
-- 8. realtime_hose_stock_signal — Batch indicators + streaming price
--    Updated on every incoming candle via scheduled query
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.realtime_hose_stock_signal
(
    symbol         LowCardinality(String),
    latest_price   Float64,
    vwap           Float64,
    sma20          Float64,
    ema20          Float64,
    rsi14          Float64,
    signal_type    String,   -- BULLISH / BEARISH / NEUTRAL
    created_at     DateTime64(3, 'Asia/Ho_Chi_Minh')
)
ENGINE = MergeTree()
ORDER BY (symbol, created_at)
TTL toDate(created_at) + INTERVAL 30 DAY;

-- ---------------------------------------------------------------
-- 9. hose_alert_events — Alert history with severity
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.hose_alert_events
(
    alert_time       DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol           LowCardinality(String),
    rule_name        LowCardinality(String),   -- SINGLE / COMBINED
    alert_type       String,                   -- PRICE_ABOVE_SMA20, COMBINED_PUMP_RISK, ...
    severity         LowCardinality(String),   -- INFO / WARNING / CRITICAL
    price            Float64,
    indicator_value  Float64,
    threshold        Float64,
    deviation_pct   Float64,
    message          String
)
ENGINE = MergeTree()
ORDER BY (alert_time, symbol, rule_name)
TTL toDate(alert_time) + INTERVAL 180 DAY;
