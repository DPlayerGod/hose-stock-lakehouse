-- =============================================================
-- Stock Lakehouse Streaming — ClickHouse Schema
-- Idempotent: all tables use CREATE TABLE IF NOT EXISTS
-- Run via streaming/clickhouse/init.py or manually in ClickHouse
-- All tables live in database 'lakehouse'
--
-- Single-source-of-truth note (2026-07-04 cleanup):
--   * VWAP + σ are computed ONLY in Python (alerts/vwap.py) and
--     written by detector.py into rt_hose_indicators.
--   * rt_hose_intraday_vwap + realtime_hose_stock_signal +
--     rt_hose_latest_price have been removed (no MV populator,
--     no Python consumer — redundant with rt_hose_indicators).
-- =============================================================

SET allow_experimental_lightweight_delete = 1;

-- ---------------------------------------------------------------
-- 1. rt_hose_ohlcv_1m — OHLCV 1-minute candles from Kafka
--    Source: MV from kafka_ohlc (Section 2-3)
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
    kafka_broker_list         = '${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}',
    kafka_topic_list          = '${KAFKA_TOPIC_OHLC:-dnse.ohlc}',
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
-- 4. rt_hose_indicators — Single source of truth for streaming
--    indicators. Populated by detector.py (_insert_indicator)
--    ONCE per candle: VWAP, σ (Wilder's session VWAP) + RSI14 +
--    volume_ratio. Dashboard + Alert Rules read from here.
--    DDL is owned by this schema file (not the application).
--    detector.py verifies the table exists at startup and fails
--    fast if init.py has not been run yet.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.rt_hose_indicators
(
    candle_time   DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol        LowCardinality(String),
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Int64,
    vwap          Nullable(Float64),
    sigma         Nullable(Float64),
    rsi14         Nullable(Float64),
    volume_ratio  Nullable(Float64),
    created_at    DateTime64(3, 'Asia/Ho_Chi_Minh')
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(candle_time)
ORDER BY (symbol, candle_time)
TTL toDate(candle_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------
-- 5. rt_hose_alerts — Alert history from Python Alert Detector
--    ORDER BY (alert_time, symbol, rule_name) enables efficient dedup
--    TTL: 90 days
--    DDL is owned by this schema file (not the application).
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.rt_hose_alerts
(
    alert_time      DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol          LowCardinality(String),
    rule_name       LowCardinality(String),
    alert_type      String,
    severity        LowCardinality(String),
    price           Float64,
    indicator_value Float64,
    threshold       Float64,
    deviation_pct   Float64,
    message         String
) ENGINE = MergeTree()
ORDER BY (alert_time, symbol, rule_name)
TTL toDate(alert_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
