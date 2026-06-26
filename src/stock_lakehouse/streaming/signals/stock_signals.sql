-- =============================================================
-- Stock Lakehouse Streaming — Signal Computation
-- Updates realtime_hose_stock_signal table
-- Run as ClickHouse scheduled query (every 30 seconds)
-- =============================================================

INSERT INTO realtime_hose_stock_signal
SELECT
    rt.symbol,
    rt.latest_price,
    vwap.vwap,
    batch.sma20,
    batch.ema20,
    batch.rsi14,
    CASE
        WHEN rt.latest_price > batch.sma20
         AND rt.latest_price > vwap.vwap THEN 'BULLISH'
        WHEN rt.latest_price < batch.sma20
         AND rt.latest_price < vwap.vwap THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS signal_type,
    now64(3) AS created_at
FROM rt_hose_latest_price rt
LEFT JOIN (
    SELECT symbol, sma20, ema20, rsi14
    FROM fact_hose_daily_market
    WHERE trading_date = (
        SELECT max(trading_date)
        FROM fact_hose_daily_market
    )
) batch ON rt.symbol = batch.symbol
LEFT JOIN (
    SELECT symbol,
           sumMerge(sum_pv) / sumMerge(sum_vol) AS vwap
    FROM rt_hose_intraday_vwap
    WHERE trading_date = today()
    GROUP BY symbol
) vwap ON rt.symbol = vwap.symbol;
