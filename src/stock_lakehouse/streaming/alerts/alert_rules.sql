-- =============================================================
-- Stock Lakehouse Streaming — Signal + Alert SQL
-- Run as ClickHouse scheduled queries (every 30 seconds)
-- =============================================================

-- ---------------------------------------------------------------
-- STEP 1: Update realtime_hose_stock_signal
-- JOIN batch indicators (fact_hose_daily_market) with streaming
-- price (rt_hose_latest_price) and intraday VWAP
-- ---------------------------------------------------------------
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
        SELECT max(trading_date) FROM fact_hose_daily_market
    )
) batch ON rt.symbol = batch.symbol
LEFT JOIN (
    SELECT symbol,
           sumMerge(sum_pv) / sumMerge(sum_vol) AS vwap
    FROM rt_hose_intraday_vwap
    WHERE trading_date = today()
    GROUP BY symbol
) vwap ON rt.symbol = vwap.symbol;

-- ---------------------------------------------------------------
-- STEP 2: Alert detection
-- Single alerts + Combined alerts (>= 2 signals consensus)
--
-- Single rules:
--   PRICE_ABOVE_SMA20 | PRICE_BELOW_SMA20
--   PRICE_ABOVE_VWAP  | PRICE_BELOW_VWAP
--   VOLUME_SPIKE — intraday rolling avg (20 x 1m candles)
--
-- Combined (needs >= 2 signals):
--   COMBINED_PUMP_RISK       CRITICAL   price>VWAP + RSI>70 + vol spike
--   COMBINED_PANIC_SELL      CRITICAL   price<VWAP + RSI<30 + vol spike
--   COMBINED_OVERBOUGHT_...  WARNING    price>VWAP + RSI>70
--   COMBINED_OVERSOLD_...    WARNING    price<VWAP + RSI<30
--   COMBINED_UNUSUAL_VOLUME  WARNING    vol spike in band
-- ---------------------------------------------------------------
INSERT INTO hose_alert_events
WITH
    latest AS (
        SELECT symbol, latest_price, vwap
        FROM realtime_hose_stock_signal
        ORDER BY created_at DESC
        LIMIT 1 BY symbol
    ),
    batch AS (
        SELECT symbol, sma20, ema20, rsi14
        FROM fact_hose_daily_market
        WHERE trading_date = (SELECT max(trading_date) FROM fact_hose_daily_market)
    ),
    vol_stats AS (
        SELECT
            symbol,
            latest_price,
            avg_vol,
            latest_vol,
            latest_vol / avg_vol AS vol_ratio
        FROM (
            SELECT
                symbol,
                latest_price,
                avg(volume) OVER (
                    PARTITION BY symbol ORDER BY candle_time
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS avg_vol,
                volume AS latest_vol
            FROM rt_hose_ohlcv_1m
            WHERE toDate(candle_time) = today()
              AND received_at > now64(3) - INTERVAL 5 MINUTE
        )
    ),
    alert_candidates AS (
        SELECT
            l.symbol,
            l.latest_price       AS price,
            l.vwap               AS vwap,
            b.sma20,
            b.ema20,
            b.rsi14,
            v.vol_ratio         AS vol_ratio,
            v.avg_vol           AS avg_vol,
            -- Single signals
            if(l.latest_price > b.sma20, 1, 0)           AS sig_above_sma,
            if(l.latest_price < b.sma20, 1, 0)           AS sig_below_sma,
            if(l.latest_price > l.vwap, 1, 0)            AS sig_above_vwap,
            if(l.latest_price < l.vwap, 1, 0)             AS sig_below_vwap,
            if(v.vol_ratio >= 2.0, 1, 0)                 AS sig_vol_spike,
            -- Combined signals
            if(l.latest_price > l.vwap AND b.rsi14 > 70, 1, 0)    AS sig_breakout_overbought,
            if(l.latest_price < l.vwap AND b.rsi14 < 30, 1, 0)    AS sig_breakdown_oversold,
            if(v.vol_ratio >= 2.0, 1, 0)                         AS sig_vol_spike2,
            now64(3) AS alert_time
        FROM latest l
        LEFT JOIN batch b ON l.symbol = b.symbol
        LEFT JOIN vol_stats v ON l.symbol = v.symbol
    )
SELECT
    alert_time,
    symbol,
    'COMBINED' AS rule_name,
    alert_type,
    severity,
    price,
    indicator_value,
    threshold,
    deviation_pct,
    message
FROM (
    -- COMBINED_PUMP_RISK
    SELECT alert_time, symbol, price,
           'COMBINED_PUMP_RISK'     AS alert_type,
           'CRITICAL'               AS severity,
           rsi14                    AS indicator_value,
           70.0                     AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' BREAKOUT VWAP + RSI=', toString(rsi14), ' (overbought) + KL spike ', toString(round(vol_ratio, 1)), 'x — BREAKOUT的风险')
           AS message
    FROM alert_candidates
    WHERE sig_above_vwap + sig_below_vwap = 0  -- above vwap
      AND sig_overbought = 1
      AND sig_vol_spike2 = 1

    UNION ALL

    -- COMBINED_PANIC_SELL
    SELECT alert_time, symbol, price,
           'COMBINED_PANIC_SELL'   AS alert_type,
           'CRITICAL'              AS severity,
           rsi14                   AS indicator_value,
           30.0                    AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' BREAKDOWN VWAP + RSI=', toString(rsi14), ' (oversold) + KL spike ', toString(round(vol_ratio, 1)), 'x — 恐慌性抛售')
           AS message
    FROM alert_candidates
    WHERE sig_below_vwap = 1
      AND sig_oversold = 1
      AND sig_vol_spike2 = 1

    UNION ALL

    -- COMBINED_OVERBOUGHT_BREAKOUT
    SELECT alert_time, symbol, price,
           'COMBINED_OVERBOUGHT_BREAKOUT' AS alert_type,
           'WARNING'               AS severity,
           rsi14                    AS indicator_value,
           70.0                     AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' Breakout VWAP + RSI=', toString(rsi14), ' (overbought) — 请谨慎')
           AS message
    FROM alert_candidates
    WHERE sig_above_vwap + sig_below_vwap = 0
      AND sig_overbought = 1

    UNION ALL

    -- COMBINED_OVERSOLD_BREAKDOWN
    SELECT alert_time, symbol, price,
           'COMBINED_OVERSOLD_BREAKDOWN' AS alert_type,
           'WARNING'              AS severity,
           rsi14                  AS indicator_value,
           30.0                   AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' Breakdown VWAP + RSI=', toString(rsi14), ' (oversold) — 可能是机会')
           AS message
    FROM alert_candidates
    WHERE sig_below_vwap = 1
      AND sig_oversold = 1

    UNION ALL

    -- COMBINED_UNUSUAL_VOLUME
    SELECT alert_time, symbol, price,
           'COMBINED_UNUSUAL_VOLUME' AS alert_type,
           'WARNING'           AS severity,
           vol_ratio           AS indicator_value,
           2.0                 AS threshold,
           (vol_ratio - 1.0) * 100 AS deviation_pct,
           concat(symbol, ' KL 异常波动 ', toString(round(vol_ratio, 1)), 'x — RSI=', toString(rsi14), ', price in VWAP band')
           AS message
    FROM alert_candidates
    WHERE sig_vol_spike2 = 1
      AND sig_above_vwap + sig_below_vwap = 0
      AND (sig_overbought + sig_oversold) = 0

    UNION ALL

    -- PRICE_ABOVE_SMA20 (single)
    SELECT alert_time, symbol, price,
           'PRICE_ABOVE_SMA20' AS alert_type,
           'INFO'              AS severity,
           sma20               AS indicator_value,
           sma20               AS threshold,
           (price - sma20) / sma20 * 100 AS deviation_pct,
           concat(symbol, ' price above SMA20 — ', toString(round((price - sma20) / sma20 * 100, 2)), '%')
           AS message
    FROM alert_candidates
    WHERE sig_above_sma = 1 AND sma20 IS NOT NULL

    UNION ALL

    -- PRICE_BELOW_SMA20 (single)
    SELECT alert_time, symbol, price,
           'PRICE_BELOW_SMA20' AS alert_type,
           'INFO'              AS severity,
           sma20               AS indicator_value,
           sma20               AS threshold,
           (price - sma20) / sma20 * 100 AS deviation_pct,
           concat(symbol, ' price below SMA20 — ', toString(round((price - sma20) / sma20 * 100, 2)), '%')
           AS message
    FROM alert_candidates
    WHERE sig_below_sma = 1 AND sma20 IS NOT NULL

    UNION ALL

    -- PRICE_ABOVE_VWAP (single)
    SELECT alert_time, symbol, price,
           'PRICE_ABOVE_VWAP' AS alert_type,
           'INFO'             AS severity,
           vwap               AS indicator_value,
           vwap               AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' price above intraday VWAP — ', toString(round((price - vwap) / vwap * 100, 2)), '%')
           AS message
    FROM alert_candidates
    WHERE sig_above_vwap = 1 AND vwap IS NOT NULL AND vwap > 0

    UNION ALL

    -- PRICE_BELOW_VWAP (single)
    SELECT alert_time, symbol, price,
           'PRICE_BELOW_VWAP' AS alert_type,
           'INFO'             AS severity,
           vwap               AS indicator_value,
           vwap               AS threshold,
           (price - vwap) / vwap * 100 AS deviation_pct,
           concat(symbol, ' price below intraday VWAP — ', toString(round((price - vwap) / vwap * 100, 2)), '%')
           AS message
    FROM alert_candidates
    WHERE sig_below_vwap = 1 AND vwap IS NOT NULL AND vwap > 0

    UNION ALL

    -- VOLUME_SPIKE (single, using intraday rolling avg)
    SELECT alert_time, symbol, price,
           'VOLUME_SPIKE'      AS alert_type,
           'WARNING'           AS severity,
           vol_ratio           AS indicator_value,
           2.0                 AS threshold,
           (vol_ratio - 1.0) * 100 AS deviation_pct,
           concat(symbol, ' Volume spike ', toString(round(vol_ratio, 1)), 'x vs intraday avg (20 x 1m candles)')
           AS message
    FROM alert_candidates
    WHERE sig_vol_spike2 = 1
);
