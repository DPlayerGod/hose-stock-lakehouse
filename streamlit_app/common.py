"""Shared helpers for the HOSE Analytics Streamlit dashboard."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import cast

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from stock_lakehouse.clickhouse.client import get_clickhouse_client
from stock_lakehouse.config import ClickHouseConfig

logging.basicConfig(level=logging.WARNING)

ICT = timezone(timedelta(hours=7))
# Batch pipeline chỉ xử lý 5 mã (giới hạn API free-tier vnstock).
# Streamlit UI cũng giới hạn theo batch để đồng bộ.
SYMBOLS = ["FPT", "VCB", "HPG", "VNM", "MWG"]
REFRESH_SEC = int(os.getenv("DASHBOARD_REFRESH_SEC", "5"))

UP_COLOR = "#26a69a"
DOWN_COLOR = "#ef5350"
EMA_COLOR = "#f59e0b"
EVENT_COLOR = "#2563eb"
VNINDEX_COLOR = "#2563eb"
VN30_COLOR = "#16a34a"
REALTIME_BULL_COLOR = "#16a34a"
REALTIME_BEAR_COLOR = "#dc2626"
REALTIME_NEUTRAL_COLOR = "#6b7280"
RSI_PERIOD = 14
VOLUME_SPIKE_RATIO = 3.0
BAND_SIGMA_MULTIPLIER = 2.0
PLOT_FONT = '"Be Vietnam Pro", Inter, Segoe UI, Arial, sans-serif'

SEVERITY_COLORS = {
    "CRITICAL": ("#ff4444", "#fee2e2"),
    "WARNING": ("#ffa800", "#fef3c7"),
    "INFO": ("#4fc3f7", "#e0f7fa"),
}
ALERT_TYPE_LABELS = {
    "VWAP_BREAKOUT_UP": ("VWAP vượt lên", "#00d4aa"),
    "VWAP_BREAKDOWN": ("VWAP phá xuống", "#ff6b6b"),
    "RSI_OVERBOUGHT": ("RSI quá mua", "#ff6b6b"),
    "RSI_OVERSOLD": ("RSI quá bán", "#00d4aa"),
    "VOLUME_SPIKE": ("Volume spike", "#ffa800"),
}

STYLE_BLOCK = """
<style>
  html, body, [class*="css"] {
      font-family: "Segoe UI", Inter, Arial, sans-serif;
      text-rendering: optimizeLegibility;
      -webkit-font-smoothing: antialiased;
  }
  [data-testid="stHorizontalBlock"] { flex-wrap: nowrap !important; }
  [data-testid="stSegmentedControl"] div { flex-wrap: nowrap !important; min-width: 0 !important; }
  [data-testid="stSegmentedControl"] button { white-space: nowrap !important; flex-shrink: 0 !important; }
  button, input, select, textarea, label, span, p, div {
      font-family: "Segoe UI", Inter, Arial, sans-serif;
  }
  .stApp { background: #f8f9fc; color: #1e293b; }
  .block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1180px; }
  div[data-testid="stMetric"] {
      background: #ffffff; border: 1px solid #e2e8f0; padding: 15px 20px;
      border-radius: 12px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
  }
  div[data-testid="stMetricLabel"] > div > div > p { font-size: .95rem; color: #64748b; font-weight: 600; }
  div[data-testid="stMetricValue"] > div { font-size: 1.8rem; font-weight: 800; color: #0f172a; }
  h1 { font-weight: 800 !important; }
  [data-testid="stSidebar"] { background-color: #ffffff !important; border-right: 1px solid #e2e8f0; }
  [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; border: 1px solid #e2e8f0; }
  thead tr th { background-color: #f1f5f9 !important; color: #334155 !important; font-weight: 600 !important; }
  .ha-sub { color: #6b7280; font-size: .85rem; margin-top: 2px; }
  .ha-card { border: 1px solid #e2e8f0; border-radius: 14px; padding: 18px 20px; background: #ffffff; box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04); }
  .ha-card h4 { margin: 0 0 14px 0; font-size: 1rem; font-weight: 700; color: #0f172a; display: flex; align-items: center; gap: 8px; padding-bottom: 10px; border-bottom: 1px solid #f1f5f9; }
  .ha-evt { display: flex; gap: 12px; padding: 12px 10px; margin: 0 -10px; border-bottom: 1px solid #f1f5f9; align-items: flex-start; border-radius: 8px; transition: background-color .15s ease; }
  .ha-evt:hover { background-color: #f8fafc; }
  .ha-evt:last-child { border-bottom: none; }
  .ha-evt-body { font-size: .88rem; line-height: 1.4; color: #1e293b; font-weight: 500; }
  .ha-evt-date { color: #6b7280; font-size: .76rem; margin-top: 2px; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
  .ha-soon-wrap { display: inline-flex; align-items: center; margin-left: 6px; }
  .ha-soon { font-size: .65rem; font-weight: 700; color: #92400e; background: #fef3c7; padding: 1px 8px; border-radius: 999px; border: 1.5px solid #f59e0b; line-height: 1.5; text-transform: uppercase; letter-spacing: .04em; vertical-align: middle; white-space: nowrap; }
  .ha-soon::before { content: "\\1F551"; margin-right: 3px; font-size: .7rem; }
  .ha-ind { display: flex; justify-content: space-between; align-items: center; padding: 11px 12px; margin: 0 -12px; border-bottom: 1px solid #f1f5f9; border-radius: 8px; transition: background-color .15s ease; position: relative; z-index: 1; }
  .ha-ind:hover { background-color: #f8fafc; }
  .ha-ind:last-child { border-bottom: none; }
  .ha-ind-name { color: #475569; font-size: .88rem; font-weight: 500; }
  .ha-ind-val { font-weight: 700; font-size: .98rem; color: #0f172a; font-variant-numeric: tabular-nums; }
</style>
"""


def inject_styles() -> None:
    st.markdown(STYLE_BLOCK, unsafe_allow_html=True)


@st.cache_resource
def client():
    return get_clickhouse_client(ClickHouseConfig())


@st.cache_data(ttl=60)
def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return client().query_df(sql, parameters=params or {})


def vn_int(x: float | None) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.0f}".replace(",", ".")


def vn_dec(x: float | None, dp: int = 1) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.{dp}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(x: float | None) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x * 100:+.2f}%"


def fmt_vol(x: float | None) -> str:
    if x is None or pd.isna(x):
        return "-"
    if abs(x) >= 1e9:
        return f"{x / 1e9:.1f}B"
    if abs(x) >= 1e6:
        return f"{x / 1e6:.1f}M"
    if abs(x) >= 1e3:
        return f"{x / 1e3:.0f}K"
    return f"{x:.0f}"


_SIGNAL_STYLE = {
    "Mua": ("#047857", "#d1fae5"),
    "Bán": ("#7f1d1d", "#fee2e2"),
    "Trung tính": ("#1f2937", "#e5e7eb"),
}


def signal_badge(text: str) -> str:
    fg, bg = _SIGNAL_STYLE.get(text, _SIGNAL_STYLE["Trung tính"])
    return (f'<span style="display:inline-flex;align-items:center;font-size:.7rem;font-weight:600;'
            f'padding:2px 8px;border-radius:999px;white-space:nowrap;line-height:1.1;letter-spacing:.02em;'
            f'color:{fg};background:{bg};border:1.5px solid {fg}">{text}</span>')


def trend_signal(price: float | None, ref: float | None) -> str:
    if price is None or ref is None or pd.isna(price) or pd.isna(ref):
        return "Trung tính"
    return "Mua" if price > ref else "Bán"


def rsi_signal(rsi: float | None) -> str:
    if rsi is None or pd.isna(rsi):
        return "Trung tính"
    if rsi >= 70:
        return "Bán"
    if rsi <= 30:
        return "Mua"
    return "Trung tính"


def macd_signal(macd: float | None) -> str:
    if macd is None or pd.isna(macd):
        return "Trung tính"
    return "Mua" if macd > 0 else "Bán"


_EVENT_TAGS = {
    "DIV": ("Cổ tức", "#065f46", "#d1fae5"),
    "ISS": ("Phát hành", "#78350f", "#fef3c7"),
    "MTG": ("Đại hội", "#1e3a8a", "#dbeafe"),
    "RPT": ("Báo cáo", "#4c1d95", "#ede9fe"),
}


def event_tag(code: str, label: str) -> tuple[str, str, str]:
    return _EVENT_TAGS.get(code.upper(), (label or code or "Sự kiện", "#374151", "#eef0f2"))


@st.cache_data(ttl=60)
def load_meta() -> dict:
    rows = client().query(
        """
        SELECT max(trading_date) AS latest_date, count() AS rows
        FROM fact_hose_daily_market
        """
    ).result_rows
    if not rows:
        return {"latest_date": None, "rows": 0}
    latest_date, rows_count = rows[0]
    return {"latest_date": latest_date, "rows": rows_count}


@st.cache_data(ttl=60)
def load_symbols() -> pd.DataFrame:
    return query_df("SELECT symbol, exchange_code, company_name FROM dim_symbol ORDER BY symbol")


@st.cache_data(ttl=60)
def load_prices() -> pd.DataFrame:
    df = query_df(
        """
        SELECT
            s.symbol,
            f.trading_date,
            f.open_price,
            f.high_price,
            f.low_price,
            f.close_price,
            f.volume,
            f.price_change,
            f.pct_change,
            f.sma20,
            f.ema20,
            f.rsi14,
            f.macd,
            f.avg_volume_20d
        FROM fact_hose_daily_market AS f
        INNER JOIN dim_symbol AS s ON f.symbol_key = s.symbol_key
        ORDER BY s.symbol, f.trading_date
        """
    )
    if not df.empty:
        df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


@st.cache_data(ttl=60)
def load_index() -> pd.DataFrame:
    df = query_df(
        """
        SELECT index_code, trading_date, close_price, volume, price_change, pct_change, avg_volume_20d
        FROM fact_hose_index_daily
        ORDER BY index_code, trading_date
        """
    )
    if not df.empty:
        df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


@st.cache_data(ttl=60)
def load_events() -> pd.DataFrame:
    df = query_df(
        """
        SELECT symbol, event_date, event_code, event_label, title_vi, value
        FROM fact_corporate_events
        ORDER BY event_date DESC
        """
    )
    if not df.empty:
        df["event_date"] = pd.to_datetime(df["event_date"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    return df


@st.cache_data(ttl=10)
def load_latest_prices() -> pd.DataFrame:
    return query_df(
        """
        SELECT
            symbol,
            argMax(close, candle_time) AS latest_price,
            argMax(volume, candle_time) AS latest_quantity,
            max(candle_time) AS last_trade_time,
            argMin(close, candle_time) AS open_price
        FROM rt_hose_ohlcv_1m
        GROUP BY symbol
        ORDER BY symbol
        """
    )


@st.cache_data(ttl=10)
def load_realtime_candles(symbol: str = "", minutes: int = 180, trading_day: date | None = None, _cache_buster: str = "") -> pd.DataFrame:
    symbol_filter = "AND symbol = %(symbol)s" if symbol else ""
    day = trading_day or datetime.now(ICT).date()
    limit = max(1, minutes)
    df = query_df(
        f"""
        SELECT candle_time, symbol, open, high, low, close, volume, vwap, sigma, rsi14, volume_ratio
        FROM (
            SELECT candle_time, symbol, open, high, low, close, volume, vwap, sigma, rsi14, volume_ratio
            FROM rt_hose_indicators
            WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = %(trading_day)s
              {symbol_filter}
            ORDER BY candle_time DESC
            LIMIT {limit}
        )
        ORDER BY candle_time ASC
        """,
        {"symbol": symbol, "trading_day": day},
    )
    if not df.empty:
        df["candle_time"] = pd.to_datetime(df["candle_time"])
    return df


@st.cache_data(ttl=30)
def load_realtime_alerts(symbol: str | None = None, limit: int = 50) -> pd.DataFrame:
    symbol_filter = f"AND symbol = '{symbol}'" if symbol else ""
    return query_df(
        f"""
        SELECT alert_time, symbol, alert_type, severity, price, indicator_value, deviation_pct, message
        FROM rt_hose_alerts
        WHERE 1=1 {symbol_filter}
        ORDER BY alert_time DESC
        LIMIT {limit}
        """
    )


@st.cache_data(ttl=60)
def load_realtime_latency() -> dict:
    rows = client().query(
        """
        SELECT
            count() AS total,
            round(avg(date_diff('millisecond', candle_time, received_at)), 1) AS avg_ms,
            round(quantile(0.50)(date_diff('millisecond', candle_time, received_at)), 1) AS p50_ms,
            round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1) AS p95_ms,
            round(quantile(0.99)(date_diff('millisecond', candle_time, received_at)), 1) AS p99_ms,
            round(min(date_diff('millisecond', candle_time, received_at)), 1) AS min_ms,
            round(max(date_diff('millisecond', candle_time, received_at)), 1) AS max_ms
        FROM rt_hose_ohlcv_1m
        WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
          AND candle_time >= now('Asia/Ho_Chi_Minh') - INTERVAL 10 MINUTE
        """
    ).result_rows
    if rows and rows[0][0] > 0:
        total, avg, p50, p95, p99, min_l, max_l = rows[0]
        return {"total": total, "avg": avg, "p50": p50, "p95": p95, "p99": p99, "min": min_l, "max": max_l}
    return {}


def enrich_realtime_candles(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich candles with derived columns (indicators already in rt_hose_indicators)."""
    if df.empty:
        return df
    out = df.copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "sma20" not in out.columns:
        out["sma20"] = out["close"].rolling(20, min_periods=1).mean()
    if "volume_avg20" not in out.columns:
        out["volume_avg20"] = out["volume"].fillna(0).rolling(20, min_periods=1).mean()
    return out


@st.cache_data(ttl=30)
def load_realtime_latency_current() -> dict:
    rows = client().query(
        """
        SELECT symbol, candle_time, received_at,
               date_diff('millisecond', candle_time, received_at) AS latency_ms
        FROM rt_hose_ohlcv_1m
        WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
        ORDER BY received_at DESC
        LIMIT 1
        """
    ).result_rows
    if not rows:
        return {}
    symbol, candle_time, received_at, latency_ms = rows[0]
    return {"symbol": symbol, "candle_time": candle_time, "received_at": received_at, "latency_ms": latency_ms}


@st.cache_data(ttl=30)
def load_realtime_latency_timeseries(window: int = 30) -> pd.DataFrame:
    df = query_df(
        """
        SELECT
            toStartOfMinute(received_at) AS minute,
            count() AS msg_count,
            round(avg(date_diff('millisecond', candle_time, received_at)), 1) AS avg_ms,
            round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1) AS p95_ms
        FROM rt_hose_ohlcv_1m
        WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
          AND candle_time >= now('Asia/Ho_Chi_Minh') - INTERVAL %(window)s MINUTE
        GROUP BY minute
        ORDER BY minute ASC
        """,
        {"window": window},
    )
    if not df.empty:
        df["minute"] = pd.to_datetime(df["minute"])
    return df


@st.cache_data(ttl=30)
def load_realtime_latency_distribution(window: int = 30) -> pd.DataFrame:
    return query_df(
        """
        SELECT
            multiIf(
                lat < 500, '<500ms', lat < 1000, '500-1000ms', lat < 1500, '1000-1500ms',
                lat < 2000, '1500-2000ms', lat < 3000, '2000-3000ms', '>3000ms'
            ) AS bucket,
            count() AS count
        FROM (
            SELECT date_diff('millisecond', candle_time, received_at) AS lat
            FROM rt_hose_ohlcv_1m
            WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
              AND candle_time >= now('Asia/Ho_Chi_Minh') - INTERVAL %(window)s MINUTE
        )
        GROUP BY bucket
        """,
        {"window": window},
    )


@st.cache_data(ttl=30)
def load_realtime_throughput() -> float:
    rows = client().query(
        """
        SELECT count() / 60.0
        FROM rt_hose_ohlcv_1m
        WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
          AND received_at >= now('Asia/Ho_Chi_Minh') - INTERVAL 1 MINUTE
        """
    ).result_rows
    return round(float(rows[0][0]), 1) if rows else 0.0


@st.cache_data(ttl=30)
def load_summary_realtime() -> dict:
    candles = client().query(
        """
        SELECT count()
        FROM rt_hose_ohlcv_1m
        WHERE toDate(candle_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
        """
    ).result_rows
    alerts = client().query(
        """
        SELECT count()
        FROM rt_hose_alerts
        WHERE toDate(alert_time, 'Asia/Ho_Chi_Minh') = toDate(now('Asia/Ho_Chi_Minh'))
        """
    ).result_rows
    return {"candles": candles[0][0] if candles else 0, "alerts": alerts[0][0] if alerts else 0}


@st.cache_data(ttl=30)
def load_last_price_realtime(symbol: str) -> float | None:
    rows = client().query(
        """
        SELECT argMax(close, candle_time)
        FROM rt_hose_ohlcv_1m
        WHERE symbol = %(sym)s
        """,
        parameters={"sym": symbol},
    ).result_rows
    return rows[0][0] if rows and rows[0][0] is not None else None


@st.cache_data(ttl=3600)
def is_trading_day(day) -> bool:
    """True nếu `day` là ngày giao dịch (dim_date.is_day_off = False).

    Trả về False nếu dim_date không có dòng cho ngày đó (vd quá khứ xa
    chưa được build hoặc tương lai xa).
    """
    try:
        from datetime import date as _date
        d = day if isinstance(day, _date) else day.date()
    except Exception:
        return False

    df = query_df(
        """
        SELECT is_day_off
        FROM dim_date
        WHERE full_date = %(day)s
        LIMIT 1
        """,
        {"day": d},
    )
    if df.empty:
        return False
    return not bool(df.iloc[0, 0])


@st.cache_data(ttl=3600)
def previous_close_for(symbol: str, day) -> float | None:
    """Close của phiên giao dịch liền kề trước `day` cho `symbol`.

    `fact_hose_daily_market` không có cột ngày nghỉ, nên JOIN sang `dim_date`
    để lọc `is_day_off = 0`. Trả về None nếu không tìm được phiên trước.
    """
    try:
        from datetime import date as _date
        d = day if isinstance(day, _date) else day.date()
    except Exception:
        return None

    df = query_df(
        """
        SELECT f.close_price
        FROM fact_hose_daily_market AS f
        INNER JOIN dim_symbol AS s ON f.symbol_key = s.symbol_key
        INNER JOIN dim_date   AS d ON f.date_key   = d.date_key
        WHERE s.symbol = %(symbol)s
          AND d.full_date < %(day)s
          AND d.is_day_off = 0
        ORDER BY d.full_date DESC
        LIMIT 1
        """,
        {"symbol": symbol, "day": d},
    )
    if df.empty or df.iloc[0, 0] is None:
        return None
    return float(df.iloc[0, 0])


@st.cache_data(ttl=60)
def load_prev_close(symbols: list[str] | None = None) -> dict[str, float]:
    """Return {symbol: close_price} for the latest EOD date available (= previous trading day)."""
    sym_filter = ""
    params: dict = {}
    if symbols:
        sym_filter = "AND s.symbol IN %(syms)s"
        params["syms"] = symbols
    rows = client().query(
        f"""
        SELECT s.symbol, f.close_price
        FROM fact_hose_daily_market AS f
        INNER JOIN dim_symbol AS s ON f.symbol_key = s.symbol_key
        WHERE f.trading_date = (SELECT max(trading_date) FROM fact_hose_daily_market)
          {sym_filter}
        """,
        parameters=params,
    ).result_rows
    return {str(r[0]): float(r[1]) for r in rows if r[1] is not None}


@st.cache_data(ttl=60)
def load_eod_prices() -> pd.DataFrame:
    df = query_df(
        """
        SELECT
            s.symbol,
            f.trading_date,
            f.close_price AS latest_price,
            f.volume AS latest_quantity,
            f.trading_date AS last_trade_time,
            f.open_price AS open,
            f.high_price AS high,
            f.low_price AS low,
            f.sma20,
            f.ema20,
            f.rsi14,
            f.macd,
            f.avg_volume_20d,
            f.price_change,
            f.pct_change
        FROM fact_hose_daily_market AS f
        INNER JOIN dim_symbol AS s ON f.symbol_key = s.symbol_key
        ORDER BY s.symbol, f.trading_date
        """
    )
    if not df.empty:
        df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


def _base_layout(fig: go.Figure, height: int, margin: dict | None = None) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=margin or dict(l=40, r=40, t=20, b=20),
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#475569", family=PLOT_FONT),
        legend=dict(
            bgcolor="rgba(255,255,255,0.9)", bordercolor="rgba(0,0,0,0.1)", borderwidth=1,
            orientation="h", yanchor="bottom", y=1.0, xanchor="center", x=0.5,
            yref="paper",
        ),
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    fig.update_xaxes(gridcolor="rgba(0,0,0,0.06)")
    return fig


def realtime_candle_figure(*_args, **_kwargs):
    """Removed in v2 (2026-07): superseded by `build_multi_chart()` which renders
    VWAP + σ bands directly from the `rt_hose_indicators` columns.
    Kept as a stub for one release to avoid hard import errors in any external
    script; raise if invoked.
    """
    raise NotImplementedError(
        "realtime_candle_figure() was removed — VWAP + σ are now read from "
        "rt_hose_indicators. Use build_multi_chart() instead."
    )


def realtime_signal_figure(df: pd.DataFrame, symbol: str) -> go.Figure:
    return build_multi_chart(df, symbol)


def build_multi_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.06, row_heights=[0.65, 0.15, 0.20],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": False}]],
    )
    if df.empty:
        return _base_layout(fig, 750)
    times_m = pd.to_datetime(df["candle_time"])
    x_idx_m = [t.strftime("%H:%M") for t in times_m]
    tick_step_m = max(1, len(df) // 8)
    tick_pos_m = list(range(0, len(df), tick_step_m))
    tick_lbl_m = [times_m.iloc[i].strftime("%H:%M") for i in tick_pos_m]  # noqa: F841

    fig.add_trace(
        go.Scatter(x=x_idx_m, y=df["close"], mode="lines", name="Price",
                   xaxis="x", yaxis="y",
                   line=dict(color="#0f172a", width=2),
                   connectgaps=False),
        row=1, col=1,
    )
    if "vwap" in df.columns and df["vwap"].notna().any():
        fig.add_trace(
            go.Scatter(x=x_idx_m, y=df["vwap"], mode="lines", name="VWAP",
                       xaxis="x", yaxis="y",
                       line=dict(color="#2563eb", width=2.5, dash="dash"),
                       connectgaps=False),
            row=1, col=1,
        )
        if "sigma" in df.columns and df["sigma"].notna().any():
            k = BAND_SIGMA_MULTIPLIER
            hi = df["vwap"] + k * df["sigma"]
            lo = df["vwap"] - k * df["sigma"]
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([pd.Series(x_idx_m), pd.Series(x_idx_m[::-1])]),
                    y=pd.concat([hi, lo.iloc[::-1]]),
                    fill="toself", fillcolor="rgba(37, 99, 235, 0.08)",
                    line=dict(color="rgba(0,0,0,0)"), name=f"+/-{k:.0f} sigma", hoverinfo="skip",
                    xaxis="x", yaxis="y",
                ),
                row=1, col=1,
            )
    if "rsi14" in df.columns and df["rsi14"].notna().any():
        fig.add_trace(
            go.Scatter(x=x_idx_m, y=df["rsi14"], mode="lines", name="RSI",
                   xaxis="x2", yaxis="y2",
                   line=dict(color="#9333ea", width=2),
                   connectgaps=False),
        row=2, col=1,
    )
        x_first, x_last = x_idx_m[0], x_idx_m[-1]
        for level, color, dash in ((70, DOWN_COLOR, "dash"), (30, UP_COLOR, "dash"), (50, "rgba(0,0,0,0.25)", "dot")):
            fig.add_trace(
                go.Scatter(
                    x=[x_first, x_last], y=[level, level], mode="lines",
                    xaxis="x2", yaxis="y2",
                    line=dict(color=color, width=1, dash=dash),
                    showlegend=False, hoverinfo="skip",
                ),
                row=2, col=1,
            )
    vol_avg = df["volume"].rolling(20, min_periods=1).mean()
    vol_up = df["close"] >= df["open"]
    vol_spike = df["volume"] >= vol_avg * VOLUME_SPIKE_RATIO
    vol_colors = []
    for i in range(len(df)):
        if vol_spike.iloc[i]:
            vol_colors.append(UP_COLOR if vol_up.iloc[i] else DOWN_COLOR)
        else:
            vol_colors.append("rgba(100, 116, 139, 0.45)")
    fig.add_trace(
        go.Bar(x=x_idx_m, y=df["volume"], name="Volume",
               xaxis="x3", yaxis="y3",
               marker_color=vol_colors, marker_line_width=0),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x_idx_m, y=vol_avg, mode="lines", name="Vol Avg",
                   xaxis="x3", yaxis="y3",
                   line=dict(color="#ea580c", width=1.5, dash="dot"),
                   connectgaps=False),
        row=3, col=1,
    )
    _base_layout(fig, 750)
    # Categorical axis: coi mỗi nến là 1 category, tự bỏ khoảng trống giờ nghỉ trưa
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=2, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=3, col=1)
    fig.update_xaxes(type="category", row=1, col=1)
    fig.update_xaxes(type="category", row=2, col=1)
    fig.update_xaxes(type="category", row=3, col=1,
                     tickvals=tick_pos_m, ticktext=tick_lbl_m, hoverformat="")
    fig.update_yaxes(title_text="Giá", row=1, col=1)
    fig.update_yaxes(title_text="RSI", range=[0, 100], row=2, col=1)
    fig.update_yaxes(title_text="KL", row=3, col=1)
    price_min = pd.to_numeric(df["low"], errors="coerce").min()
    price_max = pd.to_numeric(df["high"], errors="coerce").max()
    pad = (price_max - price_min) * 0.08
    fig.update_yaxes(range=[price_min - pad, price_max + pad], row=1, col=1)
    return fig


def build_candlestick_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Nến + EMA20/SMA20/VWAP overlay (hàng trên), volume bar (hàng dưới).

    Logic tương tự ``candlestick_figure`` trong stock_tab: sử dụng
    ``go.Candlestick`` gốc của Plotly thay vì vẽ wick+bar thủ công,
    overlay EMA20 (chấm), SMA20, VWAP (nét đứt), cùng rangebreaks
    để bỏ khoảng trống cuối tuần / nghỉ lễ.
    """
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.74, 0.26],
    )
    if df.empty:
        return _base_layout(fig, 520)

    times = pd.to_datetime(df["candle_time"])
    x_idx = [t.strftime("%H:%M") for t in times]
    tick_step = max(1, len(df) // 8)
    tick_positions = list(range(0, len(df), tick_step))
    tick_labels = [times.iloc[i].strftime("%H:%M") for i in tick_positions]
    opens = pd.to_numeric(df["open"], errors="coerce")
    highs = pd.to_numeric(df["high"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")
    closes = pd.to_numeric(df["close"], errors="coerce")

    # ─── Row 1: Nến giá ──────────────────────────────────────────────────────────
    fig.add_trace(
        go.Candlestick(
            x=x_idx, open=opens, high=highs, low=lows, close=closes,
            name="Giá",
            increasing=dict(line=dict(color=UP_COLOR), fillcolor=UP_COLOR),
            decreasing=dict(line=dict(color=DOWN_COLOR), fillcolor=DOWN_COLOR),
            showlegend=False,
        ),
        row=1, col=1,
    )

    # ─── EMA20 overlay (dotted) ──────────────────────────────────────────────────
    if "ema20" in df.columns and df["ema20"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=x_idx, y=df["ema20"], name="EMA20",
                line=dict(color=EMA_COLOR, width=1.4, dash="dot"),
                connectgaps=False,
            ),
            row=1, col=1,
        )

    # ─── SMA20 overlay ──────────────────────────────────────────────────────────
    if "sma20" in df.columns and df["sma20"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=x_idx, y=df["sma20"], name="SMA20",
                line=dict(color="#ef4444", width=1.4),
                connectgaps=False,
            ),
            row=1, col=1,
        )

    # ─── VWAP overlay (dashed) ───────────────────────────────────────────────────
    if "vwap" in df.columns and df["vwap"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=x_idx, y=df["vwap"], name="VWAP",
                line=dict(color="#2563eb", width=2, dash="dash"),
                connectgaps=False,
            ),
            row=1, col=1,
        )
        k = BAND_SIGMA_MULTIPLIER
        if "sigma" in df.columns and df["sigma"].notna().any():
            hi = df["vwap"] + k * df["sigma"]
            lo = df["vwap"] - k * df["sigma"]
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([pd.Series(x_idx), pd.Series(x_idx[::-1])]),
                    y=pd.concat([hi.reset_index(drop=True), lo.iloc[::-1].reset_index(drop=True)]),
                    fill="toself", fillcolor="rgba(37, 99, 235, 0.08)",
                    line=dict(color="rgba(0,0,0,0)"), name=f"+/-{k:.0f}\u03c3",
                    hoverinfo="skip", connectgaps=False,
                ),
                row=1, col=1,
            )

    # ─── Row 2: Volume bar ───────────────────────────────────────────────────────
    vol_colors = [UP_COLOR if c >= o else DOWN_COLOR for c, o in zip(closes, opens)]
    fig.add_trace(
        go.Bar(
            x=x_idx, y=df["volume"], marker_color=vol_colors,
            name="Khối lượng", showlegend=False,
        ),
        row=2, col=1,
    )

    # ─── Layout giống stock tab ──────────────────────────────────────────────────
    fig.update_layout(
        height=520, margin=dict(l=40, r=40, t=40, b=20),
        xaxis_rangeslider_visible=False, hovermode="x unified",
        legend=dict(
            bgcolor="rgba(255,255,255,0.9)", bordercolor="rgba(0,0,0,0.1)", borderwidth=1,
            orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#475569", family=PLOT_FONT),
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    fig.update_yaxes(title_text="Giá", row=1, col=1)
    fig.update_yaxes(title_text="KL", row=2, col=1)

    # Dynamic Y-axis zoom with 8% padding
    pmin = float(lows.min())
    pmax = float(highs.max())
    if np.isfinite(pmin) and np.isfinite(pmax) and pmax > pmin:
        pad = (pmax - pmin) * 0.08
        fig.update_yaxes(range=[pmin - pad, pmax + pad], row=1, col=1)

    # ─── Rangebreaks: bỏ khoảng trống cuối tuần + nghỉ lễ ──────
    # Giờ nghỉ trưa 11:30-13:00 ICT = 04:30-06:00 UTC
    present = times
    present_set = set(present.dt.normalize())
    if len(present_set) > 1:
        full = pd.date_range(present.min(), present.max(), freq="D")
        holidays = [d for d in full if d.weekday() < 5 and d not in present_set]
        fig.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),
                dict(values=holidays),
            ]
        )
    fig.update_xaxes(type="category", row=1, col=1,
                     tickvals=tick_positions, ticktext=tick_labels,
                     hoverformat="")
    fig.update_xaxes(type="category", row=2, col=1,
                     tickvals=tick_positions, ticktext=tick_labels,
                     hoverformat="")

    return fig


def build_realtime_bar_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.14, row_heights=[0.75, 0.25],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]],
    )
    if df.empty:
        return _base_layout(fig, 600)
    times_rt = pd.to_datetime(df["candle_time"])
    x_idx_rt = [t.strftime("%H:%M") for t in times_rt]
    tick_step_rt = max(1, len(df) // 8)
    tick_pos_rt = list(range(0, len(df), tick_step_rt))
    tick_lbl_rt = [times_rt.iloc[i].strftime("%H:%M") for i in tick_pos_rt]
    opens = pd.to_numeric(df["open"], errors="coerce")
    highs = pd.to_numeric(df["high"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")
    closes = pd.to_numeric(df["close"], errors="coerce")
    bar_colors = [UP_COLOR if closes.iloc[i] >= opens.iloc[i] else DOWN_COLOR for i in range(len(df))]
    fig.add_trace(
        go.Ohlc(
            x=x_idx_rt,
            open=opens, high=highs, low=lows, close=closes,
            name="OHLC",
            xaxis="x", yaxis="y",
            increasing=dict(line=dict(color=UP_COLOR, width=1.2)),
            decreasing=dict(line=dict(color=DOWN_COLOR, width=1.2)),
            tickwidth=0.4,
        ),
        row=1, col=1,
    )
    if "sma20" in df.columns and df["sma20"].notna().any():
        fig.add_trace(
            go.Scatter(x=x_idx_rt, y=df["sma20"], mode="lines", name="SMA(20)",
                       xaxis="x", yaxis="y",
                       line=dict(color="#ef4444", width=2.5), connectgaps=False),
            row=1, col=1,
        )
    if "vwap" in df.columns and df["vwap"].notna().any():
        fig.add_trace(
            go.Scatter(x=x_idx_rt, y=df["vwap"], mode="lines", name="VWAP",
                       xaxis="x", yaxis="y",
                       line=dict(color="#2563eb", width=2.5, dash="dash"), connectgaps=False),
            row=1, col=1,
        )
    vol_avg = pd.to_numeric(df["volume"], errors="coerce").rolling(20, min_periods=1).mean()
    fig.add_trace(
        go.Bar(x=x_idx_rt, y=df["volume"], name="Volume",
               xaxis="x2", yaxis="y2",
               marker_color=bar_colors, marker_line_width=0),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x_idx_rt, y=vol_avg, mode="lines", name="Vol Avg",
                   xaxis="x2", yaxis="y2",
                   line=dict(color="#ea580c", width=1.5, dash="dot"), connectgaps=False),
        row=2, col=1,
    )
    _base_layout(fig, 600)
    # Categorical axis: coi mỗi nến là 1 category, tự bỏ khoảng trống giờ nghỉ trưa
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=2, col=1)
    fig.update_xaxes(type="category", row=1, col=1,
                     tickvals=tick_pos_rt, ticktext=tick_lbl_rt, hoverformat="")
    fig.update_xaxes(type="category", row=2, col=1,
                     tickvals=tick_pos_rt, ticktext=tick_lbl_rt, hoverformat="")
    fig.update_yaxes(title_text="Giá", row=1, col=1)
    fig.update_yaxes(title_text="KL", row=2, col=1)
    price_min = lows.min()
    price_max = highs.max()
    pad = (price_max - price_min) * 0.08
    fig.update_yaxes(range=[price_min - pad, price_max + pad], row=1, col=1)
    return fig


def latency_over_time_figure(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(go.Scatter(x=df["minute"], y=df["avg_ms"], mode="lines+markers", name="Avg", line=dict(color=VNINDEX_COLOR, width=2), marker=dict(size=4)))
        fig.add_trace(go.Scatter(x=df["minute"], y=df["p95_ms"], mode="lines", name="p95", line=dict(color=DOWN_COLOR, width=1.8, dash="dash")))
    _base_layout(fig, 320, dict(l=40, r=40, t=60, b=40))
    fig.update_layout(title=dict(text="Latency over time", font=dict(size=16, color="#1e293b")), xaxis=dict(title="Thời gian"))
    fig.update_yaxes(title="Latency (ms)")
    return fig


def latency_distribution_figure(df: pd.DataFrame) -> go.Figure:
    bucket_order = ["<500ms", "500-1000ms", "1000-1500ms", "1500-2000ms", "2000-3000ms", ">3000ms"]
    colors = [UP_COLOR, "#34d399", "#fbbf24", "#f59e0b", DOWN_COLOR, "#991b1b"]
    data = pd.DataFrame({"bucket": bucket_order, "count": [0] * len(bucket_order)}) if df.empty else df.set_index("bucket").reindex(bucket_order).fillna(0).reset_index()
    fig = go.Figure(go.Bar(x=data["bucket"], y=data["count"], marker_color=colors, text=data["count"].astype(int), textposition="outside"))
    _base_layout(fig, 320, dict(l=40, r=40, t=60, b=40))
    fig.update_layout(title=dict(text="Latency distribution", font=dict(size=16, color="#1e293b")), showlegend=False, xaxis=dict(title="Khoảng latency"))
    fig.update_yaxes(title="Số messages")
    return fig