from __future__ import annotations

from common import *


def aggregate_intraday_candles(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty or timeframe == "1 phút":
        return df
    freq_map = {"5 phút": "5min", "15 phút": "15min"}
    freq = freq_map.get(timeframe)
    if not freq:
        return df

    work = df.sort_values("candle_time").copy().set_index("candle_time")
    grouped = work.resample(freq).agg(
        symbol=("symbol", "last"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )
    return grouped.dropna(subset=["open", "high", "low", "close"]).reset_index()


def realtime_symbol_options(latest_prices: pd.DataFrame) -> list[str]:
    symbols: list[str] = []
    if "symbol" in latest_prices.columns and not latest_prices.empty:
        symbols.extend(str(s) for s in latest_prices["symbol"].dropna().unique())
    for sym in SYMBOLS:
        if len(symbols) >= 5:
            break
        if sym not in symbols:
            symbols.append(sym)
    return symbols[:5]


def eod_as_intraday_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.rename(columns={"trading_date": "candle_time", "latest_price": "close", "latest_quantity": "volume"}).copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def signal_snapshot_from_candles(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    last = df.iloc[-1]
    latest_price = float(last["close"])
    vwap = last.get("vwap")
    sma20 = last.get("sma20")
    rsi14 = last.get("rsi14")
    signal = "NEUTRAL"
    if pd.notna(vwap) and pd.notna(sma20):
        if latest_price > float(vwap) and latest_price > float(sma20):
            signal = "BULLISH"
        elif latest_price < float(vwap) and latest_price < float(sma20):
            signal = "BEARISH"
    return pd.DataFrame([
        {
            "symbol": symbol,
            "latest_price": latest_price,
            "vwap": vwap,
            "sma20": sma20,
            "rsi14": rsi14,
            "signal_type": signal,
            "created_at": last["candle_time"],
            "source": "computed_from_candles",
        }
    ])


def render_realtime_view() -> None:
    today_date = datetime.now(ICT).date()
    summary = load_summary_realtime()
    realtime_today = summary.get("candles", 0) > 0
    latest_prices = load_latest_prices()
    market_closed = not realtime_today
    eod = pd.DataFrame()

    if latest_prices.empty or market_closed:
        eod = load_eod_prices()
        if latest_prices.empty and eod.empty:
            st.info("Chưa có dữ liệu realtime/EOD. Kiểm tra Kafka, OHLC producer và DDL streaming.")
            return
        if latest_prices.empty:
            latest_prices = eod.sort_values(["symbol", "trading_date"]).groupby("symbol", as_index=False).last()
        if market_closed:
            st.info(
                "**Phiên đang đóng hoặc chưa có nến realtime hôm nay - đang hiển thị dữ liệu EOD gần nhất.** "
                "Khi realtime có dữ liệu, view này sẽ tự chuyển sang intraday."
            )

    st.markdown("#### Realtime")
    options = realtime_symbol_options(latest_prices)
    default_symbol = options[0] if options else SYMBOLS[0]
    default_index = options.index(default_symbol) if default_symbol in options else 0

    ctrl_symbol, ctrl_date, ctrl_tf, ctrl_chart = st.columns([1.3, 1, 1, 1.2])
    with ctrl_symbol:
        rt_symbol = st.selectbox("Mã CK", options or SYMBOLS, index=default_index, key="rt_symbol")
    with ctrl_date:
        selected_day = st.date_input("Ngày", value=today_date, key="rt_trading_day")
    with ctrl_tf:
        rt_timeframe = st.selectbox("Khung nến", ["1 phút", "5 phút", "15 phút", "Ngày"], key="rt_timeframe")
    with ctrl_chart:
        chart_style = st.selectbox("Kiểu biểu đồ", ["Nến", "Multi indicator"], key="rt_chart_style")

    prev_close_map = load_prev_close()
    sym_row = latest_prices[latest_prices["symbol"] == rt_symbol]
    if not sym_row.empty:
        row = sym_row.iloc[-1]
        price = row.get("latest_price")
        qty = row.get("latest_quantity")
        ref_price = prev_close_map.get(rt_symbol)
        if pd.notna(price) and ref_price and ref_price != 0:
            pct = (float(price) - ref_price) / ref_price
        else:
            pct = 0.0
        c1, c2 = st.columns([1, 1])
        c1.metric("Giá realtime", vn_dec(cast(float, price), 2) if pd.notna(price) else "-",
                  fmt_pct(pct))
        c2.metric("Khối lượng", vn_int(cast(float, qty)) if pd.notna(qty) else "-")

    ctrl_left, ctrl_right = st.columns([1, 1])
    with ctrl_left:
        candle_count = st.slider("Số nến", 10, 200, 60, step=10, key="rt_candle_count")
    with ctrl_right:
        rt_start_time = st.time_input("Từ giờ", value=datetime.strptime("09:00", "%H:%M").time(), key="rt_start_time")

    st.divider()

    sym_candles = pd.DataFrame()
    if rt_timeframe == "Ngày" or market_closed:
        if eod.empty:
            eod = load_eod_prices()
        sym_eod = eod[eod["symbol"] == rt_symbol].sort_values("trading_date")
        sym_eod = sym_eod[sym_eod["trading_date"].dt.date <= selected_day].tail(candle_count)  # type: ignore[attr-defined]
        latest_day = sym_eod["trading_date"].max() if not sym_eod.empty else pd.NaT
        suffix = latest_day.strftime("%d/%m/%Y") if pd.notna(latest_day) else "EOD"
        st.markdown(f"**{rt_symbol}** - EOD ({suffix})")
        eod_chart_df = enrich_realtime_candles(eod_as_intraday_frame(sym_eod))
        if chart_style == "Bar + SMA/VWAP":
            fig = build_realtime_bar_chart(eod_chart_df, rt_symbol)
        elif chart_style == "Nến":
            fig = build_candlestick_chart(eod_chart_df, rt_symbol)
        else:
            fig = eod_realtime_fallback_figure(sym_eod, rt_symbol)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        candles_raw = load_realtime_candles(rt_symbol, candle_count * 20, selected_day)
        if not candles_raw.empty:
            start_dt = pd.Timestamp(
                year=selected_day.year,
                month=selected_day.month,
                day=selected_day.day,
                hour=rt_start_time.hour,
                minute=rt_start_time.minute,
                tz="Asia/Ho_Chi_Minh",
            )
            candles_raw = candles_raw[candles_raw["candle_time"] >= start_dt]
        candles_raw = aggregate_intraday_candles(candles_raw, rt_timeframe)
        sym_candles = enrich_realtime_candles(candles_raw).tail(candle_count)

        if sym_candles.empty:
            st.info(f"Chưa có nến intraday cho {rt_symbol} ngày {selected_day:%d/%m/%Y}.")
        else:
            st.markdown(f"**{rt_symbol}** - {rt_timeframe} ({selected_day:%d/%m/%Y})")
            if chart_style == "Nến":
                fig = build_candlestick_chart(sym_candles, rt_symbol)
            elif chart_style == "Multi indicator":
                fig = build_multi_chart(sym_candles, rt_symbol)
            else:
                fig = build_realtime_bar_chart(sym_candles, rt_symbol)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

            rsi_now = sym_candles["rsi14"].iloc[-1] if sym_candles["rsi14"].notna().any() else None
            vol_avg_20 = sym_candles["volume"].tail(21).head(20).mean()
            vol_current = sym_candles["volume"].iloc[-1]
            vol_ratio = vol_current / vol_avg_20 if vol_avg_20 > 0 else 0.0
            vwap_now = sym_candles["vwap"].iloc[-1] if sym_candles["vwap"].notna().any() else None
            r1, r2, r3 = st.columns(3)
            r1.metric(f"RSI({RSI_PERIOD})", f"{rsi_now:.1f}" if rsi_now and not pd.isna(rsi_now) else "-")
            r2.metric("Vol Ratio", f"{vol_ratio:.1f}x")
            r3.metric("VWAP", f"{vwap_now:.2f}" if vwap_now and not pd.isna(vwap_now) else "-")

    st.divider()

    tab_signal, tab_latency = st.tabs(["Signal Detection", "Latency Monitor"])

    with tab_signal:
        signals = load_realtime_signals()
        if not signals.empty:
            st.markdown("**Latest realtime signals**")
            st.dataframe(signals.head(20), use_container_width=True, hide_index=True)
        else:
            fallback_signal = signal_snapshot_from_candles(sym_candles, rt_symbol)
            if fallback_signal.empty:
                st.info("Chưa có tín hiệu trong phiên.")
            else:
                st.caption("Bảng signal chưa có dữ liệu, đang tính snapshot từ nến realtime hiện có.")
                st.dataframe(fallback_signal, use_container_width=True, hide_index=True)

        alerts = load_realtime_alerts()
        if not alerts.empty:
            st.markdown("**Recent alerts**")
            display_cols = [
                c for c in ["alert_time", "symbol", "alert_type", "severity", "price", "indicator_value", "deviation_pct", "message"]
                if c in alerts.columns
            ]
            st.dataframe(alerts[display_cols].head(20), use_container_width=True, hide_index=True)

    with tab_latency:
        if market_closed:
            st.caption("Độ trễ pipeline không khả dụng ngoài giờ giao dịch hoặc khi chưa có nến realtime hôm nay.")
        else:
            latency_window = st.slider("Cửa sổ phân tích (phút)", 5, 60, 30, step=5, key="latency_window")
            current = load_realtime_latency_current()
            summary_lat = load_realtime_latency()
            throughput = load_realtime_throughput()
            total_today = summary.get("candles", 0)

            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("Current", f"{current['latency_ms']:.0f}ms" if current else "-")
            m2.metric("Avg", f"{summary_lat.get('avg', 0):.0f}ms" if summary_lat else "-")
            m3.metric("p95", f"{summary_lat.get('p95', 0):.0f}ms" if summary_lat else "-")
            m4.metric("p99", f"{summary_lat.get('p99', 0):.0f}ms" if summary_lat else "-")
            m5.metric("Msgs/s", f"{throughput:.1f}")
            m6.metric("Total", f"{total_today:,}")

            st.divider()

            col_line, col_dist = st.columns(2)
            with col_line:
                st.plotly_chart(latency_over_time_figure(load_realtime_latency_timeseries(latency_window)), use_container_width=True, config={"displayModeBar": False})
            with col_dist:
                st.plotly_chart(latency_distribution_figure(load_realtime_latency_distribution(latency_window)), use_container_width=True, config={"displayModeBar": False})

            if current:
                received_at = pd.Timestamp(current["received_at"])
                age_sec = (
                    (pd.Timestamp.now(tz=received_at.tz) - received_at).total_seconds()
                    if received_at.tzinfo
                    else (pd.Timestamp.now() - received_at).total_seconds()
                )
                if age_sec < 120:
                    st.success(f"Kết nối hoạt động - message gần nhất cách đây {age_sec:.0f}s [{current['symbol']}]")
                elif age_sec < 300:
                    st.warning(f"Dữ liệu chậm - message gần nhất cách đây {age_sec:.0f}s [{current['symbol']}]")
                else:
                    st.error(f"Mất kết nối - không có dữ liệu mới trong {age_sec:.0f}s")
            else:
                st.error("Không có dữ liệu streaming hôm nay. Kiểm tra producer + Kafka.")