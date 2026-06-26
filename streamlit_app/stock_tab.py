from __future__ import annotations

from common import *

# VIEW 1 — CỔ PHIẾU
# ════════════════════════════════════════════════════════════════════════════════
def render_stock_view(batch_ready: bool) -> None:
    if not batch_ready:
        st.warning(
            "Chưa có dữ liệu batch trong ClickHouse. "
            "Chạy `docker compose --profile pipeline up` rồi trigger DAG để nạp Gold."
        )
        return
    symbols = load_symbols()
    options = symbols["symbol"].tolist() if "symbol" in symbols.columns else []
    if not options:
        st.warning("Không có mã nào trong dim_symbol.")
        return

    sym = st.segmented_control("symbol", options, default=options[0], label_visibility="collapsed")
    sym = sym or options[0]

    prices = load_prices()
    df = cast(pd.DataFrame, prices[prices["symbol"] == sym]).sort_values("trading_date")
    if df.empty:
        st.warning(f"Không có dữ liệu giá cho {sym}.")
        return
    last = df.iloc[-1]

    # ── 4 thẻ chỉ số ────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Giá đóng cửa", vn_dec(last["close_price"], 2),
        f"{vn_dec(last['price_change'], 2)} ({fmt_pct(last['pct_change'])})"
        if pd.notna(last["price_change"]) else None,
    )
    c2.metric(
        "Khối lượng", fmt_vol(last["volume"]),
        f"TB20: {fmt_vol(last['avg_volume_20d'])}", delta_color="off",
    )
    rsi = last["rsi14"]
    rsi_zone = "Quá mua" if pd.notna(rsi) and rsi >= 70 else "Quá bán" if pd.notna(rsi) and rsi <= 30 else "Trung tính"
    c3.metric("RSI (14)", vn_dec(rsi, 1), rsi_zone, delta_color="off")
    above = pd.notna(last["ema20"]) and last["close_price"] >= last["ema20"]
    c4.metric(
        "EMA (20)", vn_dec(last["ema20"], 2),
        ("Trên EMA ▲" if above else "Dưới EMA ▼") if pd.notna(last["ema20"]) else None,
        delta_color="normal" if above else "inverse",
    )

    st.write("")

    # ── Nến + sự kiện ────────────────────────────────────────────────────────────
    st.markdown(f"**{sym} — Biểu đồ nến + sự kiện**")
    col_l, col_r = st.columns([8, 3])
    with col_r:
        tf = st.radio(
            "tf", ["1T", "3T", "6T", "1N"], index=2,
            horizontal=True, label_visibility="collapsed",
        )
    st.markdown(
        "<style>[data-testid='stRadio'] [data-testid='stHorizontalBlock'] > div { justify-content: flex-end; }</style>",
        unsafe_allow_html=True,
    )
    tf = tf or "6T"
    days = {"1T": 30, "3T": 90, "6T": 180, "1N": 365}[tf]
    cutoff = df["trading_date"].max() - pd.Timedelta(days=days)
    cdf = cast(pd.DataFrame, df[df["trading_date"] >= cutoff])

    events = load_events()
    sym_events = cast(pd.DataFrame, events[events["symbol"] == sym]) if not events.empty else events
    chart_box = st.container(border=True)
    with chart_box:
        st.plotly_chart(
            candlestick_figure(cdf, sym_events), width="stretch",
            config={"displayModeBar": False},
        )

    st.write("")

    # ── Sự kiện doanh nghiệp + Chỉ báo kỹ thuật ───────────────────────────────────
    left, right = st.columns(2)
    with left:
        render_events_card(sym)
    with right:
        render_indicator_card(last)


def candlestick_figure(df: pd.DataFrame, events: pd.DataFrame) -> go.Figure:
    """Nến + EMA20 overlay + marker sự kiện (hàng trên), volume bar (hàng dưới)."""
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.74, 0.26],
    )
    fig.add_trace(
        go.Candlestick(
            x=df["trading_date"], open=df["open_price"], high=df["high_price"],
            low=df["low_price"], close=df["close_price"], name="Giá",
            increasing=dict(line=dict(color=UP_COLOR), fillcolor=UP_COLOR),
            decreasing=dict(line=dict(color=DOWN_COLOR), fillcolor=DOWN_COLOR),
            showlegend=False,
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["trading_date"], y=df["ema20"], name="EMA20",
            line=dict(color=EMA_COLOR, width=1.4, dash="dot"),
        ),
        row=1, col=1,
    )

    # Marker sự kiện: tam giác xanh ngay trên đỉnh nến của ngày có sự kiện.
    if events is not None and not events.empty and not df.empty:
        window = events[
            (events["event_date"] >= df["trading_date"].min())
            & (events["event_date"] <= df["trading_date"].max())
        ]
        if not window.empty:
            high_by_date = df.set_index("trading_date")["high_price"]
            ys, xs, texts = [], [], []
            for ev in window.itertuples():
                nearest = high_by_date.index.asof(ev.event_date)
                if pd.isna(nearest):
                    continue
                xs.append(ev.event_date)
                ys.append(high_by_date.loc[nearest] * 1.02)
                texts.append(f"{ev.event_label}: {ev.title_vi or ''}".strip(": "))
            if xs:
                fig.add_trace(
                    go.Scatter(
                        x=xs, y=ys, mode="markers", name="Sự kiện",
                        marker=dict(symbol="triangle-up", size=11, color=EVENT_COLOR),
                        text=texts, hovertemplate="%{text}<extra></extra>",
                    ),
                    row=1, col=1,
                )

    vol_colors = [UP_COLOR if c >= o else DOWN_COLOR
                  for c, o in zip(df["close_price"], df["open_price"])]
    fig.add_trace(
        go.Bar(x=df["trading_date"], y=df["volume"], marker_color=vol_colors,
               name="Khối lượng", showlegend=False),
        row=2, col=1,
    )

    fig.update_layout(
        height=520, margin=dict(l=40, r=40, t=40, b=20),
        xaxis_rangeslider_visible=False, hovermode="x unified",
        legend=dict(
            bgcolor="rgba(255,255,255,0.9)", bordercolor="rgba(0,0,0,0.1)", borderwidth=1,
            orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#475569", family="Inter"),
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    # Bỏ khoảng trống ngày không giao dịch (cuối tuần + nghỉ lễ HOSE) cho liền mạch.
    present = df["trading_date"]
    present_set = set(present.dt.normalize())
    full = pd.date_range(present.min(), present.max(), freq="D")
    holidays = [d for d in full if d.weekday() < 5 and d not in present_set]
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"]), dict(values=holidays)])
    return fig


def render_events_card(sym: str) -> None:
    events = load_events()
    sym_events = (
        cast(pd.DataFrame, events[events["symbol"] == sym]).head(6)
        if not events.empty else events
    )
    today = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").normalize().tz_localize(None)
    rows = []
    for ev in sym_events.itertuples():
        tag, fg, bg = event_tag(str(ev.event_code), str(ev.event_label))
        title = ev.title_vi or ev.event_label
        if pd.notna(ev.value) and ev.value:
            title = f"{title} · {vn_int(cast(float, ev.value))}đ/cp"
        event_date = cast(pd.Timestamp, ev.event_date)
        soon = '<span class="ha-soon">Sắp tới</span>' if event_date > today else ""
        rows.append(
            f'<div class="ha-evt">'
            f'<span style="display:inline-flex;align-items:center;font-size:.7rem;font-weight:600;'
            f'padding:3px 10px;border-radius:999px;white-space:nowrap;line-height:1.1;letter-spacing:.02em;'
            f'color:{fg};background:{bg};border:1.5px solid {fg}">{tag}</span>'
            f'<div><div class="ha-evt-body">{title}</div>'
            f'<div class="ha-evt-date">{event_date.strftime("%d/%m/%Y")}<span class="ha-soon-wrap">{soon}</span></div></div></div>'
        )
    body = "".join(rows) if rows else '<div class="ha-evt-date">Chưa có sự kiện.</div>'
    st.markdown(
        f'<div class="ha-card"><h4>🗓️ Sự kiện doanh nghiệp</h4>{body}</div>',
        unsafe_allow_html=True,
    )


def render_indicator_card(last: pd.Series) -> None:
    close = last["close_price"]
    rows = [
        ("SMA (20)", vn_dec(last["sma20"], 2), trend_signal(close, last["sma20"])),
        ("EMA (20)", vn_dec(last["ema20"], 2), trend_signal(close, last["ema20"])),
        ("RSI (14)", vn_dec(last["rsi14"], 1), rsi_signal(last["rsi14"])),
        ("MACD", vn_dec(last["macd"], 2), macd_signal(last["macd"])),
    ]
    body = "".join(
        f'<div class="ha-ind"><span class="ha-ind-name">{name}</span>'
        f'<span><span class="ha-ind-val">{val}</span>&nbsp;&nbsp;{signal_badge(sig)}</span></div>'
        for name, val, sig in rows
    )
    st.markdown(
        f'<div class="ha-card"><h4>📊 Chỉ báo kỹ thuật</h4>{body}</div>',
        unsafe_allow_html=True,
    )


# ════════════════════════════════════════════════════════════════════════════════
