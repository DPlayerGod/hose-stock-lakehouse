from __future__ import annotations

from common import *

# VIEW 2 — MARKET OVERVIEW
# ════════════════════════════════════════════════════════════════════════════════
def render_market_view(batch_ready: bool) -> None:
    if not batch_ready:
        st.warning(
            "Chưa có dữ liệu batch trong ClickHouse. "
            "Chạy `docker compose --profile pipeline up` rồi trigger DAG để nạp Gold."
        )
        return
    idx = load_index()
    prices = load_prices()
    if idx.empty:
        st.info("Chưa có dữ liệu chỉ số (fact_hose_index_daily). Trigger `dag_daily_index` để nạp.")
        return

    def idx_last(code: str) -> pd.Series | None:
        sub = idx[idx["index_code"] == code].sort_values("trading_date")
        return sub.iloc[-1] if not sub.empty else None

    vni = idx_last("VNINDEX")
    vn30 = idx_last("VN30")

    # ── 4 thẻ chỉ số thị trường ──────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    if vni is not None:
        c1.metric("VN-Index (điểm)", vn_dec(vni["close_price"], 1),
                  f"{vn_dec(vni['price_change'], 1)} ({fmt_pct(vni['pct_change'])})"
                  if pd.notna(vni["price_change"]) else None)
    if vn30 is not None:
        c2.metric("VN30 (điểm)", vn_dec(vn30["close_price"], 1),
                  f"{vn_dec(vn30['price_change'], 1)} ({fmt_pct(vn30['pct_change'])})"
                  if pd.notna(vn30["price_change"]) else None)

    # KLGD = volume VN-Index (khớp toàn thị trường); GTGD ước tính từ nhóm theo dõi.
    # TB20 dùng thẳng cột avg_volume_20d của Gold (warmup chuẩn: null tới khi đủ 20 phiên).
    if vni is not None:
        tb20_vol = vni["avg_volume_20d"]
        c3.metric("Tổng KLGD", fmt_vol(vni["volume"]),
                  f"TB20: {fmt_vol(tb20_vol)}" if pd.notna(tb20_vol) else None,
                  delta_color="off")
    if not prices.empty:
        latest = prices["trading_date"].max()
        day = prices[prices["trading_date"] == latest]
        # Giá lưu theo đơn vị nghìn VND ⇒ GTGD (tỷ VND) = Σ(close·volume)·1000 / 1e9 = Σ(close·volume)/1e6.
        gtgd = (day["close_price"] * day["volume"]).sum()
        recent = prices[prices["trading_date"] >= latest - pd.Timedelta(days=40)].copy()
        recent["gtgd"] = recent["close_price"] * recent["volume"]
        tb20_val = recent.groupby("trading_date")["gtgd"].sum().tail(20).mean()
        c4.metric("GTGD (nhóm theo dõi)", f"{vn_int(gtgd / 1e6)} tỷ",
                  f"TB20: {vn_int(tb20_val / 1e6)} tỷ", delta_color="off")

    st.write("")

    # ── VN-Index vs VN30 (3 tháng) ────────────────────────────────────────────────
    with st.container(border=True):
        st.markdown("#### VN-Index vs VN30 — 3 tháng")
        cutoff = idx["trading_date"].max() - pd.Timedelta(days=90)
        fig = go.Figure()
        for code, color in (("VNINDEX", VNINDEX_COLOR), ("VN30", VN30_COLOR)):
            sub = idx[(idx["index_code"] == code) & (idx["trading_date"] >= cutoff)].sort_values("trading_date")
            if not sub.empty:
                fig.add_trace(go.Scatter(
                    x=sub["trading_date"], y=sub["close_price"],
                    name="VN-Index" if code == "VNINDEX" else code,
                    line=dict(color=color, width=1.8),
                ))
        fig.update_layout(
            height=320, margin=dict(l=40, r=40, t=40, b=20), hovermode="x unified",
            legend=dict(
                bgcolor="rgba(255,255,255,0.9)", bordercolor="rgba(0,0,0,0.1)", borderwidth=1,
                orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1,
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#475569", family="Inter"),
        )
        fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

    st.write("")

    # ── Hiệu suất & tương quan so với VN-Index (20 phiên) ─────────────────────────
    perf, corr = compute_relative_to_index(prices, idx)
    left, right = st.columns(2)
    with left:
        with st.container(border=True):
            st.markdown("#### Hiệu suất so với VN-Index (20 phiên)")
            if perf:
                bar = go.Figure(go.Bar(
                    x=list(perf.keys()), y=[v * 100 for v in perf.values()],
                    marker_color=[UP_COLOR if v >= 0 else DOWN_COLOR for v in perf.values()],
                ))
                bar.update_layout(height=260, margin=dict(l=40, r=40, t=10, b=20),
                                  paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                  font=dict(color="#475569", family="Inter"), yaxis_title="%")
                bar.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=True, zerolinecolor="#d1d5db")
                st.plotly_chart(bar, width="stretch", config={"displayModeBar": False})
            else:
                st.caption("Chưa đủ dữ liệu (cần ≥20 phiên).")
    with right:
        with st.container(border=True):
            st.markdown("#### Tương quan với VN-Index (20 phiên)")
            if corr:
                bar = go.Figure(go.Bar(
                    x=list(corr.keys()), y=list(corr.values()), marker_color=VNINDEX_COLOR,
                ))
                bar.update_layout(height=260, margin=dict(l=40, r=40, t=10, b=20),
                                  paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                  font=dict(color="#475569", family="Inter"), yaxis=dict(range=[-1, 1.2]))
                bar.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=True, zerolinecolor="#d1d5db")
                st.plotly_chart(bar, width="stretch", config={"displayModeBar": False})
            else:
                st.caption("Chưa đủ dữ liệu (cần ≥20 phiên).")


def compute_relative_to_index(
    prices: pd.DataFrame, idx: pd.DataFrame,
) -> tuple[dict[str, float], dict[str, float]]:
    """20-phiên: hiệu suất (return mã − return VN-Index) & tương quan pct_change với VN-Index."""
    perf: dict[str, float] = {}
    corr: dict[str, float] = {}
    if prices.empty or idx.empty:
        return perf, corr
    vni = idx[idx["index_code"] == "VNINDEX"].sort_values("trading_date").tail(20)
    if len(vni) < 20:
        return perf, corr
    vni_ret = vni["close_price"].iloc[-1] / vni["close_price"].iloc[0] - 1
    vni_series = vni.set_index("trading_date")["pct_change"]

    for sym in sorted(prices["symbol"].unique()):
        sub = prices[prices["symbol"] == sym].sort_values("trading_date").tail(20)
        if len(sub) < 20:
            continue
        sym_ret = sub["close_price"].iloc[-1] / sub["close_price"].iloc[0] - 1
        perf[sym] = sym_ret - vni_ret
        joined = pd.concat(
            [sub.set_index("trading_date")["pct_change"], vni_series], axis=1, join="inner"
        ).dropna()
        if len(joined) >= 5:
            c = joined.iloc[:, 0].corr(joined.iloc[:, 1])
            if pd.notna(c):
                corr[sym] = round(float(c), 2)
    return perf, corr


