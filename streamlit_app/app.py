"""HOSE Analytics Streamlit entrypoint."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

st.set_page_config(
    page_title="HOSE Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

from common import REFRESH_SEC, ICT, load_meta, inject_styles
inject_styles()
from market_tab import render_market_view
from realtime_tab import render_realtime_view
from stock_tab import render_stock_view

try:
    meta = load_meta()
    batch_ready = meta["latest_date"] is not None
except Exception:
    meta = {"latest_date": None, "rows": 0}
    batch_ready = False

with st.sidebar:
    st.title("DNSE Streaming")
    st.caption("Powered by ClickHouse Kafka Engine")

st.title("HOSE Analytics")

view = st.segmented_control(
    "view", ["Cổ phiếu", "Market Overview", "Realtime"], default="Cổ phiếu",
    label_visibility="collapsed",
)
view = view or "Cổ phiếu"

# ── "Cập nhật" label: context-aware per view ──
if view == "Realtime":
    now_ict = datetime.now(ICT)
    updated_label = now_ict.strftime("%d/%m/%Y %H:%M")
    st.markdown(f'<p class="ha-sub">Cập nhật {updated_label}</p>', unsafe_allow_html=True)
elif batch_ready:
    latest_date = meta["latest_date"]
    updated_label = latest_date.strftime("%d/%m/%Y") + " 15:00"
    st.markdown(f'<p class="ha-sub">Cập nhật {updated_label}</p>', unsafe_allow_html=True)
else:
    st.markdown('<p class="ha-sub">Chưa có dữ liệu batch - Realtime vẫn hoạt động</p>', unsafe_allow_html=True)

st.write("")

if view == "Market Overview":
    render_market_view(batch_ready)
elif view == "Realtime":
    render_realtime_view()
else:
    render_stock_view(batch_ready)

if view == "Realtime":
    time.sleep(REFRESH_SEC)
    st.rerun()

