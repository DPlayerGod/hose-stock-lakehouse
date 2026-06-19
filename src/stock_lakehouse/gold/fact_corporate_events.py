"""Gold = factless fact cho sự kiện doanh nghiệp.

Khác fact giá/chỉ số: **không** indicator/rolling và measure rất thưa (``value`` = cổ
tức tiền/cp, chỉ có ở sự kiện DIV). Đây là **factless fact** — bảng ghi *sự kiện xảy ra*,
gắn FK fail-loud tới ``dim_symbol`` (``symbol_key``) + ``dim_date`` (``date_key``) để join
star-schema. Giữ ``symbol`` denormalized cho tiện serving (lọc theo mã ở Streamlit).

Idempotency: feed trả full lịch sử ⇒ fact dựng lại từ toàn bộ silver rồi **overwrite**
cả bảng (dedup theo ``event_id``). Chạy lại cho ra cùng tập rows.

Sự kiện có thể có ``event_date`` *trước* phạm vi ``dim_date`` (vd cổ tức 2018, trong khi
dim_date phủ 2020–2030). Các sự kiện ngoài phạm vi này không gắn được lịch/không vẽ
được marker (giá cũng chưa có lịch sử đó) nên bị **lọc bỏ ở Gold + log rõ số lượng**.
Sự kiện *trong* phạm vi nhưng thiếu khỏi dim_date vẫn **fail-loud** (lỗi thật).
"""
from __future__ import annotations

import logging

import polars as pl

from stock_lakehouse.quality import validate_fact_corporate_events
from stock_lakehouse.utils.dates import now_utc


logger = logging.getLogger(__name__)


FACT_CORPORATE_EVENTS_COLUMNS = (
    "event_id",
    "symbol_key",
    "date_key",
    "symbol",
    "event_date",
    "event_code",
    "event_label",
    "title_vi",
    "value",
    "updated_at",
)


def build_fact_corporate_events(
    silver_events_df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
) -> pl.DataFrame:
    """Dựng ``fact_corporate_events`` từ toàn bộ silver + 2 dim.

    Bước: lọc event ngoài phạm vi dim_date (log) → join dim_symbol (FK fail-loud) →
    join dim_date (FK fail-loud) → dedup ``event_id`` → validate.
    """
    events = silver_events_df.with_columns(
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("event_date").cast(pl.Date, strict=False),
    )
    events = _drop_out_of_range(events, dim_date_df)

    fact = (
        _attach_symbol_key(events, dim_symbol_df)
        .pipe(_attach_date_key, dim_date_df)
        .with_columns(pl.lit(now_utc()).alias("updated_at"))
        .unique(subset=["event_id"], keep="last", maintain_order=True)
        .select(FACT_CORPORATE_EVENTS_COLUMNS)
        .sort("symbol", "event_date", "event_id")
    )
    validate_fact_corporate_events(fact, dim_symbol_df, dim_date_df).raise_for_errors()
    return fact


def _drop_out_of_range(events: pl.DataFrame, dim_date_df: pl.DataFrame) -> pl.DataFrame:
    """Bỏ sự kiện có ``event_date`` ngoài [min, max] của dim_date; log số lượng bị bỏ."""
    if events.is_empty():
        return events
    lo = dim_date_df.get_column("full_date").min()
    hi = dim_date_df.get_column("full_date").max()
    in_range = events.filter(pl.col("event_date").is_between(lo, hi))
    dropped = events.height - in_range.height
    if dropped:
        sample = events.filter(~pl.col("event_date").is_between(lo, hi)).get_column("event_id").to_list()
        logger.warning(
            "fact_corporate_events: bỏ %d sự kiện ngoài phạm vi dim_date [%s..%s] "
            "(pre-history, không chart được). event_id mẫu: %s",
            dropped, lo, hi, sample[:10],
        )
    return in_range


def _attach_symbol_key(df: pl.DataFrame, dim_symbol_df: pl.DataFrame) -> pl.DataFrame:
    """LEFT-join ``symbol_key`` từ dim_symbol; symbol lạ ⇒ key null ⇒ fail-loud."""
    joined = df.join(dim_symbol_df.select("symbol", "symbol_key"), on="symbol", how="left")
    orphans = joined.filter(pl.col("symbol_key").is_null()).get_column("symbol").unique().to_list()
    if orphans:
        raise ValueError(
            f"fact_corporate_events FK violation -> symbols missing from dim_symbol: {sorted(orphans)}"
        )
    return joined


def _attach_date_key(df: pl.DataFrame, dim_date_df: pl.DataFrame) -> pl.DataFrame:
    """LEFT-join ``date_key`` từ dim_date; date trong-range mà thiếu ⇒ fail-loud."""
    joined = df.join(
        dim_date_df.select(pl.col("full_date").alias("event_date"), "date_key"),
        on="event_date",
        how="left",
    )
    orphan_dates = joined.filter(pl.col("date_key").is_null()).get_column("event_date").unique().to_list()
    if orphan_dates:
        missing = sorted(str(d) for d in orphan_dates)
        raise ValueError(
            f"fact_corporate_events FK violation -> event_dates missing from dim_date: {missing}"
        )
    return joined
