from __future__ import annotations

import polars as pl

from stock_lakehouse.ingestion.corporate_events import EVENT_CODE_LABELS
from stock_lakehouse.quality import validate_silver_corporate_events


SILVER_CORPORATE_EVENTS_COLUMNS = (
    "event_id",
    "symbol",
    "event_date",
    "event_code",
    "event_label",
    "title_vi",
    "value",
    "source",
    "ingested_at",
)


def build_silver_corporate_events(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Silver: làm sạch + dedup theo ``event_id`` + suy ``event_label`` từ ``event_code``.

    - ``title_vi``  ← ``event_title_vi`` (text tooltip).
    - ``value``     ← ``value_per_share`` (cổ tức tiền/cp; null nếu không phải DIV).
    - ``event_label`` = nhãn VN ngắn theo ``event_code`` (mã lạ giữ nguyên mã).
    Feed trả full lịch sử nên dedup theo ``event_id`` (giữ bản mới nhất theo ``ingested_at``).
    """
    silver = (
        bronze_df.rename({"event_title_vi": "title_vi", "value_per_share": "value"})
        .with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("event_code").cast(pl.Utf8).str.to_uppercase(),
            pl.col("event_date").cast(pl.Date, strict=False),
            pl.col("value").cast(pl.Float64, strict=False),
            pl.col("event_code")
            .replace_strict(EVENT_CODE_LABELS, default=pl.col("event_code"), return_dtype=pl.Utf8)
            .alias("event_label"),
        )
        .sort("event_id", "ingested_at")
        .unique(subset=["event_id"], keep="last", maintain_order=True)
        .select(SILVER_CORPORATE_EVENTS_COLUMNS)
        .sort("symbol", "event_date", "event_id")
    )
    validate_silver_corporate_events(silver).raise_for_errors()
    return silver
