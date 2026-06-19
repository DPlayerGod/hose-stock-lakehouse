"""Extract sự kiện doanh nghiệp (corporate events) từ VNStock/VCI.

Khác OHLCV: feed VCI (``Company(symbol).events()``) trả **toàn bộ lịch sử** sự kiện
của một mã mỗi lần gọi (cổ tức, phát hành, giao dịch nội bộ, ĐHĐCĐ, niêm yết thêm…),
không theo ngày. Vì vậy không có "ngày D" — idempotency về sau làm bằng dedup theo
natural key ``event_id`` + overwrite cả bảng.

Tầng này chỉ *trích + chuẩn hoá thô* (ép kiểu, rename về tên cột chuẩn, gắn lineage);
mọi logic nghiệp vụ (suy ``event_label``, dedup, derive ``event_date`` ở Silver) để
các tầng sau xử lý. Fetcher tách rời (injectable) để test không cần gọi mạng.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from uuid import uuid4

import polars as pl

from stock_lakehouse.config import SYMBOLS
from stock_lakehouse.utils.dates import now_utc, parse_date


# Fetcher: nhận symbol → trả về bảng sự kiện thô (pandas/polars) của mã đó.
EventsFetcher = Callable[[str], object]

# Tên cột nguồn (VCI) → tên cột chuẩn của project.
_COLUMN_ALIASES = {
    "id": "event_id",
    "ticker": "symbol",
    # ``event_date`` lấy thẳng từ display_date1 (ngày hiển thị trên chart).
    "display_date1": "event_date",
}

# Cột optional có thể thiếu ở feed thật → thêm null trước khi select.
_OPTIONAL_STR = ("event_title_vi",)
_OPTIONAL_FLOAT = ("value_per_share",)

# Nhãn loại sự kiện (ngắn, tiếng Việt) suy từ ``event_code`` — dùng cho marker/bảng dashboard.
EVENT_CODE_LABELS: dict[str, str] = {
    "DIV": "Cổ tức tiền mặt",
    "ISS": "Phát hành cổ phiếu",
    "DDIND": "GD nội bộ (cá nhân)",
    "DDINS": "GD nội bộ (tổ chức)",
    "DDRP": "GD nội bộ (liên quan)",
    "AGME": "ĐHĐCĐ thường niên",
    "EGME": "ĐHĐCĐ bất thường",
    "AIS": "Niêm yết thêm",
    "OTHE": "Sự kiện khác",
}


def event_label_for(event_code: str | None) -> str:
    """Map ``event_code`` → nhãn tiếng Việt ngắn; mã lạ trả về chính nó (không null)."""
    if event_code is None:
        return "Sự kiện khác"
    return EVENT_CODE_LABELS.get(event_code, event_code)


def fetch_vnstock_events(symbol: str) -> object:
    """Lấy toàn bộ lịch sử sự kiện của một mã qua VNStock VCI."""
    from vnstock.api.company import Company

    return Company(symbol=symbol, source="VCI").events()


def extract_corporate_events(
    symbols: Sequence[str] = SYMBOLS,
    *,
    source: str = "VCI",
    batch_id: str | None = None,
    processing_date: str | date | None = None,
    fetcher: EventsFetcher = fetch_vnstock_events,
) -> pl.DataFrame:
    """Trích + chuẩn hoá sự kiện cho danh sách mã thành một DataFrame thô.

    Mỗi mã gọi ``fetcher`` một lần; kết quả được chuẩn hoá rồi nối lại. Luôn trả
    về DataFrame có đủ cột chuẩn + lineage (``source``/``batch_id``/``ingested_at``/
    ``processing_date``), kể cả khi rỗng.
    """
    batch_id = batch_id or uuid4().hex
    proc_date = parse_date(processing_date) if processing_date is not None else now_utc().date()

    frames: list[pl.DataFrame] = []
    for symbol in symbols:
        raw = fetcher(symbol.upper())
        frame = normalize_events_response(
            raw, symbol=symbol, source=source, batch_id=batch_id, processing_date=proc_date
        )
        if frame.height:
            frames.append(frame)

    if not frames:
        return empty_events_frame()
    return pl.concat(frames, how="vertical_relaxed")


def normalize_events_response(
    raw: object,
    *,
    symbol: str,
    source: str,
    batch_id: str,
    processing_date: date,
) -> pl.DataFrame:
    """Chuẩn hoá một bảng sự kiện thô về schema cột chuẩn (chưa dedup, chưa suy label)."""
    df = _to_polars(raw)
    if df.is_empty():
        return empty_events_frame()

    df = _rename_known_columns(df)
    if "symbol" not in df.columns:
        df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

    required = {"event_id", "event_code", "event_date"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Corporate-events response missing required columns: {sorted(missing)}")

    # Thêm cột optional còn thiếu dưới dạng null để select phẳng không vỡ.
    for column in _OPTIONAL_STR:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(column))
    for column in _OPTIONAL_FLOAT:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))

    return df.select(
        pl.col("event_id").cast(pl.Utf8),
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("event_code").cast(pl.Utf8).str.to_uppercase(),
        pl.col("event_title_vi").cast(pl.Utf8, strict=False),
        pl.col("value_per_share").cast(pl.Float64, strict=False),
        _to_date("event_date"),
        pl.lit(source).alias("source"),
        pl.lit(batch_id).alias("batch_id"),
        pl.lit(now_utc()).alias("ingested_at"),
        pl.lit(processing_date).alias("processing_date"),
    )


def empty_events_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_id": pl.Utf8,
            "symbol": pl.Utf8,
            "event_code": pl.Utf8,
            "event_title_vi": pl.Utf8,
            "value_per_share": pl.Float64,
            "event_date": pl.Date,
            "source": pl.Utf8,
            "batch_id": pl.Utf8,
            "ingested_at": pl.Datetime(time_zone="UTC"),
            "processing_date": pl.Date,
        }
    )


def _to_date(column: str) -> pl.Expr:
    """Ép cột ngày về ``pl.Date``, chịu được cả 'YYYY-MM-DD' lẫn 'YYYY-MM-DDTHH:MM:SS'."""
    return (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.slice(0, 10)
        .str.to_date("%Y-%m-%d", strict=False)
        .alias(column)
    )


def _rename_known_columns(df: pl.DataFrame) -> pl.DataFrame:
    normalized = {name: name.strip().lower() for name in df.columns}
    df = df.rename(normalized)
    aliases = {
        src: target
        for src, target in _COLUMN_ALIASES.items()
        if src in df.columns and target not in df.columns
    }
    return df.rename(aliases)


def _to_polars(raw: object) -> pl.DataFrame:
    if isinstance(raw, pl.DataFrame):
        return raw
    return pl.from_pandas(raw)  # type: ignore[arg-type]
