from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from uuid import uuid4

import polars as pl

from stock_lakehouse.utils.dates import format_date, now_utc, parse_date


RawFetcher = Callable[[str, str, str, str], object]

_COLUMN_ALIASES = {
    "date": "time",
    "datetime": "time",
    "trading_date": "time",
    "open_price": "open",
    "high_price": "high",
    "low_price": "low",
    "close_price": "close",
    "ticker": "symbol",
}


@dataclass(frozen=True)
class OhlcvExtractRequest:
    processing_date: date
    symbols: Sequence[str]
    source: str = "VCI"
    batch_id: str = ""

    @classmethod
    def daily(
        cls,
        processing_date: str | date,
        symbols: Sequence[str],
        source: str = "VCI",
        batch_id: str | None = None,
    ) -> "OhlcvExtractRequest":
        return cls(
            processing_date=parse_date(processing_date),
            symbols=tuple(symbols),
            source=source,
            batch_id=batch_id or uuid4().hex,
        )


def fetch_vnstock_ohlcv(symbol: str, start: str, end: str, source: str = "VCI") -> object:
    from vnstock import Quote

    return Quote(source=source, symbol=symbol).history(
        symbol=symbol,
        start=start,
        end=end,
        interval="1D",
    )


def extract_ohlcv(
    request: OhlcvExtractRequest,
    fetcher: RawFetcher = fetch_vnstock_ohlcv,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    day = format_date(request.processing_date)

    for symbol in request.symbols:
        raw = fetcher(symbol.upper(), day, day, request.source)
        frame = normalize_ohlcv_response(
            raw,
            symbol=symbol,
            source=request.source,
            batch_id=request.batch_id,
            processing_date=request.processing_date,
        )
        if frame.height:
            frames.append(frame)

    if not frames:
        return empty_ohlcv_frame()
    return pl.concat(frames, how="vertical_relaxed")


def normalize_ohlcv_response(
    raw: object,
    *,
    symbol: str,
    source: str,
    batch_id: str,
    processing_date: str | date,
) -> pl.DataFrame:
    df = _to_polars(raw)
    if df.is_empty():
        return empty_ohlcv_frame()

    df = _rename_known_columns(df)
    if "symbol" not in df.columns:
        df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

    required = {"symbol", "time", "open", "high", "low", "close", "volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"OHLCV response missing required columns: {sorted(missing)}")

    ingested_at = now_utc()
    return (
        df.select("symbol", "time", "open", "high", "low", "close", "volume")
        .with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("time").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
            pl.lit(source).alias("source"),
            pl.lit(batch_id).alias("batch_id"),
            pl.lit(ingested_at).alias("ingested_at"),
            pl.lit(parse_date(processing_date)).alias("processing_date"),
        )
    )


def empty_ohlcv_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "time": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "source": pl.Utf8,
            "batch_id": pl.Utf8,
            "ingested_at": pl.Datetime(time_zone="UTC"),
            "processing_date": pl.Date,
        }
    )


def _to_polars(raw: object) -> pl.DataFrame:
    if isinstance(raw, pl.DataFrame):
        return raw
    return pl.from_pandas(raw)  # type: ignore[arg-type]


def _rename_known_columns(df: pl.DataFrame) -> pl.DataFrame:
    normalized = {name: name.strip().lower() for name in df.columns}
    df = df.rename(normalized)
    aliases = {
        source: target
        for source, target in _COLUMN_ALIASES.items()
        if source in df.columns and target not in df.columns
    }
    return df.rename(aliases)

