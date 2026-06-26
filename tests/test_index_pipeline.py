from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from stock_lakehouse.gold.dim_date import build_dim_date
from stock_lakehouse.gold.fact_index_daily import (
    build_fact_index_daily,
    replace_index_daily,
)


def test_build_fact_index_daily_joins_dim_date_and_calculates_metrics() -> None:
    silver = _silver_index_history()
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    fact = build_fact_index_daily(silver, dim_date, processing_date="2026-01-20")

    assert fact.height == 1
    # Định danh bằng natural key index_code, KHÔNG có symbol_key
    assert fact.select("index_code").item() == "VNINDEX"
    assert "symbol_key" not in fact.columns
    assert fact.select("date_key").item() == 20260120
    assert fact.select("price_change").item() == 1.0
    assert fact.select("sma20").item() is not None
    assert 0 <= fact.select("rsi14").item() <= 100


def test_index_fact_supports_multiple_indices_independently() -> None:
    silver = pl.concat([_silver_index_history("VNINDEX"), _silver_index_history("VN30")])
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    fact = build_fact_index_daily(silver, dim_date, processing_date="2026-01-20")

    assert set(fact.get_column("index_code").to_list()) == {"VNINDEX", "VN30"}
    assert fact.height == 2


def test_index_ema20_and_macd_apply_null_warmup() -> None:
    silver = _silver_index_history()  # 21 phiên
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    fact = build_fact_index_daily(silver, dim_date).sort("trading_date")
    ema20 = fact.get_column("ema20").to_list()
    macd = fact.get_column("macd").to_list()

    assert all(v is None for v in ema20[:19])
    assert ema20[19] is not None
    assert all(v is None for v in macd)  # cần 26 phiên, history chỉ 21


def test_replace_index_daily_is_idempotent_for_target_date_only() -> None:
    silver = _silver_index_history()
    dim_date = build_dim_date("2026-01-01", "2026-01-25")
    existing = build_fact_index_daily(silver, dim_date)
    replacement = build_fact_index_daily(
        silver.with_columns(
            pl.when(pl.col("trading_date").cast(pl.Utf8) == "2026-01-20")
            .then(999.0)
            .otherwise(pl.col("close_price"))
            .alias("close_price")
        ),
        dim_date,
        processing_date="2026-01-20",
    )

    first = replace_index_daily(existing, replacement, "2026-01-20")
    second = replace_index_daily(first, replacement, "2026-01-20")

    assert first.height == existing.height
    assert second.to_dicts() == first.to_dicts()
    assert second.filter(pl.col("trading_date") == date(2026, 1, 19)).select("close_price").item() == 19.0
    assert second.filter(pl.col("trading_date") == date(2026, 1, 20)).select("close_price").item() == 999.0


def test_build_fact_index_daily_raises_on_date_missing_from_dim() -> None:
    silver = _silver_index_history()
    dim_date = build_dim_date("2026-01-01", "2026-01-10")  # 2026-01-20 absent

    with pytest.raises(ValueError, match="missing from dim_date"):
        build_fact_index_daily(silver, dim_date, processing_date="2026-01-20")


def _silver_index_history(index_code: str = "VNINDEX") -> pl.DataFrame:
    """Silver index rows — carry entity under generic ``symbol`` column (reuses OHLCV transforms)."""
    rows = []
    for day in range(1, 22):
        rows.append(
            {
                "symbol": index_code,
                "trading_date": date(2026, 1, day),
                "open_price": float(day),
                "high_price": float(day + 1),
                "low_price": float(day - 1 if day > 1 else 1),
                "close_price": float(day),
                "volume": day * 1000,
                "source": "VCI",
                "batch_id": "batch-1",
                "ingested_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )
    return pl.DataFrame(rows).with_columns(
        pl.col("ingested_at").str.to_datetime(time_zone="UTC"),
        pl.col("updated_at").str.to_datetime(time_zone="UTC"),
    )
