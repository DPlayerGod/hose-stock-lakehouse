from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from stock_lakehouse.gold.dim_date import build_dim_date
from stock_lakehouse.gold.dim_symbol import build_dim_symbol
from stock_lakehouse.gold.fact_daily_market import (
    build_fact_daily_market,
    replace_daily_market,
)


def test_build_dim_date_marks_weekend_and_holiday() -> None:
    dim_date = build_dim_date("2026-01-01", "2026-01-04")

    assert dim_date.height == 4
    assert dim_date.filter(pl.col("full_date") == date(2026, 1, 1)).select("date_key").item() == 20260101
    assert dim_date.filter(pl.col("full_date") == date(2026, 1, 3)).select("is_weekend").item() is True
    assert dim_date.filter(pl.col("full_date") == date(2026, 1, 1)).select("is_day_off").item() is True


def test_build_dim_symbol_preserves_keys_and_marks_delisted() -> None:
    existing = pl.DataFrame(
        {
            "symbol_key": [1, 2],
            "symbol": ["FPT", "VNM"],
            "company_name": ["FPT Corp", "Vinamilk"],
            "sector_name": ["Tech", "Consumer"],
            "company_profile": [None, None],
            "listing_date": [date(2006, 12, 13), date(2006, 1, 19)],
            "exchange_code": ["HOSE", "HOSE"],
            "listed_status": ["LISTED", "LISTED"],
            "updated_at": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
        }
    ).with_columns(pl.col("updated_at").str.to_datetime(time_zone="UTC"))
    latest = pl.DataFrame(
        {
            "symbol": ["fpt", "msn"],
            "company_name": ["FPT Corp Updated", "Masan"],
            "sector_name": ["Tech", "Consumer"],
        }
    )

    dim_symbol = build_dim_symbol(latest, existing_dim=existing)

    assert dim_symbol.filter(pl.col("symbol") == "FPT").select("symbol_key").item() == 1
    assert dim_symbol.filter(pl.col("symbol") == "MSN").select("symbol_key").item() == 3
    assert dim_symbol.filter(pl.col("symbol") == "VNM").select("listed_status").item() == "DELISTED"


def test_build_fact_daily_market_joins_dimensions_and_calculates_metrics() -> None:
    silver = _silver_history()
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": ["FPT"]}))
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    fact = build_fact_daily_market(silver, dim_symbol, dim_date, processing_date="2026-01-20")

    assert fact.height == 1
    assert fact.select("symbol_key").item() == 1
    assert fact.select("date_key").item() == 20260120
    assert fact.select("price_change").item() == 1.0
    assert fact.select("sma20").item() is not None
    assert 0 <= fact.select("rsi14").item() <= 100


def test_ema20_and_macd_apply_null_warmup() -> None:
    silver = _silver_history()  # 21 phiên FPT
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": ["FPT"]}))
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    fact = build_fact_daily_market(silver, dim_symbol, dim_date).sort("trading_date")
    ema20 = fact.get_column("ema20").to_list()
    macd = fact.get_column("macd").to_list()

    # ema20: null 19 phiên đầu, có giá trị từ phiên thứ 20 (đồng bộ sma20)
    assert all(v is None for v in ema20[:19])
    assert ema20[19] is not None
    # macd cần 26 phiên (leg ema26) -> toàn bộ 21 phiên đầu vẫn null
    assert all(v is None for v in macd)


def test_replace_daily_market_is_idempotent_for_target_date_only() -> None:
    silver = _silver_history()
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": ["FPT"]}))
    dim_date = build_dim_date("2026-01-01", "2026-01-25")
    existing = build_fact_daily_market(silver, dim_symbol, dim_date)
    replacement = build_fact_daily_market(
        silver.with_columns(
            pl.when(pl.col("trading_date").cast(pl.Utf8) == "2026-01-20")
            .then(999.0)
            .otherwise(pl.col("close_price"))
            .alias("close_price")
        ),
        dim_symbol,
        dim_date,
        processing_date="2026-01-20",
    )

    first = replace_daily_market(existing, replacement, "2026-01-20")
    second = replace_daily_market(first, replacement, "2026-01-20")

    assert first.height == existing.height
    assert second.to_dicts() == first.to_dicts()
    assert second.filter(pl.col("trading_date") == date(2026, 1, 19)).select("close_price").item() == 19.0
    assert second.filter(pl.col("trading_date") == date(2026, 1, 20)).select("close_price").item() == 999.0


def test_replace_daily_market_ignores_rows_outside_target_date() -> None:
    fact = pl.DataFrame(
        {
            "symbol_key": [1, 1],
            "date_key": [20260119, 20260120],
            "trading_date": [date(2026, 1, 19), date(2026, 1, 20)],
            "open_price": [1.0, 1.0],
            "high_price": [1.0, 1.0],
            "low_price": [1.0, 1.0],
            "close_price": [19.0, 20.0],
            "volume": [1, 1],
            "price_change": [None, None],
            "pct_change": [None, None],
            "sma20": [1.0, 1.0],
            "ema20": [1.0, 1.0],
            "rsi14": [None, None],
            "macd": [0.0, 0.0],
            "avg_volume_20d": [1.0, 1.0],
            "updated_at": ["2026-01-20T00:00:00Z", "2026-01-20T00:00:00Z"],
        }
    ).with_columns(pl.col("updated_at").str.to_datetime(time_zone="UTC"))
    replacement = fact.with_columns(
        pl.when(pl.col("trading_date") == date(2026, 1, 20))
        .then(999.0)
        .otherwise(888.0)
        .alias("close_price")
    )

    result = replace_daily_market(fact, replacement, "2026-01-20")

    assert result.filter(pl.col("trading_date") == date(2026, 1, 19)).select("close_price").item() == 19.0
    assert result.filter(pl.col("trading_date") == date(2026, 1, 20)).select("close_price").item() == 999.0


def test_build_fact_daily_market_raises_on_symbol_missing_from_dim() -> None:
    silver = _silver_history()
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": ["VNM"]}))  # FPT not in dim
    dim_date = build_dim_date("2026-01-01", "2026-01-25")

    with pytest.raises(ValueError, match="missing from dim_symbol"):
        build_fact_daily_market(silver, dim_symbol, dim_date, processing_date="2026-01-20")


def test_build_fact_daily_market_raises_on_date_missing_from_dim() -> None:
    silver = _silver_history()
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": ["FPT"]}))
    dim_date = build_dim_date("2026-01-01", "2026-01-10")  # 2026-01-20 absent

    with pytest.raises(ValueError, match="missing from dim_date"):
        build_fact_daily_market(silver, dim_symbol, dim_date, processing_date="2026-01-20")


def _silver_history() -> pl.DataFrame:
    rows = []
    for day in range(1, 22):
        rows.append(
            {
                "symbol": "FPT",
                "trading_date": date(2026, 1, day),
                "open_price": float(day),
                "high_price": float(day + 1),
                "low_price": float(day - 1 if day > 1 else 1),
                "close_price": float(day),
                "volume": day * 100,
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
