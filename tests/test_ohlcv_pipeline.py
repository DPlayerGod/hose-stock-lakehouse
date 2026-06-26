from __future__ import annotations

import polars as pl
from pathlib import Path

import pytest

from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
from stock_lakehouse.staging.writer import read_staging_parquet, write_staging_parquet


def test_extract_normalizes_injected_fetcher() -> None:
    def fetcher(symbol: str, start: str, end: str, source: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "time": [start],
                "open": [10],
                "high": [12],
                "low": [9],
                "close": [11],
                "volume": [1000],
            }
        )

    request = OhlcvExtractRequest.daily("2026-06-14", ["fpt"], batch_id="batch-1")
    df = extract_ohlcv(request, fetcher=fetcher)

    assert df.select("symbol").item() == "FPT"
    assert df.select("source").item() == "VCI"
    assert df.select("batch_id").item() == "batch-1"


def test_staging_roundtrip_local_parquet() -> None:
    df = pl.DataFrame({"symbol": ["FPT"], "volume": [100]})
    path = Path("data/test_outputs/staging/part.parquet")

    write_staging_parquet(df, str(path))
    actual = read_staging_parquet(str(path))

    assert actual.to_dicts() == df.to_dicts()


def test_bronze_and_silver_ohlcv_transforms() -> None:
    staging_df = pl.DataFrame(
        {
            "symbol": ["fpt", "FPT"],
            "time": ["2026-06-14", "2026-06-14"],
            "open": [10, 11],
            "high": [12, 13],
            "low": [9, 10],
            "close": [11, 12],
            "volume": [1000, 1100],
            "source": ["VCI", "VCI"],
            "batch_id": ["batch-1", "batch-1"],
            "ingested_at": ["2026-06-14T00:00:00Z", "2026-06-14T00:01:00Z"],
            "processing_date": ["2026-06-14", "2026-06-14"],
        }
    )

    bronze = build_bronze_ohlcv(staging_df)
    silver = build_silver_ohlcv(bronze, processing_date="2026-06-14")

    assert silver.height == 1
    assert silver.select("symbol").item() == "FPT"
    assert silver.select("close_price").item() == 12.0


def test_silver_ohlcv_rejects_non_positive_prices() -> None:
    staging_df = pl.DataFrame(
        {
            "symbol": ["FPT"],
            "time": ["2026-06-14"],
            "open": [10],
            "high": [12],
            "low": [-1],  # negative low slips past high/low relative checks
            "close": [11],
            "volume": [1000],
            "source": ["VCI"],
            "batch_id": ["batch-1"],
            "ingested_at": ["2026-06-14T00:00:00Z"],
            "processing_date": ["2026-06-14"],
        }
    )

    bronze = build_bronze_ohlcv(staging_df)
    with pytest.raises(ValueError, match="must be > 0"):
        build_silver_ohlcv(bronze, processing_date="2026-06-14")
