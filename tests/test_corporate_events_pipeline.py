from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from stock_lakehouse.bronze.corporate_events import build_bronze_corporate_events
from stock_lakehouse.gold.fact_corporate_events import build_fact_corporate_events
from stock_lakehouse.ingestion.corporate_events import (
    event_label_for,
    extract_corporate_events,
    normalize_events_response,
)
from stock_lakehouse.silver.corporate_events import build_silver_corporate_events


CSV_SAMPLE = Path("data/feasibility/expansion_events_sample.csv")


# ─────────────────────────────── Ingestion ──────────────────────────────────


def test_event_label_maps_code_and_falls_back_to_code() -> None:
    assert event_label_for("DIV") == "Cổ tức tiền mặt"
    assert event_label_for("AGME") == "ĐHĐCĐ thường niên"
    assert event_label_for("ZZZ") == "ZZZ"  # mã lạ giữ nguyên (không null)
    assert event_label_for(None) == "Sự kiện khác"


def test_normalize_renames_aliases_and_parses_dates() -> None:
    raw = pl.DataFrame(
        {
            "id": ["abc"],
            "ticker": ["fpt"],
            "event_code": ["div"],
            "event_title_vi": ["Trả cổ tức"],
            "value_per_share": ["1000.0"],
            "display_date1": ["2026-06-15T00:00:00"],
        }
    )
    out = normalize_events_response(
        raw, symbol="FPT", source="VCI", batch_id="b1", processing_date=date(2026, 6, 19)
    )

    assert out.select("event_id").item() == "abc"
    assert out.select("symbol").item() == "FPT"
    assert out.select("event_code").item() == "DIV"
    assert out.select("event_date").item() == date(2026, 6, 15)  # display_date1 → event_date
    assert out.select("value_per_share").item() == 1000.0
    assert out.select("source").item() == "VCI"


def test_normalize_raises_on_missing_required_columns() -> None:
    raw = pl.DataFrame({"id": ["x"], "event_code": ["DIV"]})  # thiếu display_date1/event_date
    with pytest.raises(ValueError, match="missing required columns"):
        normalize_events_response(raw, symbol="FPT", source="VCI", batch_id="b", processing_date=date(2026, 6, 19))


def test_extract_uses_injected_fetcher_per_symbol() -> None:
    def fetcher(symbol: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [f"{symbol}-1"],
                "ticker": [symbol],
                "event_code": ["AGME"],
                "event_title_vi": ["ĐHĐCĐ"],
                "value_per_share": [None],
                "display_date1": ["2026-04-20"],
            }
        )

    df = extract_corporate_events(["FPT", "VCB"], batch_id="b", processing_date="2026-06-19", fetcher=fetcher)
    assert set(df.get_column("symbol").to_list()) == {"FPT", "VCB"}
    assert df.height == 2


# ───────────────────────────── Bronze / Silver ──────────────────────────────


def _staging_from_csv() -> pl.DataFrame:
    """Trích toàn bộ sample CSV qua ingestion (CSV-backed fetcher)."""
    raw = pl.read_csv(CSV_SAMPLE, infer_schema_length=0)

    def fetcher(symbol: str) -> pl.DataFrame:
        return raw.filter(pl.col("ticker") == symbol)

    symbols = raw.get_column("ticker").unique().to_list()
    return extract_corporate_events(symbols, batch_id="b", processing_date="2026-06-19", fetcher=fetcher)


def test_bronze_casts_and_keeps_lineage() -> None:
    bronze = build_bronze_corporate_events(_staging_from_csv())
    assert bronze.schema["event_date"] == pl.Date
    assert bronze.schema["value_per_share"] == pl.Float64
    assert bronze.height == 250
    assert set(["source", "batch_id", "ingested_at", "processing_date"]).issubset(bronze.columns)


def test_silver_dedups_and_derives_label() -> None:
    silver = build_silver_corporate_events(build_bronze_corporate_events(_staging_from_csv()))

    # event_id duy nhất
    assert silver.get_column("event_id").n_unique() == silver.height
    # đổi tên cột theo schema lean
    assert {"title_vi", "value", "event_label"}.issubset(silver.columns)
    assert "event_title_vi" not in silver.columns
    # event_label suy từ event_code
    div = silver.filter(pl.col("event_code") == "DIV")
    assert div.get_column("event_label").unique().to_list() == ["Cổ tức tiền mặt"]


def test_silver_dedups_duplicate_event_ids() -> None:
    bronze = build_bronze_corporate_events(_staging_from_csv())
    doubled = pl.concat([bronze, bronze])  # mô phỏng feed trả trùng
    silver = build_silver_corporate_events(doubled)
    assert silver.get_column("event_id").n_unique() == silver.height


# ─────────────────────────────────── Gold ───────────────────────────────────


def _dim_symbol() -> pl.DataFrame:
    return pl.DataFrame(
        {"symbol": ["FPT", "VCB", "HPG", "VNM", "MWG"], "symbol_key": [1, 2, 3, 4, 5]}
    ).with_columns(pl.col("symbol_key").cast(pl.Int64))


def _silver_events(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        pl.col("event_date").cast(pl.Date),
        pl.col("value").cast(pl.Float64),
        pl.col("ingested_at").str.to_datetime(time_zone="UTC"),
    )


def _row(event_id: str, symbol: str, d: date, code: str = "DIV", value: float | None = 1000.0) -> dict:
    return {
        "event_id": event_id,
        "symbol": symbol,
        "event_date": d,
        "event_code": code,
        "event_label": "Cổ tức tiền mặt",
        "title_vi": "Trả cổ tức",
        "value": value,
        "source": "VCI",
        "ingested_at": "2026-01-01T00:00:00Z",
    }


def test_gold_attaches_keys_and_drops_out_of_range() -> None:
    from stock_lakehouse.gold.dim_date import build_dim_date

    silver = _silver_events(
        [
            _row("e1", "FPT", date(2026, 6, 15)),  # trong range
            _row("e2", "VCB", date(2018, 3, 1)),   # ngoài range (pre-2020) → bị bỏ
        ]
    )
    dim_date = build_dim_date("2020-01-01", "2030-12-31")

    fact = build_fact_corporate_events(silver, _dim_symbol(), dim_date)

    assert fact.height == 1  # e2 bị lọc
    assert fact.get_column("event_id").to_list() == ["e1"]
    assert fact.select("symbol_key").item() == 1
    assert fact.select("date_key").item() == 20260615
    assert "symbol" in fact.columns and "value" in fact.columns


def test_gold_fails_loud_on_symbol_missing_from_dim() -> None:
    from stock_lakehouse.gold.dim_date import build_dim_date

    silver = _silver_events([_row("e1", "XXX", date(2026, 6, 15))])
    dim_date = build_dim_date("2026-01-01", "2026-12-31")

    with pytest.raises(ValueError, match="missing from dim_symbol"):
        build_fact_corporate_events(silver, _dim_symbol(), dim_date)


def test_gold_fails_loud_on_in_range_date_missing_from_dim() -> None:
    # dim_date có "lỗ": 2026-01-01 và 2026-01-03, thiếu 2026-01-02 (vẫn nằm trong [min,max]).
    dim_date_gap = pl.DataFrame(
        {"full_date": [date(2026, 1, 1), date(2026, 1, 3)], "date_key": [20260101, 20260103]}
    ).with_columns(pl.col("date_key").cast(pl.Int32))
    silver = _silver_events([_row("e1", "FPT", date(2026, 1, 2))])

    with pytest.raises(ValueError, match="missing from dim_date"):
        build_fact_corporate_events(silver, _dim_symbol(), dim_date_gap)


def test_gold_full_sample_end_to_end() -> None:
    from stock_lakehouse.gold.dim_date import build_dim_date

    silver = build_silver_corporate_events(build_bronze_corporate_events(_staging_from_csv()))
    dim_date = build_dim_date("2020-01-01", "2030-12-31")

    fact = build_fact_corporate_events(silver, _dim_symbol(), dim_date)

    # mọi event trong fact đều thuộc range dim_date
    assert fact.get_column("event_date").min() >= date(2020, 1, 1)
    # không mất dòng nào ngoài việc lọc out-of-range
    dropped = silver.height - fact.height
    assert dropped > 0
    assert fact.get_column("event_id").n_unique() == fact.height
