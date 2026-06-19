from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from stock_lakehouse.clickhouse.client import get_clickhouse_client
from stock_lakehouse.config import ClickHouseConfig
from stock_lakehouse.utils.dates import format_date


@dataclass(frozen=True)
class GoldFrames:
    dim_date: pl.DataFrame
    dim_symbol: pl.DataFrame
    fact_daily_market: pl.DataFrame


def ensure_gold_schema(client: Any, database: str = "lakehouse") -> None:
    client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.command(
        """
        CREATE TABLE IF NOT EXISTS dim_date
        (
            date_key UInt32,
            full_date Date,
            day UInt8,
            cal_week UInt8,
            cal_month UInt8,
            cal_quarter UInt8,
            cal_year UInt16,
            is_weekend Bool,
            event_name Nullable(String),
            event_type Nullable(String),
            is_day_off Bool
        )
        ENGINE = MergeTree
        ORDER BY date_key
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS dim_symbol
        (
            symbol_key UInt64,
            symbol String,
            company_name Nullable(String),
            sector_name Nullable(String),
            company_profile Nullable(String),
            listing_date Nullable(Date),
            exchange_code String,
            listed_status String,
            updated_at DateTime64(6, 'UTC')
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY symbol_key
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS fact_hose_daily_market
        (
            symbol_key UInt64,
            date_key UInt32,
            trading_date Date,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            volume UInt64,
            price_change Nullable(Float64),
            pct_change Nullable(Float64),
            sma20 Nullable(Float64),
            ema20 Nullable(Float64),
            rsi14 Nullable(Float64),
            macd Nullable(Float64),
            avg_volume_20d Nullable(Float64),
            updated_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(trading_date)
        ORDER BY (trading_date, symbol_key)
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS fact_hose_index_daily
        (
            index_code String,
            date_key UInt32,
            trading_date Date,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            volume UInt64,
            price_change Nullable(Float64),
            pct_change Nullable(Float64),
            sma20 Nullable(Float64),
            ema20 Nullable(Float64),
            rsi14 Nullable(Float64),
            macd Nullable(Float64),
            avg_volume_20d Nullable(Float64),
            updated_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(trading_date)
        ORDER BY (trading_date, index_code)
        """
    )


def sync_gold_to_clickhouse(
    frames: GoldFrames,
    processing_date: str | date | None = None,
    config: ClickHouseConfig = ClickHouseConfig(),
) -> None:
    client = get_clickhouse_client(config)
    ensure_gold_schema(client, config.database)
    _replace_dimension(client, "dim_date", frames.dim_date)
    _replace_dimension(client, "dim_symbol", frames.dim_symbol)

    fact = frames.fact_daily_market
    if processing_date is not None:
        target_date = format_date(processing_date)
        fact = fact.filter(pl.col("trading_date").cast(pl.Utf8) == target_date)
        client.command(f"ALTER TABLE fact_hose_daily_market DELETE WHERE trading_date = toDate('{target_date}')")
    else:
        client.command("TRUNCATE TABLE fact_hose_daily_market")

    _insert_frame(client, "fact_hose_daily_market", fact)


def sync_dim_date_to_clickhouse(
    dim_date: pl.DataFrame,
    config: ClickHouseConfig = ClickHouseConfig(),
) -> None:
    client = get_clickhouse_client(config)
    ensure_gold_schema(client, config.database)
    _replace_dimension(client, "dim_date", dim_date)


def sync_dim_symbol_to_clickhouse(
    dim_symbol: pl.DataFrame,
    config: ClickHouseConfig = ClickHouseConfig(),
) -> None:
    """Sync dim_symbol to ClickHouse (truncate + insert)."""
    client = get_clickhouse_client(config)
    ensure_gold_schema(client, config.database)
    _replace_dimension(client, "dim_symbol", dim_symbol)


def sync_fact_to_clickhouse(
    fact_df: pl.DataFrame,
    processing_date: str | date | None = None,
    config: ClickHouseConfig = ClickHouseConfig(),
) -> None:
    """Sync fact_hose_daily_market to ClickHouse (idempotent per processing_date)."""
    client = get_clickhouse_client(config)
    ensure_gold_schema(client, config.database)
    fact = fact_df
    if processing_date is not None:
        target_date = format_date(processing_date)
        fact = fact.filter(pl.col("trading_date").cast(pl.Utf8) == target_date)
        client.command(f"ALTER TABLE fact_hose_daily_market DELETE WHERE trading_date = toDate('{target_date}')")
    else:
        client.command("TRUNCATE TABLE fact_hose_daily_market")
    _insert_frame(client, "fact_hose_daily_market", fact)


def sync_index_fact_to_clickhouse(
    fact_df: pl.DataFrame,
    processing_date: str | date | None = None,
    config: ClickHouseConfig = ClickHouseConfig(),
) -> None:
    """Sync fact_hose_index_daily to ClickHouse (idempotent per processing_date)."""
    client = get_clickhouse_client(config)
    ensure_gold_schema(client, config.database)
    fact = fact_df
    if processing_date is not None:
        target_date = format_date(processing_date)
        fact = fact.filter(pl.col("trading_date").cast(pl.Utf8) == target_date)
        client.command(f"ALTER TABLE fact_hose_index_daily DELETE WHERE trading_date = toDate('{target_date}')")
    else:
        client.command("TRUNCATE TABLE fact_hose_index_daily")
    _insert_frame(client, "fact_hose_index_daily", fact)


def _replace_dimension(client: Any, table_name: str, df: pl.DataFrame) -> None:
    client.command(f"TRUNCATE TABLE {table_name}")
    _insert_frame(client, table_name, df)


def _insert_frame(client: Any, table_name: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    clean = df.with_columns(
        [pl.col(column).fill_nan(None) for column, dtype in df.schema.items() if dtype.is_float()]
    )
    client.insert(table_name, clean.rows(), column_names=clean.columns)
