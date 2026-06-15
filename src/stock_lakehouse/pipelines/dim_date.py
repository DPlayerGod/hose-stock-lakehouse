from __future__ import annotations

import argparse
from dataclasses import dataclass

from stock_lakehouse.clickhouse.loader import sync_dim_date_to_clickhouse
from stock_lakehouse.config import PipelineConfig
from stock_lakehouse.gold.dim_date import build_dim_date
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.tables import DIM_DATE_SCHEMA
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe


DEFAULT_START_DATE = "2020-01-01"
DEFAULT_END_DATE = "2030-12-31"


@dataclass(frozen=True)
class DimDatePipelineResult:
    start_date: str
    end_date: str
    rows: int
    iceberg_table: str
    synced_clickhouse: bool


def run_dim_date_pipeline(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    sync_clickhouse: bool = True,
    config: PipelineConfig = PipelineConfig(),
) -> DimDatePipelineResult:
    dim_date = build_dim_date(start_date, end_date)
    catalog = load_lakehouse_catalog(config.iceberg)
    identifier = f"{config.iceberg.namespace}.dim_date"
    table = ensure_table(catalog, identifier, DIM_DATE_SCHEMA)
    write_dataframe(table, dim_date, mode="overwrite")

    if sync_clickhouse:
        sync_dim_date_to_clickhouse(dim_date, config.clickhouse)

    return DimDatePipelineResult(
        start_date=start_date,
        end_date=end_date,
        rows=dim_date.height,
        iceberg_table=identifier,
        synced_clickhouse=sync_clickhouse,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Gold dim_date once for a fixed date range.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--no-clickhouse", action="store_true")
    args = parser.parse_args()
    result = run_dim_date_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        sync_clickhouse=not args.no_clickhouse,
    )
    print(result)


if __name__ == "__main__":
    main()
