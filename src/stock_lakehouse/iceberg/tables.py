from __future__ import annotations
# pyright: reportCallIssue=false

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


BRONZE_OHLCV_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=2, name="time", field_type=DateType(), required=True),
    NestedField(field_id=3, name="open", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="high", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="low", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="close", field_type=DoubleType(), required=False),
    NestedField(field_id=7, name="volume", field_type=LongType(), required=False),
    NestedField(field_id=8, name="source", field_type=StringType(), required=True),
    NestedField(field_id=9, name="batch_id", field_type=StringType(), required=True),
    NestedField(field_id=10, name="ingested_at", field_type=TimestamptzType(), required=True),
    NestedField(field_id=11, name="processing_date", field_type=DateType(), required=True),
)

SILVER_OHLCV_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=2, name="trading_date", field_type=DateType(), required=True),
    NestedField(field_id=3, name="open_price", field_type=DoubleType(), required=True),
    NestedField(field_id=4, name="high_price", field_type=DoubleType(), required=True),
    NestedField(field_id=5, name="low_price", field_type=DoubleType(), required=True),
    NestedField(field_id=6, name="close_price", field_type=DoubleType(), required=True),
    NestedField(field_id=7, name="volume", field_type=LongType(), required=True),
    NestedField(field_id=8, name="source", field_type=StringType(), required=True),
    NestedField(field_id=9, name="batch_id", field_type=StringType(), required=True),
    NestedField(field_id=10, name="ingested_at", field_type=TimestamptzType(), required=True),
    NestedField(field_id=11, name="updated_at", field_type=TimestamptzType(), required=True),
)

BRONZE_SYMBOLS_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=2, name="company_name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="sector_name", field_type=StringType(), required=False),
    NestedField(field_id=4, name="company_profile", field_type=StringType(), required=False),
    NestedField(field_id=5, name="listing_date", field_type=DateType(), required=False),
    NestedField(field_id=6, name="exchange_code", field_type=StringType(), required=True),
    NestedField(field_id=7, name="listed_status", field_type=StringType(), required=True),
    NestedField(field_id=8, name="source", field_type=StringType(), required=True),
    NestedField(field_id=9, name="batch_id", field_type=StringType(), required=True),
    NestedField(field_id=10, name="ingested_at", field_type=TimestamptzType(), required=True),
)

SILVER_SYMBOLS_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=2, name="company_name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="sector_name", field_type=StringType(), required=False),
    NestedField(field_id=4, name="company_profile", field_type=StringType(), required=False),
    NestedField(field_id=5, name="listing_date", field_type=DateType(), required=False),
    NestedField(field_id=6, name="exchange_code", field_type=StringType(), required=True),
    NestedField(field_id=7, name="listed_status", field_type=StringType(), required=True),
    NestedField(field_id=8, name="source", field_type=StringType(), required=True),
    NestedField(field_id=9, name="batch_id", field_type=StringType(), required=True),
    NestedField(field_id=10, name="ingested_at", field_type=TimestamptzType(), required=True),
    NestedField(field_id=11, name="updated_at", field_type=TimestamptzType(), required=True),
)

DIM_DATE_SCHEMA = Schema(
    NestedField(field_id=1, name="date_key", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="full_date", field_type=DateType(), required=True),
    NestedField(field_id=3, name="day", field_type=IntegerType(), required=True),
    NestedField(field_id=4, name="cal_week", field_type=IntegerType(), required=True),
    NestedField(field_id=5, name="cal_month", field_type=IntegerType(), required=True),
    NestedField(field_id=6, name="cal_quarter", field_type=IntegerType(), required=True),
    NestedField(field_id=7, name="cal_year", field_type=IntegerType(), required=True),
    NestedField(field_id=8, name="is_weekend", field_type=BooleanType(), required=True),
    NestedField(field_id=9, name="event_name", field_type=StringType(), required=False),
    NestedField(field_id=10, name="event_type", field_type=StringType(), required=False),
    NestedField(field_id=11, name="is_day_off", field_type=BooleanType(), required=True),
)

DIM_SYMBOL_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol_key", field_type=LongType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="company_name", field_type=StringType(), required=False),
    NestedField(field_id=4, name="sector_name", field_type=StringType(), required=False),
    NestedField(field_id=5, name="company_profile", field_type=StringType(), required=False),
    NestedField(field_id=6, name="listing_date", field_type=DateType(), required=False),
    NestedField(field_id=7, name="exchange_code", field_type=StringType(), required=True),
    NestedField(field_id=8, name="listed_status", field_type=StringType(), required=True),
    NestedField(field_id=9, name="updated_at", field_type=TimestamptzType(), required=True),
)

FACT_HOSE_DAILY_MARKET_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol_key", field_type=LongType(), required=True),
    NestedField(field_id=2, name="date_key", field_type=IntegerType(), required=True),
    NestedField(field_id=3, name="trading_date", field_type=DateType(), required=True),
    NestedField(field_id=4, name="open_price", field_type=DoubleType(), required=True),
    NestedField(field_id=5, name="high_price", field_type=DoubleType(), required=True),
    NestedField(field_id=6, name="low_price", field_type=DoubleType(), required=True),
    NestedField(field_id=7, name="close_price", field_type=DoubleType(), required=True),
    NestedField(field_id=8, name="volume", field_type=LongType(), required=True),
    NestedField(field_id=9, name="price_change", field_type=DoubleType(), required=False),
    NestedField(field_id=10, name="pct_change", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="sma20", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="ema20", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="rsi14", field_type=DoubleType(), required=False),
    NestedField(field_id=14, name="macd", field_type=DoubleType(), required=False),
    NestedField(field_id=15, name="avg_volume_20d", field_type=DoubleType(), required=False),
    NestedField(field_id=16, name="updated_at", field_type=TimestamptzType(), required=True),
)

BRONZE_OHLCV_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="time_month")
)
SILVER_OHLCV_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="trading_date_month")
)
FACT_HOSE_DAILY_MARKET_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=MonthTransform(), name="trading_date_month")
)
