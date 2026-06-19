from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

import polars as pl
import s3fs

from stock_lakehouse.config import MinioConfig
from stock_lakehouse.utils.dates import format_date


@dataclass(frozen=True)
class StagingPath:
    domain: str
    processing_date: str
    batch_id: str
    filename: str = "part-00000.parquet"


class StagingPathBuilder:
    def __init__(self, bucket: str = "lakehouse", root: str = "staging") -> None:
        self.bucket = bucket
        self.root = root.strip("/")

    def ohlcv(self, processing_date: str, batch_id: str) -> str:
        return self.build(
            StagingPath(
                domain="ohlcv",
                processing_date=format_date(processing_date),
                batch_id=batch_id,
            )
        )

    def index(self, processing_date: str, batch_id: str) -> str:
        return self.build(
            StagingPath(
                domain="index",
                processing_date=format_date(processing_date),
                batch_id=batch_id,
            )
        )

    def build(self, path: StagingPath) -> str:
        return (
            f"s3://{self.bucket}/{self.root}/{path.domain}/"
            f"processing_date={path.processing_date}/batch_id={path.batch_id}/{path.filename}"
        )


def write_staging_parquet(df: pl.DataFrame, uri: str, config: MinioConfig = MinioConfig()) -> str:
    if uri.startswith("s3://"):
        fs = _s3_filesystem(config)
        with fs.open(uri, "wb") as raw_file:
            file = cast(BinaryIO, raw_file)
            df.write_parquet(file)
        return uri

    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return str(path)


def read_staging_parquet(uri: str, config: MinioConfig = MinioConfig()) -> pl.DataFrame:
    if uri.startswith("s3://"):
        fs = _s3_filesystem(config)
        with fs.open(uri, "rb") as raw_file:
            file = cast(BinaryIO, raw_file)
            return pl.read_parquet(file)
    return pl.read_parquet(uri)


def _s3_filesystem(config: MinioConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=config.access_key,
        secret=config.secret_key,
        client_kwargs={"endpoint_url": config.endpoint, "region_name": config.region},
    )
