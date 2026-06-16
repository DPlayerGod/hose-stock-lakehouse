from __future__ import annotations

import json
from typing import BinaryIO, cast

import polars as pl
import s3fs

from stock_lakehouse.config import MinioConfig


def write_quarantine(
    df: pl.DataFrame,
    errors: tuple[str, ...],
    *,
    domain: str,
    processing_date: str,
    batch_id: str,
    config: MinioConfig = MinioConfig(),
    bucket: str = "lakehouse",
) -> str:
    """Write a failed-validation batch and its errors to the quarantine prefix.

    Returns the base S3 URI where the files were written.
    """
    fs = _make_fs(config)
    base = f"s3://{bucket}/quarantine/{domain}/processing_date={processing_date}/batch_id={batch_id}"

    with fs.open(f"{base}/data.parquet", "wb") as f:
        df.write_parquet(cast(BinaryIO, f))

    with fs.open(f"{base}/errors.json", "w") as f:
        json.dump(
            {
                "domain": domain,
                "processing_date": processing_date,
                "batch_id": batch_id,
                "errors": list(errors),
            },
            f,
            indent=2,
        )

    return base


def _make_fs(config: MinioConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=config.access_key,
        secret=config.secret_key,
        client_kwargs={"endpoint_url": config.endpoint, "region_name": config.region},
    )
