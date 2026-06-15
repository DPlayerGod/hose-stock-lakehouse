from __future__ import annotations

from typing import cast

import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table


def read_table(table: Table) -> pl.DataFrame:
    return cast(pl.DataFrame, pl.from_arrow(table.scan().to_arrow()))


def try_read_table(catalog: Catalog, identifier: str) -> pl.DataFrame | None:
    try:
        return read_table(catalog.load_table(identifier))
    except Exception:
        return None
