from __future__ import annotations

from typing import Literal

import pyarrow as pa
import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.expressions import BooleanExpression


WriteMode = Literal["append", "overwrite"]


def ensure_table(
    catalog: Catalog,
    identifier: str,
    schema: Schema,
    partition_spec: PartitionSpec | None = None,
) -> Table:
    try:
        return catalog.load_table(identifier)
    except Exception:
        namespace = ".".join(identifier.split(".")[:-1])
        if namespace:
            try:
                catalog.create_namespace(namespace)
            except Exception:
                pass
        if partition_spec is None:
            return catalog.create_table(identifier, schema=schema)
        return catalog.create_table(identifier, schema=schema, partition_spec=partition_spec)


def write_dataframe(
    table: Table,
    df: pl.DataFrame,
    mode: WriteMode = "append",
    overwrite_filter: str | BooleanExpression | None = None,
) -> None:
    arrow_table = _align_arrow_schema(df.to_arrow(), table)
    if mode == "append":
        table.append(arrow_table)
        return
    if mode == "overwrite":
        if overwrite_filter is not None:
            table.overwrite(arrow_table, overwrite_filter=overwrite_filter)
        else:
            table.overwrite(arrow_table)
        return
    raise ValueError(f"Unsupported write mode: {mode}")


def _align_arrow_schema(arrow_table: pa.Table, iceberg_table: Table) -> pa.Table:
    """Cast the Arrow table to match the Iceberg table's expected Arrow schema.

    Polars' ``to_arrow()`` produces all-nullable fields, but Iceberg schemas
    may declare certain fields as *required* (non-nullable).  PyIceberg
    validates this and rejects mismatches.  We resolve the issue by casting
    the Arrow table to the schema that PyIceberg derives from the Iceberg
    table metadata.

    Timestamp type mismatches (``timestamp[us]`` to ``timestamp[us, tz=UTC]``)
    are also handled by this cast.
    """
    target_schema = schema_to_pyarrow(iceberg_table.schema())
    if arrow_table.schema.equals(target_schema):
        return arrow_table
    return arrow_table.cast(target_schema)
