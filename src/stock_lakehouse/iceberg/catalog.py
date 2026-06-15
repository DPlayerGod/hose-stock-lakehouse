from __future__ import annotations

from pyiceberg.catalog import Catalog, load_catalog

from stock_lakehouse.config import IcebergConfig


def load_lakehouse_catalog(config: IcebergConfig = IcebergConfig()) -> Catalog:
    return load_catalog(
        config.catalog_name,
        **{
            "type": "rest",
            "uri": config.uri,
            "warehouse": config.warehouse,
            "s3.endpoint": config.s3_endpoint,
            "s3.access-key-id": config.access_key,
            "s3.secret-access-key": config.secret_key,
            "s3.region": config.region,
            "s3.path-style-access": "true",
        },
    )
