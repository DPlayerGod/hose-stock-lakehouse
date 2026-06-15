"""DAG: dim_date — one-time calendar generation.

Flow:
    generate_calendar → validate_dim_date → write_dim_date_iceberg → sync_dim_date_to_clickhouse

This DAG generates dim_date exactly ONCE from 2020-01-01 to 2030-12-31.
It is configured with schedule_interval=None (manual trigger only).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

START_DATE = "2020-01-01"
END_DATE = "2030-12-31"

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _get_config():
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def task_generate_calendar(**ctx):
    """Generate the full date dimension from 2020 to 2030."""
    from stock_lakehouse.gold.dim_date import build_dim_date
    df = build_dim_date(START_DATE, END_DATE)
    ctx["ti"].xcom_push(key="row_count", value=df.height)


def task_validate_dim_date(**ctx):
    """Validate the generated dim_date DataFrame."""
    from stock_lakehouse.gold.dim_date import build_dim_date
    from stock_lakehouse.quality.gold import validate_dim_date
    df = build_dim_date(START_DATE, END_DATE)
    result = validate_dim_date(df)
    result.raise_for_errors()


def task_write_dim_date_iceberg(**ctx):
    """Write dim_date to Iceberg (overwrite)."""
    from stock_lakehouse.gold.dim_date import build_dim_date
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import DIM_DATE_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    df = build_dim_date(START_DATE, END_DATE)
    table = ensure_table(catalog, f"{ns}.dim_date", DIM_DATE_SCHEMA)
    write_dataframe(table, df, mode="overwrite")
    ctx["ti"].xcom_push(key="iceberg_table", value=f"{ns}.dim_date")


def task_sync_dim_date_to_clickhouse(**ctx):
    """Sync dim_date to ClickHouse."""
    from stock_lakehouse.gold.dim_date import build_dim_date
    from stock_lakehouse.clickhouse.loader import sync_dim_date_to_clickhouse
    config = _get_config()
    df = build_dim_date(START_DATE, END_DATE)
    sync_dim_date_to_clickhouse(df, config.clickhouse)


with DAG(
    dag_id="dag_dim_date",
    default_args=default_args,
    description="One-time dim_date generation (2020-2030): Generate → Validate → Iceberg → ClickHouse",
    schedule_interval=None,  # Manual trigger only — run exactly once
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dim_date", "one-time"],
) as dag:
    t1 = PythonOperator(task_id="generate_calendar", python_callable=task_generate_calendar)
    t2 = PythonOperator(task_id="validate_dim_date", python_callable=task_validate_dim_date)
    t3 = PythonOperator(task_id="write_dim_date_iceberg", python_callable=task_write_dim_date_iceberg)
    t4 = PythonOperator(task_id="sync_dim_date_to_clickhouse", python_callable=task_sync_dim_date_to_clickhouse)

    t1 >> t2 >> t3 >> t4
