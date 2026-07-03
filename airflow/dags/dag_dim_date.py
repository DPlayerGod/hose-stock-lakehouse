"""DAG: dim_date — one-time calendar generation using TaskFlow API.

Flow:
    run_dim_date_pipeline (build → validate → Iceberg overwrite → ClickHouse sync)

This DAG generates dim_date exactly ONCE from 2020-01-01 to 2030-12-31.
It is configured with schedule_interval=None (manual trigger only).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

START_DATE = "2020-01-01"
END_DATE = "2030-12-31"

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@task
def run_dim_date_pipeline() -> dict:
    """Build dim_date, overwrite Iceberg, and sync to ClickHouse."""
    from stock_lakehouse.pipelines.dim_date import run_dim_date_pipeline
    result = run_dim_date_pipeline(start_date=START_DATE, end_date=END_DATE)
    return {"row_count": result.rows, "iceberg_table": result.iceberg_table}


with DAG(
    dag_id="dag_dim_date",
    default_args=default_args,
    description="One-time dim_date generation (2020-2030): build → validate → Iceberg → ClickHouse",
    schedule_interval=None,  # Manual trigger only — run exactly once
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dim_date", "one-time", "taskflow"],
) as dag:
    run_dim_date_pipeline()
