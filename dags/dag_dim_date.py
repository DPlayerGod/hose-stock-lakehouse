"""DAG: dim_date — one-time calendar generation.

Flow:
    run_dim_date_pipeline (build → validate → Iceberg overwrite → ClickHouse sync)

This DAG generates dim_date exactly ONCE from 2020-01-01 to 2030-12-31.
It is configured with schedule_interval=None (manual trigger only).

The DAG is only an Airflow shell: all business logic lives in
``stock_lakehouse.pipelines.dim_date``. dim_date is a small, deterministic,
one-time job, so a single atomic task is the right granularity — there is no
flaky step that would benefit from independent retries.
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


def task_run_dim_date_pipeline(**ctx):
    """Build dim_date, overwrite Iceberg, and sync to ClickHouse."""
    from stock_lakehouse.pipelines.dim_date import run_dim_date_pipeline

    result = run_dim_date_pipeline(start_date=START_DATE, end_date=END_DATE)
    ctx["ti"].xcom_push(key="row_count", value=result.rows)
    ctx["ti"].xcom_push(key="iceberg_table", value=result.iceberg_table)


with DAG(
    dag_id="dag_dim_date",
    default_args=default_args,
    description="One-time dim_date generation (2020-2030): build → validate → Iceberg → ClickHouse",
    schedule_interval=None,  # Manual trigger only — run exactly once
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dim_date", "one-time"],
) as dag:
    PythonOperator(
        task_id="run_dim_date_pipeline",
        python_callable=task_run_dim_date_pipeline,
    )
