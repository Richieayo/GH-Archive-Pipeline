"""Hourly GitHub Archive pipeline.

fetch -> Auto Loader -> dbt deps -> freshness -> dbt build

Every task is safe to retry: the ingestion script skips files already in the
volume, Auto Loader's checkpoint skips files already consumed, and the mart
merges on (repo_id, event_hour) rather than appending.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from databricks.sdk import WorkspaceClient

REPO_ROOT = os.environ["GITHUB_EVENTS_REPO"]
AUTOLOADER_JOB_ID = int(os.environ["JOB_ID"])

# dbt runs from an isolated virtualenv baked into the image: dbt-core requires
# click>=8.3 while Airflow pins click<8.3, so installing both into the same
# environment silently breaks the Airflow CLI. See Dockerfile.
DBT_BIN = "/opt/dbt-venv/bin/dbt"


DBT_FLAGS = "--project-dir github_events --profiles-dir github_events"

DEFAULT_ARGS = {
    "owner": "richard",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def run_autoloader(**_context) -> None:
    """Trigger the Databricks notebook job that loads landed files into raw_events."""
    host = os.environ["DBT_HOST"]
    if not host.startswith("https://"):
        host = f"https://{host}"
    client = WorkspaceClient(host=host, token=os.environ["DBT_TOKEN"])

    run = client.jobs.run_now(job_id=AUTOLOADER_JOB_ID).result(timeout=timedelta(minutes=30))

    state = run.state
    if state is None or state.result_state is None or state.result_state.value != "SUCCESS":
        raise RuntimeError(
            f"Auto Loader job {AUTOLOADER_JOB_ID} finished as {state} - {run.run_page_url}"
        )


with DAG(
    dag_id="github_events_hourly",
    description="GH Archive -> Unity Catalog volume -> raw events -> dbt marts",
    default_args=DEFAULT_ARGS,
    schedule="15 * * * *",
    start_date=datetime(2026, 10, 2),
    catchup=False,  # missed runs are recovered by the lookback window, not by backfill
    max_active_runs=1,  # never let two runs merge into the same hours concurrently
    tags=["github-events", "dbt", "databricks"],
) as dag:

    fetch = BashOperator(
        task_id="fetch_gharchive",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python ingestion/fetch_gharchive.py --hours 2"
        ),
    )

    load = PythonOperator(
        task_id="autoloader_to_raw_events",
        python_callable=run_autoloader,
    )

    # Separate task because dbt_packages/ is gitignored and will not exist in a
    # fresh container. Idempotent, so it costs a second or two once installed.
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {REPO_ROOT} && {DBT_BIN} deps {DBT_FLAGS}",
    )

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"cd {REPO_ROOT} && "
            f"{DBT_BIN} source freshness {DBT_FLAGS} || true"
        ),
        retries=0,
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO_ROOT} && {DBT_BIN} build {DBT_FLAGS}",
    )

    fetch >> load >> deps >> freshness >> build