# GitHub Events Pipeline

An hourly, near-real-time pipeline that pulls public GitHub activity from [GH Archive](https://www.gharchive.org/), lands it in a Databricks Unity Catalog volume, streams it into a Delta table with **Auto Loader**, and models it with **dbt** into a repo-by-hour activity mart. **Airflow** orchestrates the whole flow and a **Streamlit** dashboard sits on top.

## Architecture

![Pipeline architecture: Airflow orchestrates ingest (GH Archive → fetch script → UC volume), land (Auto Loader → raw_events), transform (dbt: stg_events → int_events_deduped → fct_repo_activity) and serve (dashboard)](docs/images/architecture.png)

## How it works

**Ingestion** — [ingestion/fetch_gharchive.py](ingestion/fetch_gharchive.py) downloads hourly `YYYY-MM-DD-H.json.gz` files and uploads them to the landing volume with the Databricks SDK. It runs 2 hours behind now (GH Archive publishes ~40 min after each hour), skips files already landed, and treats a 404 as "not published yet". Backfill with `--date 2026-10-01 --hours 24`.

**Auto Loader** — [notebooks/land_raw_events.py](notebooks/land_raw_events.py) reads new files from the volume with `cloudFiles` as plain text, so each JSON event is stored untouched in `raw_event` and GitHub schema changes can't break ingestion. It adds `source_file`, `event_hour` (from the file name) and `ingested_at`.

**Checkpointing** — the stream writes with a checkpoint at `/Volumes/github_events/raw/checkpoints/raw_events`, which records which files have been ingested, so re-runs and retries never load a file twice. `trigger(availableNow=True)` processes whatever is new and stops, so a streaming source runs as an hourly batch with no always-on cluster.

**Databricks job** — the notebook runs as an unscheduled Databricks Job that Airflow triggers via `jobs.run_now(...).result()`, failing the task if the run isn't `SUCCESS`. Setup steps: [docs/databricks_job.md](docs/databricks_job.md).

**Orchestration** — [orchestrate.py](airflow_github_events/dags/orchestrate.py) runs `fetch → Auto Loader → dbt deps → source freshness → dbt build` at 15 past every hour, with `max_active_runs=1` and 2 retries per task. Every step is safe to retry: the fetch skips landed files, the checkpoint skips consumed files, and the mart merges rather than appends. Freshness is report-only. dbt lives in its own venv in the image because dbt-core and Airflow need conflicting `click` versions.

**dbt models** — `stg_events` parses the JSON, `int_events_deduped` keeps one row per `event_id`, and `fct_repo_activity` aggregates to one row per repo per hour with a 3-hour lookback for late files. Sources, tests and analyses: [github_events/README.md](github_events/README.md).

**Dashboard** — [dashboard/app.py](dashboard/app.py) shows activity KPIs, events per hour, top owners/repos, and a pipeline-health view. Expect 2–3 hours end-to-end latency.

## Quickstart

1. **Databricks:** create the catalog, schemas, volumes and the Auto Loader job — see [docs/databricks_job.md](docs/databricks_job.md).
2. **Configure** `airflow_github_events/.env` (never commit it):

   ```dotenv
   AIRFLOW_UID=50000
   DBT_HOST=<workspace>.cloud.databricks.com
   DBT_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
   DBT_TOKEN=<personal-access-token>
   JOB_ID=<databricks-job-id>
   ```

3. **Start Airflow:**

   ```bash
   cd airflow_github_events
   docker compose up airflow-init
   docker compose up -d --build
   ```

   Open http://localhost:8080 (`airflow` / `airflow`) and unpause `github_events_hourly`.

4. **Dashboard** (with the same `DBT_*` variables exported):

   ```bash
   uv sync
   streamlit run dashboard/app.py
   ```

## Environment variables

| Variable | Purpose |
|---|---|
| `DBT_HOST` | Databricks workspace host |
| `DBT_TOKEN` | Databricks personal access token |
| `DBT_HTTP_PATH` | SQL warehouse HTTP path (dbt, dashboard) |
| `JOB_ID` | Auto Loader Databricks job ID (Airflow) |
| `LANDING_VOLUME_PATH` | Optional override for the landing volume |
| `SMTP_*` | Optional Airflow email settings |
