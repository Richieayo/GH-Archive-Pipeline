# Databricks setup: the Auto Loader job

Airflow does not run Spark itself. It triggers a **Databricks Job** whose single task runs [notebooks/land_raw_events.py](../notebooks/land_raw_events.py), then waits for it to finish.

## 1. Unity Catalog objects (one time)

Run in a Databricks SQL editor or notebook:

```sql
CREATE CATALOG IF NOT EXISTS github_events;
CREATE SCHEMA  IF NOT EXISTS github_events.raw;
CREATE SCHEMA  IF NOT EXISTS github_events.analytics;
CREATE VOLUME  IF NOT EXISTS github_events.raw.landing;
CREATE VOLUME  IF NOT EXISTS github_events.raw.checkpoints;
```

The `raw_events` table is created by the notebook's first run.

## 2. Get the notebook into the workspace

- **Git folder (recommended):** *Workspace → Create → Git folder*, point it at this repository. The notebook is at `notebooks/land_raw_events`, and updates are a `git pull` away.
- **Import:** *Workspace → (your folder) → ⋮ → Import*, upload `notebooks/land_raw_events.py`.

> Databricks opens a `.py` file as a notebook only when its first line is `# Databricks notebook source`. If the workspace shows it as a plain file, add that line.

## 3. Create the job (UI)

1. Sidebar → **Jobs & Pipelines** → **Create → Job**, name it e.g. `github_events_autoloader`.
2. Configure the task:
   - **Type:** `Notebook`
   - **Source:** `Workspace` (or `Git provider`)
   - **Path:** the notebook from step 2
   - **Compute:** `Serverless`, or a small job cluster (cheaper than all-purpose, terminates after the run)
3. Leave the job **unscheduled** — Airflow is the scheduler.
4. **Save**, copy the **Job ID** from the job page (URL `.../jobs/<JOB_ID>`), and put it in `JOB_ID` in `airflow_github_events/.env`.
5. Click **Run now** once to confirm it creates `github_events.raw.raw_events`.

## 3b. Create the job (code)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()  # DATABRICKS_HOST / DATABRICKS_TOKEN or ~/.databrickscfg
job = w.jobs.create(
    name="github_events_autoloader",
    tasks=[
        jobs.Task(
            task_key="land_raw_events",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Workspace/Users/<you>/DBT_GITHUB_EVENTS/notebooks/land_raw_events"
            ),
            # omit cluster settings to use serverless compute
        )
    ],
)
print(job.job_id)
```

## 4. How Airflow calls it

`run_autoloader` in [orchestrate.py](../airflow_github_events/dags/orchestrate.py):

```python
run = client.jobs.run_now(job_id=JOB_ID).result(timeout=timedelta(minutes=30))
```

`.result()` blocks until the run finishes. Any result other than `SUCCESS` fails the Airflow task with a link to the Databricks run page.

## Resetting the checkpoint

Deleting `/Volumes/github_events/raw/checkpoints/raw_events` makes Auto Loader re-read every file in the landing volume. Truncate or drop `raw_events` first, or the raw table will contain duplicates (dbt deduplicates on `event_id`, but the table still grows).
