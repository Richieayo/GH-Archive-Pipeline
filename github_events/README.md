# github_events (dbt)

Models GH Archive events from `github_events.raw.raw_events` into a repo-by-hour activity mart on Databricks. Profile targets catalog `github_events`, schema `analytics` ([profiles.yml](profiles.yml)); all models are Delta.

```
raw_events (source) → stg_events (view) → int_events_deduped (view) → fct_repo_activity (incremental)
```

## Source

`github_events.raw.raw_events` is written by Auto Loader and **not managed by dbt**. Freshness is checked on `ingested_at`: warn after 3 hours, error after 6.

## Models

| Model | Grain | Notes |
|---|---|---|
| [stg_events](models/staging/stg_events.sql) | one row per raw event | Parses `raw_event` JSON with `from_json`. Adds `event_type_label` = `'Unrecognized'` for unknown event types. Duplicates kept. |
| [int_events_deduped](models/intermediate/int_events_deduped.sql) | one row per `event_id` | Keeps the first ingested copy. Drops events with null `repo_id` (deleted/private repos GitHub emits as `"repo": {}`). |
| [fct_repo_activity](models/marts/fct_repo_activity.sql) | one row per repo per hour | Incremental `merge` on `(repo_id, event_hour)` with a **3-hour lookback** so late files update existing rows. Event counts by type, distinct actors, owner type. |

## Tests

- **Keys:** `event_id` unique and not null after dedup; `(repo_id, event_hour)` unique in the mart.
- **Values:** `event_type` in the known list (warn), `owner_type` in `organization`/`personal`, `total_events >= 1`.
- **Singular:** [assert_event_columns_reconcile](tests/assert_event_columns_reconcile.sql) — per-type counts sum to `total_events`; [assert_no_duplicate_event_ids](tests/assert_no_duplicate_event_ids.sql).

`org_login` is deliberately untested: it is null for personal-account repos.

## Analyses

Ad-hoc checks in [analyses/](analyses/): row counts across each layer, events with no `repo_id`, GH Archive publish delay (p50/p95), and unparsed rows.

## Running

```bash
export DBT_HOST=... DBT_HTTP_PATH=... DBT_TOKEN=...
dbt deps --profiles-dir .
dbt source freshness --profiles-dir .
dbt build --profiles-dir .
dbt build --profiles-dir . --full-refresh   # rebuild the mart from scratch
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```
