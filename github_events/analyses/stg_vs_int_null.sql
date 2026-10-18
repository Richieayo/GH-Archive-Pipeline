select
  (select count(*) from github_events.analytics.stg_events)         as stg,
  (select count(*) from github_events.analytics.stg_events where repo_id is null) as null_repo,
  (select count(*) from github_events.analytics.int_events_deduped) as deduped;
