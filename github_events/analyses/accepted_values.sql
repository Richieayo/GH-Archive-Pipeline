--select event_type, count(*) as n
--from {{ref('stg_events')}}
--group by event_type
--order by event_type asc



select
  (select count(*) from github_events.raw.raw_events)               as raw_rows,
  (select count(*) from github_events.analytics.stg_events)         as stg_rows,
  (select count(*) from github_events.analytics.int_events_deduped) as deduped_rows,
  (select sum(total_events) from github_events.analytics.fct_repo_activity) as mart_events;