--select
--  (select count(*) from github_events.raw.raw_events)        as raw_rows,
--  (select count(*) from github_events.analytics.stg_events)  as stg_rows;


  select count(*) as unparsed
from github_events.analytics.stg_events
where event_id is null;