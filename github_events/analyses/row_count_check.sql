

  select count(*) as unparsed
from github_events.analytics.stg_events
where event_id is null;