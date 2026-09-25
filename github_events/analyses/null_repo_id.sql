--Select *
--from {{ ref("stg_events")}}
--where event_type = 'ForkEvent'
-- repo_id IS NULL


select raw_event
from github_events.raw.raw_events
where raw_event like '%15616601015%';  --Repo might have been deleted or made private