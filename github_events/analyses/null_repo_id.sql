Select *
from {{ ref("stg_events")}}
where repo_id IS NULL
-- event_type = 'ForkEvent'



--select raw_event
--from github_events.raw.raw_events
--where raw_event like '%15616601015%';  --Repo might have been deleted or made private