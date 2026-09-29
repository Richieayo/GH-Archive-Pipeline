Select event_id, count(*) as n
from {{ ref("int_events_deduped")}}
group by event_id
having count(*) > 1