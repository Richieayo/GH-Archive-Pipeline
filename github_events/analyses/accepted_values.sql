select event_type, count(*) as n
from {{ref('stg_events')}}
group by event_type
order by event_type asc