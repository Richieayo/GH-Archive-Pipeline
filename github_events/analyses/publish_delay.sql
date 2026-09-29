select
    percentile(delay_minutes, 0.50) as p50,
    percentile(delay_minutes, 0.95) as p95,
    max(delay_minutes)              as worst,
    count(distinct source_file)     as files
from (
    select
        source_file,
        (unix_timestamp(min(ingested_at)) - unix_timestamp(event_hour)) / 60.0 as delay_minutes
    from github_events.raw.raw_events
    group by source_file, event_hour
);
---Run when ingestion is on a schedule