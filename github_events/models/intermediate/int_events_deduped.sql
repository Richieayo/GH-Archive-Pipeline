with staged as (
    select
        *
    from {{ ref('stg_events')}}
    where repo_id is not null
    -- GitHub occasionally emits "repo": {} for events whose repo was deleted or
    -- made private before the archive was written. No repo_id means the event
    -- can't be attributed to a repo, so it is dropped here rather than producing
    -- a NULL bucket in fct_repo_activity.
),

ranked as (
    select
        *,
        row_number() over (partition by event_id order by ingested_at, source_file) as event_rank
    from staged
)

select
    event_id,
    event_type,
    event_type_label,
    repo_id,
    repo_name,
    org_login,
    actor_login,
    created_at,
    event_hour,
    source_file,
    ingested_at
from ranked
where event_rank = 1

