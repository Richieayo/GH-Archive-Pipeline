with source as (
    select * from {{ source('github_events', 'raw_events') }}
),

parsed as (

    Select
        from_json(
            raw_event,
            'id STRING,
            type STRING,
            created_at STRING,
            public BOOLEAN,
            repo STRUCT<id: BIGINT, name: STRING>,
            org STRUCT<id: BIGINT, login: STRING>,
            actor STRUCT<id: BIGINT,  login: STRING>'
        ) as event,
        event_hour,
        source_file,
        ingested_at
    from source
)

Select
    event.id                       as event_id,
    event.type                     as event_type,
    event.repo.id                  as repo_id,
    event.repo.name                as repo_name,
    event.org.login                as org_login,
    event.actor.login              as actor_login,
    to_timestamp(event.created_at) as created_at,
    event_hour,
    source_file,
    ingested_at
from parsed