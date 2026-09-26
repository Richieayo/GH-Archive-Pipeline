{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['repo_id', 'event_hour'],
    file_format='delta',
    partition_by=['event_hour'],
    on_schema_change='sync_all_columns'
) }}

with events as (

    select *
    from {{ ref('int_events_deduped') }}

    {% if is_incremental() %}
   
    where event_hour >= (
        select dateadd(hour, -3, max(event_hour)) from {{ this }}
    )
    {% endif %}

)

select
    repo_id,
    event_hour,
    max(repo_name) as repo_name,
    max(coalesce(org_login, split(repo_name, '/')[0])) as owner_login,
    max(case when org_login is not null then 'organization' else 'personal' end) as owner_type,

    count(*) as total_events,
    count(distinct actor_login) as distinct_actors,

    count_if(event_type = 'PushEvent') as push_events,
    count_if(event_type = 'PullRequestEvent') as pull_request_events,
    count_if(event_type = 'IssuesEvent') as issue_events,
    count_if(event_type = 'IssueCommentEvent') as issue_comment_events,
    count_if(event_type = 'PullRequestReviewEvent') as pr_review_events,
    count_if(event_type = 'ForkEvent') as fork_events,
    count_if(event_type = 'WatchEvent') as watch_events,
    count_if(event_type = 'ReleaseEvent') as release_events,
    count_if(event_type not in (
        'PushEvent','PullRequestEvent','IssuesEvent','IssueCommentEvent',
        'PullRequestReviewEvent','ForkEvent','WatchEvent','ReleaseEvent'
    )) as other_events,
    count_if(event_type_label = 'Unrecognized') as unrecognized_events,

    min(created_at) as first_event_at,
    max(created_at) as last_event_at

from events
group by repo_id, event_hour