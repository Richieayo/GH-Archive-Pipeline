
select repo_id, event_hour
from {{ ref('fct_repo_activity') }}
where push_events + pull_request_events + issue_events + issue_comment_events
    + pr_review_events + fork_events + watch_events + release_events
    + other_events
  != total_events