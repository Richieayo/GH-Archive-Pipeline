"""GitHub Archive activity dashboard.

Reads the dbt marts from Unity Catalog via a Databricks SQL warehouse.

The time window is anchored to the newest hour in the data rather than to the
wall clock, so the dashboard still renders after the pipeline was paused.

Run from the repo root:
    uv run --env-file github_events/.env streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql

CATALOG = "github_events"
ANALYTICS = f"{CATALOG}.analytics"

st.set_page_config(page_title="GitHub activity", layout="wide")


def connect():
    host = os.environ["DBT_HOST"].replace("https://", "")
    return sql.connect(
        server_hostname=host,
        http_path=os.environ["DBT_HTTP_PATH"],
        access_token=os.environ["DBT_TOKEN"],
    )


@st.cache_data(ttl=600)
def query(statement: str) -> pd.DataFrame:
    """Run a query and cache for 10 minutes.

    Streamlit reruns the whole script on every widget interaction, so without
    caching each filter change would fire a fresh warehouse query.
    """
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(statement)
        return pd.DataFrame(
            cursor.fetchall(), columns=[c[0] for c in cursor.description]
        )


def fmt(value) -> str:
    """Format a count, or show a dash when the filter matched no rows.

    sum() returns NULL over an empty set, not 0, so this is the empty state.
    """
    return f"{int(value):,}" if value is not None else "—"


# ---------------------------------------------------------------- full run

alltime = query(f"""
    select count(*)                    as events,
           count(distinct event_hour)  as hours,
           count(distinct source_file) as files,
           count(distinct repo_id)     as repos,
           count(distinct actor_login) as actors,
           min(event_hour)             as first_hour,
           max(event_hour)             as last_hour
    from {ANALYTICS}.int_events_deduped
""")
a = alltime.iloc[0]

st.title("GitHub Archive activity")
st.caption(
    f"Pipeline ran hourly from {a.first_hour} to {a.last_hour} UTC, then was paused. "
    "While running, end-to-end latency "
    "was 2 to 3 hours: GH Archive publishes each hour's file about 40 minutes after the "
    "hour closes, ingestion ran hourly, and dbt ran hourly after that."
)

st.subheader("Full run")
t1, t2, t3, t4, t5 = st.columns(5)
t1.metric("Events ingested", fmt(a.events))
t2.metric("Hours of data", fmt(a.hours))
t3.metric("Files landed", fmt(a.files))
t4.metric("Distinct repos", fmt(a.repos))
t5.metric("Distinct actors", fmt(a.actors))

st.divider()


# ---------------------------------------------------------------- filters

st.sidebar.header("Filters")
hours = st.sidebar.slider("Hours of history", min_value=6, max_value=168, value=24, step=6)
owner_type = st.sidebar.radio("Repo owner", options=["All", "organization", "personal"], index=0)
min_events = st.sidebar.number_input(
    "Minimum events per repo/hour", min_value=1, value=1, step=1
)
st.sidebar.caption("Window is relative to the newest hour in the data, not to now.")

# Anchored to the data, not current_timestamp(), so the dashboard keeps working
# after the pipeline was paused.
NEWEST = f"(select max(event_hour) from {ANALYTICS}.fct_repo_activity)"
window = f"event_hour >= {NEWEST} - interval {int(hours)} hours"
owner_clause = "" if owner_type == "All" else f"and owner_type = '{owner_type}'"
filters = f"where {window} {owner_clause} and total_events >= {int(min_events)}"

st.subheader(f"Last {hours} hours of data")


# ---------------------------------------------------------------- headline

totals = query(f"""
    select sum(total_events)        as events,
           sum(push_events)         as pushes,
           sum(pull_request_events) as prs,
           sum(issue_events)        as issues,
           count(distinct repo_id)  as repos
    from {ANALYTICS}.fct_repo_activity
    {filters}
""")

# distinct_actors is non-additive: summing it across hours would count anyone
# active in several hours more than once. Computed from the event grain instead.
actors = query(f"""
    select count(distinct actor_login) as actors
    from {ANALYTICS}.int_events_deduped
    where {window}
""")

row = totals.iloc[0]

if row.events is None:
    st.warning(f"No rows matched these filters. Widen the window or lower the minimum.")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Events", fmt(row.events))
c2.metric("Pushes", fmt(row.pushes))
c3.metric("Pull requests", fmt(row.prs))
c4.metric("Issues", fmt(row.issues))
c5.metric("Repos", fmt(row.repos))
c6.metric("Distinct actors", fmt(actors["actors"].iloc[0]))


# ---------------------------------------------------------------- trend

st.subheader("Events per hour")

trend = query(f"""
    select event_hour,
           sum(push_events)         as pushes,
           sum(pull_request_events) as pull_requests,
           sum(issue_events)        as issues,
           sum(other_events)        as other
    from {ANALYTICS}.fct_repo_activity
    {filters}
    group by event_hour
    order by event_hour
""")
if not trend.empty:
    st.area_chart(trend.set_index("event_hour"))


# ---------------------------------------------------------------- breakdowns

left, right = st.columns(2)

with left:
    st.subheader("Most active owners")
    owners = query(f"""
        select owner_login, owner_type,
               sum(total_events)       as events,
               sum(push_events)        as pushes,
               count(distinct repo_id) as repos
        from {ANALYTICS}.fct_repo_activity
        {filters}
        group by owner_login, owner_type
        order by events desc
        limit 25
    """)
    st.dataframe(owners, width="stretch", hide_index=True)

with right:
    st.subheader("Most active repos")
    repos = query(f"""
        select max(repo_name) as repo_name, owner_type,
               sum(total_events)        as events,
               sum(push_events)         as pushes,
               sum(pull_request_events) as prs
        from {ANALYTICS}.fct_repo_activity
        {filters}
        group by repo_id, owner_type
        order by events desc
        limit 25
    """)
    st.dataframe(repos, width="stretch", hide_index=True)


# ---------------------------------------------------------------- health

with st.expander("Pipeline health"):
    health = query(f"""
        select event_hour,
               sum(total_events)        as events,
               count(distinct repo_id)  as repos,
               sum(unrecognized_events) as unrecognized
        from {ANALYTICS}.fct_repo_activity
        where {window}
        group by event_hour
        order by event_hour desc
    """)
    st.caption(
        "A near-zero hour means ingestion failed upstream, not that GitHub was quiet. "
        "Non-zero unrecognized means GitHub shipped an event type that is not in the "
        "accepted_values list."
    )
    st.dataframe(health, width="stretch", hide_index=True)
    st.caption(f"Rendered {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")