from __future__ import annotations

import os
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
from databricks import sql


CATALOG = "github_events"
ANALYTICS = f"{CATALOG}.analytics"

st.set_page_config(page_title="Github Activity", layout="wide")

def connect():
    host = os.environ["DBT_HOST"].replace("https://", "")
    return sql.connect(
        server_hostname=host,
        http_path=os.environ["DBT_HTTP_PATH"],
        access_token=os.environ["DBT_TOKEN"],
    )

@st.cache_data(ttl=600)
def query(statement: str) -> pd.DataFrame:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(statement)
        return pd.DataFrame(
            cursor.fetchall(), columns=[c[0] for c in cursor.description]
        )
    

st.sidebar.header("Filters")
hours = st.sidebar.slider("Hours of history", min_value=6, max_value=168, value=24, step=6)
owner_type = st.sidebar.radio("Repo owner", options=["All", "organization", "personal"], index=0)
min_events = st.sidebar.number_input(
    "Minimum events per repo/hour", min_value=1, value=1, step=1
)

window = f"event_hour >= current_timestamp() - interval {int(hours)} hours"
owner_clause = "" if owner_type == "All" else f"and owner_type = '{owner_type}'"
filters = f"where {window} {owner_clause} and total_events >= {int(min_events)}"


latency = query(f"""
        select max(event_hour) as newest_hour,
        timestampdiff(minute, max(event_hour), current_timestamp()) as lag_minutes
        from {ANALYTICS}.fct_repo_activity
        """)


newest = latency["newest_hour"].iloc[0]
lag_minutes = int(latency["lag_minutes"].iloc[0])

st.title("Github Archive activity")
st.caption(
    f"Newest hour in the mart: **{newest} UTC** - {lag_minutes // 60}h {lag_minutes % 60}m "
    "behind now. This pipeline is near-real-time, not real-time: GH Archive publishes "
    "each hour's file about 40 minutes after the hour closes, ingestion runs hourly, and "
    "dbt runs hourly after that. Typical end-to-end latency is 2-3 hours."
)

totals = query(f"""
    select sum(total_events) as events,
        sum(push_events) as pushes,
        sum(pull_request_events) as prs,
        sum(issue_events) as issues,
        count(distinct repo_id) as repos
        from {ANALYTICS}.fct_repo_activity
        {filters}
        """)


actors = query(f"""
    select count(distinct actor_login) as actors
    from {ANALYTICS}.int_events_deduped
    where {window}
    """)


row = totals.iloc[0]
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Events", f"{int(row.events):,}")
c2.metric("Pushes", f"{int(row.pushes):,}")
c3.metric("Pull requests", f"{int(row.prs):,}")
c4.metric("Issues", f"{int(row.issues):,}")
c5.metric("Repos", f"{int(row.repos):,}")
c6.metric("Distinct actors", f"{int(actors['actors'].iloc[0]):,}")

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
st.area_chart(trend.set_index("event_hour"))
 
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



 
with st.expander("Pipeline health"):
    health = query(f"""
        select event_hour,
               sum(total_events)        as events,
               count(distinct repo_id)  as repos,
               sum(unrecognized_events) as unrecognized_events
        from {ANALYTICS}.fct_repo_activity
        where {window}
        group by event_hour
        order by event_hour desc
    """)
    st.caption(
        "A near-zero hour means ingestion failed upstream, not that GitHub was quiet. "
        "Non-zero unrecognized means GitHub has shipped an event type that is not in "
        "the accepted_values list."
    )
    st.dataframe(health, width="stretch", hide_index=True)
    st.caption(f"Rendered {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")