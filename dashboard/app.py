"""Streamlit dashboard over the DuckDB marts.

Read-only consumer of the warehouse: everything shown here is a query against
fct_job_postings / fct_job_skills, so the dashboard has no pipeline logic.

Run:  streamlit run dashboard/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from jobintel.config import DUCKDB_PATH  # noqa: E402

# -- design tokens (validated reference palette, light mode) -----------------
SURFACE = "#fcfcfb"
PAGE = "#f9f9f7"
INK = "#0b0b0b"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
# categorical slots in fixed order — assigned to entities in this order, never cycled
SERIES = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]
BLUE = SERIES[0]

LAYOUT = dict(
    plot_bgcolor=SURFACE,
    paper_bgcolor=SURFACE,
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', color=INK, size=13),
    margin=dict(l=8, r=8, t=8, b=8),
    xaxis=dict(gridcolor=GRID, linecolor=BASELINE, tickfont=dict(color=INK_MUTED), zeroline=False),
    yaxis=dict(gridcolor=GRID, linecolor=BASELINE, tickfont=dict(color=INK_MUTED), zeroline=False),
    hoverlabel=dict(bgcolor=INK, font=dict(color="#ffffff", size=12)),
    barcornerradius=4,
)


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    return get_connection().execute(sql).df()


st.set_page_config(page_title="Job Market Dashboard", page_icon="📈", layout="wide")

if not DUCKDB_PATH.exists():
    st.error(
        "Warehouse not found. Run the pipeline first: materialize all assets in "
        "Dagster (`dagster dev`) or run ingestion + `dbt build` by hand."
    )
    st.stop()

st.title("Job Market Dashboard")
st.caption(
    "Data engineering job postings scraped daily from public Greenhouse and Lever "
    "job-board APIs, modeled with dbt in DuckDB, orchestrated by Dagster."
)

# -- filters (one row above the charts) ---------------------------------------
companies = query("select distinct company from main_marts.fct_job_postings order by 1")
fcol1, fcol2 = st.columns([3, 1])
with fcol1:
    picked = st.multiselect("Companies", companies["company"].tolist(), default=[])
with fcol2:
    active_only = st.toggle("Active postings only", value=True)

conds = []
if picked:
    quoted = ",".join(f"'{c}'" for c in picked)
    conds.append(f"company in ({quoted})")
if active_only:
    conds.append("is_active")
where = ("where " + " and ".join(conds)) if conds else ""

postings = query(f"select * from main_marts.fct_job_postings {where}")
skills = query(f"select * from main_marts.fct_job_skills {where}")

# -- KPI row -------------------------------------------------------------------
latest_seen = postings["last_seen_date"].max()
new_this_week = int(
    (pd.to_datetime(postings["first_seen_date"]) >= pd.Timestamp.now() - pd.Timedelta(days=7)).sum()
)
top_skill = skills["skill"].mode().iat[0] if len(skills) else "—"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Postings", f"{len(postings):,}")
k2.metric("Companies", postings["company"].nunique())
k3.metric("First seen in last 7 days", f"{new_this_week:,}")
k4.metric("Most demanded skill", top_skill)

st.divider()

# -- top skills (magnitude -> single hue, direct values on bar ends) -----------
c1, c2 = st.columns(2)

with c1:
    st.subheader("Most in-demand skills")
    top = (
        skills.groupby("skill", as_index=False)
        .agg(postings=("posting_key", "nunique"))
        .sort_values("postings", ascending=True)
        .tail(15)
    )
    fig = go.Figure(
        go.Bar(
            x=top["postings"],
            y=top["skill"],
            orientation="h",
            marker_color=BLUE,
            hovertemplate="%{y}: mentioned in %{x} postings<extra></extra>",
        )
    )
    fig.update_layout(**LAYOUT, height=440, showlegend=False)
    st.plotly_chart(fig, width="stretch")

with c2:
    st.subheader("Postings by company")
    by_company = (
        postings.groupby("company", as_index=False)
        .agg(postings=("posting_key", "nunique"))
        .sort_values("postings", ascending=True)
    )
    fig = go.Figure(
        go.Bar(
            x=by_company["postings"],
            y=by_company["company"],
            orientation="h",
            marker_color=BLUE,
            hovertemplate="%{y}: %{x} postings<extra></extra>",
        )
    )
    fig.update_layout(**LAYOUT, height=440, showlegend=False)
    st.plotly_chart(fig, width="stretch")

# -- postings over time ---------------------------------------------------------
st.subheader("Newly observed postings per week")
weekly = (
    postings.assign(week=pd.to_datetime(postings["first_seen_date"]).dt.to_period("W").dt.start_time)
    .groupby("week", as_index=False)
    .agg(postings=("posting_key", "nunique"))
)
fig = go.Figure(
    go.Bar(
        x=weekly["week"],
        y=weekly["postings"],
        marker_color=BLUE,
        hovertemplate="Week of %{x|%b %d}: %{y} new postings<extra></extra>",
    )
)
fig.update_layout(**LAYOUT, height=300, showlegend=False)
st.plotly_chart(fig, width="stretch")
st.caption(
    "“New” means first observed by this pipeline — history accumulates from the "
    "date ingestion started, one snapshot per day."
)

# -- skill trend lines (identity -> categorical slots in fixed order) -----------
st.subheader("Skill demand over time")
top5 = skills.groupby("skill")["posting_key"].nunique().nlargest(5).index.tolist()
trend = (
    skills[skills["skill"].isin(top5)]
    .assign(week=pd.to_datetime(skills["first_seen_date"]).dt.to_period("W").dt.start_time)
    .groupby(["week", "skill"], as_index=False)
    .agg(postings=("posting_key", "nunique"))
)
fig = go.Figure()
for slot, skill in enumerate(top5):
    part = trend[trend["skill"] == skill]
    fig.add_trace(
        go.Scatter(
            x=part["week"],
            y=part["postings"],
            name=skill,
            mode="lines+markers",
            line=dict(color=SERIES[slot], width=2),
            marker=dict(size=8),
            hovertemplate=skill + ", week of %{x|%b %d}: %{y} postings<extra></extra>",
        )
    )
fig.update_layout(
    **LAYOUT,
    height=340,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color=INK)),
    hovermode="x unified",
)
st.plotly_chart(fig, width="stretch")

# -- table view (accessibility + drill-down) ------------------------------------
st.subheader("Postings")
st.dataframe(
    postings[
        ["company", "title", "department", "location", "first_seen_date", "is_active", "url"]
    ].sort_values("first_seen_date", ascending=False),
    width="stretch",
    hide_index=True,
    column_config={"url": st.column_config.LinkColumn("posting", display_text="open ↗")},
)

st.caption(f"Latest snapshot: {latest_seen} · Warehouse: {DUCKDB_PATH.name}")
