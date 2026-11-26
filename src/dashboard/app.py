"""Clouded Apollo analytics dashboard entry point."""

from __future__ import annotations

import os
from datetime import datetime

import streamlit as st

try:
    from src.storage.mongodb_client import MongoDBClient
    from src.dashboard.pages.historical_analysis import render as render_historical_analysis
except ModuleNotFoundError:  # Docker image can also expose storage as a top-level package.
    from storage.mongodb_client import MongoDBClient
    from dashboard.pages.historical_analysis import render as render_historical_analysis


PAGES = (
    "📊 Historical Analysis",
    "🌡️ Temperature Analysis",
    "🔗 Correlation Analysis",
    "📡 Real-Time Monitoring",
)

st.set_page_config(
    page_title="Clouded Apollo Analytics",
    page_icon="🦋",
    layout="wide",
)


@st.cache_resource
def get_mongodb_client() -> MongoDBClient:
    """Create one pooled MongoDB client per Streamlit server process."""
    return MongoDBClient(database=os.getenv("MONGO_DATABASE", "bigdata"))


st.title("Clouded Apollo (Parnassius mnemosyne) in Romania")
st.write(
    "Explore reported Clouded Apollo observations alongside county-level environmental "
    "temperatures. Observation records describe reported sightings, not population size."
)
st.caption("Data sources: iNaturalist observations · Copernicus temperature data")

with st.sidebar:
    st.header("Dashboard")
    selected_page = st.radio("Analysis", PAGES, label_visibility="collapsed")
    st.divider()
    st.subheader("Global filters")

    client = None
    connection_error = None
    counties: list[str] = []
    first_year, last_year = 2000, datetime.now().year
    try:
        client = get_mongodb_client()
        counties = client.get_available_counties()
        minimum, maximum = client.get_date_range()
        if minimum and maximum:
            first_year = int(minimum[:4])
            last_year = int(maximum[:4])
    except Exception as exc:  # Keep the app usable while MongoDB is starting or empty.
        connection_error = str(exc)

    selected_counties = st.multiselect("County", counties, default=[])
    if first_year > last_year:
        first_year, last_year = last_year, first_year
    if first_year == last_year:
        last_year += 1
    selected_years = st.slider(
        "Year range",
        min_value=first_year,
        max_value=last_year,
        value=(first_year, last_year),
    )
    st.session_state["dashboard_filters"] = {
        "counties": selected_counties,
        "years": selected_years,
    }

st.header(selected_page)
if connection_error:
    st.warning(
        "MongoDB is not available yet. Start the database service to load dashboard data. "
        f"Connection detail: {connection_error}"
    )
elif client is not None and not counties:
    st.info("MongoDB is connected, but no observation data is available yet.")
elif selected_page == PAGES[0] and client is not None:
    try:
        render_historical_analysis(client, st.session_state["dashboard_filters"])
    except Exception as exc:
        st.error(f"Could not load historical analysis: {exc}")
else:
    st.info(
        "Choose a dashboard section from the sidebar. Its analysis view will use the "
        "global county and year filters shown there."
    )

st.divider()
st.caption("Team work dream work · Clouded Apollo Big Data project")
