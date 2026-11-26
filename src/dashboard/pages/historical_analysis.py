"""Interactive historical observation analysis page."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def render(client: Any, filters: dict[str, Any]) -> None:
    """Render observation metrics, charts, map, and the detailed table."""
    year_range = tuple(filters.get("years", (2000, 2026)))
    observations = client.get_observations(year_range=year_range)
    if observations.empty:
        st.info("No observations are available for the selected year range.")
        return

    if filters.get("counties"):
        observations = observations[observations["county"].isin(filters["counties"])]
    if observations.empty:
        st.info("No observations match the selected counties and year range.")
        return

    if "observed_on" in observations:
        observations["observed_on"] = pd.to_datetime(observations["observed_on"], errors="coerce")
    if "year" not in observations:
        observations["year"] = observations["observed_on"].dt.year
    if "month" not in observations:
        observations["month"] = observations["observed_on"].dt.month
    observations = observations.dropna(subset=["county"])

    county_counts = observations.groupby("county").size().sort_values(ascending=False)
    dates = observations["observed_on"].dropna() if "observed_on" in observations else pd.Series(dtype="datetime64[ns]")
    date_range = f"{dates.min():%Y-%m-%d} – {dates.max():%Y-%m-%d}" if not dates.empty else "Unavailable"
    most_active = str(county_counts.index[0]) if not county_counts.empty else "Unavailable"

    total_col, counties_col, range_col, active_col = st.columns(4)
    total_col.metric("Total observations", f"{len(observations):,}")
    counties_col.metric("Counties with observations", f"{observations['county'].nunique():,}")
    range_col.metric("Observation date range", date_range)
    active_col.metric("Most active county", most_active)

    yearly = observations.dropna(subset=["year"]).groupby("year").size().reset_index(name="Observations")
    yearly["year"] = yearly["year"].astype(int)
    trend = px.line(yearly, x="year", y="Observations", markers=True,
                    title="Yearly Observation Trend", labels={"year": "Year"})
    trend.update_xaxes(dtick=1)
    st.plotly_chart(trend, use_container_width=True)
    st.caption("Reported sightings by observation year; the selected county and year filters apply.")

    monthly_counts = (observations.dropna(subset=["month"]).groupby("month").size()
                      .reindex(range(1, 13), fill_value=0).rename("Observations").reset_index())
    monthly_counts["Month"] = monthly_counts["month"].astype(int).map(lambda month: MONTH_NAMES[month - 1])
    peak_month = int(monthly_counts.loc[monthly_counts["Observations"].idxmax(), "month"])
    monthly = px.bar(monthly_counts, x="Month", y="Observations", title="Monthly Observation Distribution",
                     category_orders={"Month": MONTH_NAMES})
    monthly.update_traces(marker_color=["#e76f51" if month == peak_month else "#457b9d"
                                        for month in monthly_counts["month"]])
    st.plotly_chart(monthly, use_container_width=True)
    st.caption(f"Sightings grouped by calendar month across the selected years; peak month: {MONTH_NAMES[peak_month - 1]}.")

    county_data = county_counts.rename("Observations").reset_index(name="Observations")
    county_chart = px.bar(county_data, x="Observations", y="county", orientation="h",
                          title="County-Level Comparison", labels={"county": "County"})
    county_chart.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(county_chart, use_container_width=True)
    st.caption("Count of individual records per county, ordered from fewest to most observations.")

    coordinates = observations.dropna(subset=["latitude", "longitude"]).copy()
    if not coordinates.empty:
        coordinates["latitude"] = pd.to_numeric(coordinates["latitude"], errors="coerce")
        coordinates["longitude"] = pd.to_numeric(coordinates["longitude"], errors="coerce")
        coordinates = coordinates.dropna(subset=["latitude", "longitude"])
    if not coordinates.empty:
        map_chart = px.scatter_map(
            coordinates,
            lat="latitude",
            lon="longitude",
            color="county",
            hover_name="county",
            hover_data=[column for column in ("observation_id", "observed_on") if column in coordinates],
            zoom=5,
            center={"lat": 45.9, "lon": 25.0},
            map_style="open-street-map",
            title="Observation Locations",
        )
        map_chart.update_layout(margin={"r": 0, "t": 45, "l": 0, "b": 0})
        st.plotly_chart(map_chart, use_container_width=True)
        st.caption("Geographic distribution of records with valid coordinates; colors identify counties.")
    else:
        st.info("No valid coordinates are available to display on the map.")

    with st.expander("Detailed observation data", expanded=False):
        st.dataframe(observations, use_container_width=True, hide_index=True)
        st.caption("Use the table headers to sort the matching observation records.")
