"""Live monitoring for streaming observations and county aggregates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


ACTIVE_WINDOW = timedelta(hours=1)
STREAM_FRESHNESS = timedelta(minutes=5)


def _utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def render(client: Any, filters: dict[str, Any]) -> None:
    """Render automatically refreshing streaming KPIs, feed, chart, and map."""
    refresh_seconds = st.sidebar.slider(
        "Realtime refresh interval (seconds)", min_value=5, max_value=60, value=10, step=5,
        key="realtime_refresh_seconds",
    )
    selected_counties = filters.get("counties") or None
    year_range = tuple(filters.get("years", (2000, datetime.now().year)))

    @st.fragment(run_every=timedelta(seconds=refresh_seconds))
    def live_panel() -> None:
        now = datetime.now(timezone.utc)
        try:
            statistics = client.get_realtime_stats()
            if not isinstance(statistics, list):
                statistics = []
            if selected_counties:
                statistics = [item for item in statistics if item.get("county") in selected_counties]

            recent_since = now - ACTIVE_WINDOW
            latest_since = now - STREAM_FRESHNESS
            active_counties = [item for item in statistics
                               if (updated := _utc(item.get("last_updated"))) is not None
                               and updated >= recent_since]
            stream_active = any(
                (updated := _utc(item.get("last_updated"))) is not None and updated >= latest_since
                for item in statistics
            )

            total_received = sum(int(item.get("total_observations", 0)) for item in statistics)
            last_hour = client.count_stream_observations(since=recent_since, counties=selected_counties)
            feed = client.get_latest_observations(limit=20, realtime_only=True, counties=selected_counties)
            if not feed.empty:
                if "observed_on" in feed:
                    feed["observed_on"] = pd.to_datetime(feed["observed_on"], errors="coerce")
                    feed["_year"] = feed["observed_on"].dt.year
                    feed = feed[feed["_year"].between(int(year_range[0]), int(year_range[1]))]
                if "timestamp" in feed:
                    feed["timestamp"] = pd.to_datetime(feed["timestamp"], errors="coerce", utc=True)
                if "latitude" in feed:
                    feed["latitude"] = pd.to_numeric(feed["latitude"], errors="coerce")
                if "longitude" in feed:
                    feed["longitude"] = pd.to_numeric(feed["longitude"], errors="coerce")

            latest_timestamp = None
            if not feed.empty and "timestamp" in feed and feed["timestamp"].notna().any():
                latest_timestamp = feed["timestamp"].max()

            total_col, hour_col, counties_col, latest_col = st.columns(4)
            total_col.metric("Streaming observations", f"{total_received:,}")
            hour_col.metric("Received in the last hour", f"{last_hour:,}")
            counties_col.metric("Active counties (last hour)", f"{len(active_counties):,}")
            latest_col.metric("Latest processed event", latest_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
                              if latest_timestamp is not None else "No events")

            if stream_active:
                st.success("🟢 Streaming pipeline active")
            else:
                st.error("🔴 Streaming pipeline inactive (no county update in the last 5 minutes)")

            if not statistics and feed.empty:
                st.info("No streaming data available. Start the Kafka producer and Spark streaming consumer.")
                return

            st.subheader("Observations per County")
            county_totals = pd.DataFrame([
                {"county": item.get("county", "Unknown"),
                 "observations": int(item.get("total_observations", 0))}
                for item in statistics
            ])
            if not county_totals.empty:
                county_chart = px.bar(county_totals.sort_values("observations"), x="observations", y="county",
                                      orientation="h", title="Streaming observations by county",
                                      labels={"observations": "Observations", "county": "County"})
                st.plotly_chart(county_chart, use_container_width=True)
                st.caption("Cumulative first-seen streaming observations per county.")
            else:
                st.info("No county aggregates are available for the selected counties.")

            st.subheader("Recent Observations")
            if not feed.empty:
                display_columns = [column for column in
                                   ("timestamp", "county", "latitude", "longitude", "observation_id")
                                   if column in feed]
                display = feed[display_columns].copy()
                if {"latitude", "longitude"}.issubset(display.columns):
                    display["coordinates"] = display.apply(
                        lambda row: (f"{row['latitude']:.5f}, {row['longitude']:.5f}"
                                     if pd.notna(row["latitude"]) and pd.notna(row["longitude"]) else ""), axis=1
                    )
                    display = display.drop(columns=["latitude", "longitude"])
                st.dataframe(display, use_container_width=True, hide_index=True)
                st.caption("Latest streaming events matching the selected year and county filters.")

                map_data = feed.dropna(subset=["latitude", "longitude"]) if {
                    "latitude", "longitude"
                }.issubset(feed.columns) else pd.DataFrame()
                if not map_data.empty:
                    live_map = px.scatter_mapbox(
                        map_data, lat="latitude", lon="longitude", color="county",
                        hover_name="county",
                        hover_data=[column for column in ("observation_id", "timestamp") if column in map_data],
                        center={"lat": 45.9, "lon": 25.0}, zoom=5,
                        mapbox_style="open-street-map", title="Recent Streaming Locations",
                    )
                    live_map.update_layout(margin={"r": 0, "t": 45, "l": 0, "b": 0})
                    st.plotly_chart(live_map, use_container_width=True)
                    st.caption("Recent streaming events with valid coordinates; the map refreshes automatically.")
                else:
                    st.info("No recent streaming observations have valid coordinates for the map.")
            else:
                st.info("No streaming observations match the selected filters.")
        except Exception as exc:
            st.warning(f"Could not refresh live data from MongoDB: {exc}")

    live_panel()
