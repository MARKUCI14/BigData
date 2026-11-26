"""Interactive county temperature analysis page."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def render(client: Any, filters: dict[str, Any]) -> None:
    """Render county/month temperature summaries from MongoDB Gold data."""
    monthly = client.get_monthly_stats()
    if monthly.empty:
        st.info("No monthly temperature statistics are available yet. Run the batch pipeline first.")
        return

    required = {"county", "year", "month", "avg_temperature"}
    missing = required - set(monthly.columns)
    if missing:
        st.error(f"Monthly statistics are missing required fields: {', '.join(sorted(missing))}.")
        return

    monthly["year"] = pd.to_numeric(monthly["year"], errors="coerce")
    monthly["month"] = pd.to_numeric(monthly["month"], errors="coerce")
    monthly["avg_temperature"] = pd.to_numeric(monthly["avg_temperature"], errors="coerce")
    monthly = monthly.dropna(subset=["county", "year", "month", "avg_temperature"])
    years = filters.get("years", (2000, 2026))
    monthly = monthly[monthly["year"].between(int(years[0]), int(years[1]))]
    if filters.get("counties"):
        monthly = monthly[monthly["county"].isin(filters["counties"])]
    if monthly.empty:
        st.info("No temperature records match the selected counties and year range.")
        return

    monthly["year"] = monthly["year"].astype(int)
    monthly["month"] = monthly["month"].astype(int)
    monthly = monthly[monthly["month"].between(1, 12)]
    if monthly.empty:
        st.info("No valid monthly temperature records are available for these filters.")
        return
    monthly["Month"] = monthly["month"].map(lambda month: MONTH_NAMES[month - 1])

    heat_data = monthly.groupby(["county", "Month"], as_index=False)["avg_temperature"].mean()
    heat_pivot = heat_data.pivot(index="county", columns="Month", values="avg_temperature").reindex(columns=MONTH_NAMES)
    heatmap = go.Figure(go.Heatmap(
        z=heat_pivot.values,
        x=MONTH_NAMES,
        y=heat_pivot.index,
        colorscale="RdYlBu_r",
        colorbar={"title": "°C"},
        hovertemplate="%{y}<br>%{x}: %{z:.1f} °C<extra></extra>",
    ))
    heatmap.update_layout(title="Average Temperature by County and Month", xaxis_title="Month", yaxis_title="County")
    st.plotly_chart(heatmap, use_container_width=True)
    st.caption("Cell color shows the mean monthly temperature across the selected years, in °C.")

    yearly = monthly.groupby(["year", "county"], as_index=False)["avg_temperature"].mean()
    evolution = px.line(yearly, x="year", y="avg_temperature", color="county", markers=True,
                        title="Temperature Evolution Over Time",
                        labels={"year": "Year", "avg_temperature": "Average temperature (°C)", "county": "County"})
    st.plotly_chart(evolution, use_container_width=True)
    st.caption("Each point is the mean of the available monthly values for that county and year (°C).")

    seasonal = monthly.groupby("month")["avg_temperature"].agg(mean="mean", minimum="min", maximum="max").reindex(range(1, 13))
    month_labels = [MONTH_NAMES[index - 1] for index in seasonal.index]
    pattern = go.Figure()
    pattern.add_trace(go.Scatter(x=month_labels, y=seasonal["maximum"], mode="lines", line={"width": 0},
                                 showlegend=False, hoverinfo="skip"))
    pattern.add_trace(go.Scatter(x=month_labels, y=seasonal["minimum"], mode="lines", line={"width": 0},
                                 fill="tonexty", fillcolor="rgba(69, 123, 157, 0.18)",
                                 name="Min–max range", hovertemplate="%{x}: %{y:.1f} °C<extra></extra>"))
    pattern.add_trace(go.Scatter(x=month_labels, y=seasonal["mean"], mode="lines+markers",
                                 line={"color": "#e76f51", "width": 3}, name="Mean temperature",
                                 hovertemplate="%{x}: %{y:.1f} °C<extra></extra>"))
    pattern.update_layout(title="Monthly Temperature Pattern", xaxis_title="Month", yaxis_title="Temperature (°C)")
    st.plotly_chart(pattern, use_container_width=True)
    st.caption("Line shows the across-year mean; shaded band spans the lowest to highest observed value for each month (°C).")

    comparison = px.box(monthly, x="county", y="avg_temperature", color="county", points="outliers",
                        title="County Temperature Comparison",
                        labels={"county": "County", "avg_temperature": "Temperature (°C)"})
    comparison.update_layout(showlegend=False)
    st.plotly_chart(comparison, use_container_width=True)
    st.caption("Distribution of monthly temperatures for each county in the selected years (°C).")

    counties = sorted(monthly["county"].unique().tolist())
    default_counties = counties[: min(3, len(counties))]
    trend_counties = st.multiselect("Counties for temperature trend", counties, default=default_counties,
                                    key="temperature_trend_counties")
    if trend_counties:
        trend_data = yearly[yearly["county"].isin(trend_counties)]
        trend = px.line(trend_data, x="year", y="avg_temperature", color="county", markers=True,
                        title="Temperature Trend by County",
                        labels={"year": "Year", "avg_temperature": "Average temperature (°C)", "county": "County"})
        st.plotly_chart(trend, use_container_width=True)
        st.caption("Select counties above to compare their yearly average temperature trends (°C).")
    else:
        st.info("Select at least one county to display its temperature trend.")
