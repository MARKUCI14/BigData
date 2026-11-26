"""Temperature and observation relationship analysis."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from scipy.stats import pearsonr


MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _correlation_row(label: str, frame: pd.DataFrame) -> dict[str, Any]:
    """Calculate Pearson r and its two-sided p-value for one filtered group."""
    pairs = frame[["avg_temperature", "observation_count"]].dropna()
    size = len(pairs)
    if size < 2 or pairs["avg_temperature"].nunique() < 2 or pairs["observation_count"].nunique() < 2:
        coefficient, p_value = np.nan, np.nan
    else:
        result = pearsonr(pairs["avg_temperature"], pairs["observation_count"])
        coefficient, p_value = float(result.statistic), float(result.pvalue)
    return {"Group": label, "Pearson r": coefficient, "p-value": p_value, "Sample size": size}


def _correlation_table(monthly: pd.DataFrame) -> pd.DataFrame:
    rows = [_correlation_row("Overall", monthly)]
    rows.extend(_correlation_row(f"County: {county}", group)
                for county, group in monthly.groupby("county", sort=True))
    rows.extend(_correlation_row(f"Year: {year}", group)
                for year, group in monthly.groupby("year", sort=True))
    return pd.DataFrame(rows)


def render(client: Any, filters: dict[str, Any]) -> None:
    """Render filtered correlation visuals and explain their statistical limits."""
    monthly = client.get_monthly_stats()
    if monthly.empty:
        st.info("No monthly statistics are available yet. Run the batch pipeline first.")
        return

    required = {"county", "year", "month", "avg_temperature", "observation_count"}
    missing = required - set(monthly.columns)
    if missing:
        st.error(f"Monthly statistics are missing required fields: {', '.join(sorted(missing))}.")
        return

    for column in ("year", "month", "avg_temperature", "observation_count"):
        monthly[column] = pd.to_numeric(monthly[column], errors="coerce")
    monthly = monthly.dropna(subset=list(required))
    selected_years = filters.get("years", (2000, 2026))
    monthly = monthly[monthly["year"].between(int(selected_years[0]), int(selected_years[1]))]
    if filters.get("counties"):
        monthly = monthly[monthly["county"].isin(filters["counties"])]
    if monthly.empty:
        st.info("No paired temperature and observation records match the selected filters.")
        return

    monthly["year"] = monthly["year"].astype(int)
    monthly["month"] = monthly["month"].astype(int)
    monthly = monthly[monthly["month"].between(1, 12)]
    if monthly.empty:
        st.info("No valid monthly records are available for these filters.")
        return
    monthly["Month"] = monthly["month"].map(lambda month: MONTH_NAMES[month - 1])

    sighting_count = int(monthly["observation_count"].sum())
    sighting_metric, pair_metric = st.columns(2)
    sighting_metric.metric("Reported sightings in selection", f"{sighting_count:,}")
    pair_metric.metric("County-month pairs", f"{len(monthly):,}")
    st.caption("The correlation sample is monthly county aggregates; the underlying sighting records are much fewer.")

    scatter = px.scatter(
        monthly,
        x="avg_temperature",
        y="observation_count",
        color="county",
        hover_data={"county": True, "year": True, "month": True,
                    "avg_temperature": ":.2f", "observation_count": True},
        title="Monthly Temperature vs. Observation Count",
        labels={"avg_temperature": "Average temperature (°C)",
                "observation_count": "Observations per county-month", "county": "County"},
    )
    if st.checkbox("Show overall linear trend", value=False):
        x = monthly["avg_temperature"].to_numpy(dtype=float)
        y = monthly["observation_count"].to_numpy(dtype=float)
        if len(x) >= 2 and np.unique(x).size >= 2:
            slope, intercept = np.polyfit(x, y, 1)
            trend_x = np.linspace(float(x.min()), float(x.max()), 100)
            scatter.add_trace(go.Scatter(x=trend_x, y=slope * trend_x + intercept, mode="lines",
                                         name="Overall linear trend", line={"color": "black", "dash": "dash"}))
    st.plotly_chart(scatter, use_container_width=True)
    st.caption("Each point represents a county-month pair. Temperature is in °C; the count may be zero.")

    summary = _correlation_table(monthly)
    overall = summary.iloc[0]
    if pd.notna(overall["Pearson r"]):
        direction = "positive" if overall["Pearson r"] > 0 else "negative" if overall["Pearson r"] < 0 else "near zero"
        evidence = ("statistically significant at the 5% level" if overall["p-value"] < 0.05
                    else "not statistically significant at the 5% level")
        st.info(
            f"For the selected data, the overall linear association is {direction} "
            f"(Pearson r = {overall['Pearson r']:.3f}, p = {overall['p-value']:.3g}, "
            f"n = {int(overall['Sample size'])}). It is {evidence}; this is an association, "
            "not evidence that temperature causes changes in sightings. Interpret p-values "
            "cautiously because the records are repeated county-month aggregates and there are "
            "few underlying sighting events."
        )
    else:
        st.info("There are too few paired values, or one variable is constant, so Pearson correlation is undefined.")
    st.subheader("Correlation coefficient summary")
    st.dataframe(summary, use_container_width=True, hide_index=True,
                 column_config={"Pearson r": st.column_config.NumberColumn(format="%.3f"),
                                "p-value": st.column_config.NumberColumn(format="%.3g")})
    st.caption("Pearson r and two-sided p-values are recalculated from the selected county-month pairs.")

    seasonal = monthly.groupby("month", as_index=False).agg(
        average_observations=("observation_count", "mean"),
        average_temperature=("avg_temperature", "mean"),
    )
    seasonal = seasonal.set_index("month").reindex(range(1, 13)).rename_axis("month").reset_index()
    seasonal["Month"] = seasonal["month"].map(lambda month: MONTH_NAMES[month - 1])
    overlay = make_subplots(specs=[[{"secondary_y": True}]])
    overlay.add_trace(go.Scatter(x=seasonal["Month"], y=seasonal["average_observations"],
                                 mode="lines+markers", name="Average observations"), secondary_y=False)
    overlay.add_trace(go.Scatter(x=seasonal["Month"], y=seasonal["average_temperature"],
                                 mode="lines+markers", name="Average temperature (°C)"), secondary_y=True)
    overlay.update_layout(title="Seasonal Observation and Temperature Pattern")
    overlay.update_xaxes(title_text="Month", categoryorder="array", categoryarray=MONTH_NAMES)
    overlay.update_yaxes(title_text="Average observations per county-month", secondary_y=False)
    overlay.update_yaxes(title_text="Average temperature (°C)", secondary_y=True)
    st.plotly_chart(overlay, use_container_width=True)
    st.caption("Monthly averages across the selected counties and years; the two lines use separate vertical scales.")

    counties = sorted(monthly["county"].unique().tolist())
    default_counties = counties[: min(3, len(counties))]
    compare_counties = st.multiselect("Counties to compare (choose 2–3)", counties,
                                     default=default_counties, max_selections=3,
                                     key="correlation_compare_counties")
    if compare_counties:
        comparison = monthly[monthly["county"].isin(compare_counties)]
        panel = px.scatter(comparison, x="avg_temperature", y="observation_count",
                           color="Month", facet_col="county", facet_col_wrap=3,
                           hover_data=["year", "month"], title="County Observation–Temperature Patterns",
                           labels={"avg_temperature": "Average temperature (°C)",
                                   "observation_count": "Observations per county-month"})
        panel.update_xaxes(matches=None)
        panel.update_yaxes(matches=None)
        st.plotly_chart(panel, use_container_width=True)
        st.caption("Each panel compares monthly temperature and observation counts for one selected county.")
    else:
        st.info("Select one to three counties to compare their monthly patterns.")

    with st.expander("Limitations and methodology"):
        st.markdown(
            "- An observation is a reported sighting and does not measure population size.\n"
            "- Observer effort, access, season, and reporting habits can affect the number of records.\n"
            "- Pearson correlation describes a linear association; correlation does not establish causation.\n"
            "- The sample size is the number of county-month pairs, not independent butterfly counts. "
            "Results should be interpreted cautiously, especially for individual counties or years.\n"
            "- P-values do not correct for observer bias, repeated measurements, or multiple comparisons."
        )
