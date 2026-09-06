import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def render(con):
    # --- Performance Trends ---
    section("Performance Trends")

    trends = dq.get_trends(con)
    if not trends.empty and len(trends) > 1:

        # Toggle for Plot Metric
        metric_type = st.radio("Plot Metric:", ["Pace", "Time"], horizontal=True)

        # Prepare data based on selection
        if metric_type == "Pace":
            y_col = "Pace Time"
            value_vars = ["min_pace_seconds", "p95_pace_seconds", "median_pace_seconds"]
            metric_map = {
                "min_pace_seconds": "Winning Pace",
                "p95_pace_seconds": "Slowest Pace (95th %)",
                "median_pace_seconds": "Median Pace"
            }
            tick_format = "%M:%S"
            y_range = None
        else:
            y_col = "Finish Time"
            value_vars = ["min_time_seconds", "p95_time_seconds", "median_time_seconds"]
            metric_map = {
                "min_time_seconds": "Winning Time",
                "p95_time_seconds": "Slowest Time (95th %)",
                "median_time_seconds": "Median Time"
            }
            tick_format = "%M:%S"
            y_range = None

        # Reshape
        trends_melted = trends.melt(id_vars=["event_year"],
                                    value_vars=value_vars,
                                    var_name="RawMetric", value_name="Seconds")

        # Map to friendly names
        trends_melted["Metric"] = trends_melted["RawMetric"].map(metric_map)

        # Filter outliers for Pace chart
        if metric_type == "Pace":
            trends_melted = trends_melted[trends_melted["Seconds"] <= 2700]

        # Convert seconds to datetime
        trends_melted[y_col] = pd.to_datetime(trends_melted["Seconds"], unit='s')

        # Create Label column only for "Winning" metric
        trends_melted["Label"] = trends_melted.apply(
            lambda x: x[y_col].strftime(tick_format) if "Winning" in x["Metric"] else None, axis=1
        )

        fig_trends = px.line(trends_melted, x="event_year", y=y_col, color="Metric", markers=True,
                             title=f"{metric_type} Trends Over Time", text="Label",
                             color_discrete_map={
                                 "Winning Pace": theme.PALETTE["accent2"], "Winning Time": theme.PALETTE["accent2"],
                                 "Slowest Pace (95th %)": theme.PALETTE["accent"], "Slowest Time (95th %)": theme.PALETTE["accent"],
                                 "Median Pace": theme.PALETTE["muted"], "Median Time": theme.PALETTE["muted"]
                             })

        fig_trends.update_traces(textposition="top center")

        fig_trends.update_layout(
            yaxis_tickformat=tick_format,
            xaxis_title="Event Year",
            yaxis_title=metric_type
        )
        if y_range:
            fig_trends.update_layout(yaxis_range=y_range)

        chart(fig_trends)
    elif not trends.empty:
        st.info("Upload data from multiple years to see performance trends.")

    # --- Pace Distribution ---
    section("Pace Distribution")

    dist_df = dq.get_distribution(con)

    if not dist_df.empty:
        # 1. Calculate Statistics for Context
        median_pace = dist_df["pace_minutes"].median()
        median_str = f"{int(median_pace)}:{int((median_pace*60)%60):02d}"

        # 2. Create the Chart
        fig = px.histogram(dist_df, x="pace_minutes",
                           color_discrete_sequence=[theme.PALETTE["highlight"]],
                           nbins=40)

        # 3. Add the Median Line
        fig.add_vline(x=median_pace, line_width=2, line_dash="dash", line_color=theme.PALETTE["accent"])

        # 4. Add a clean annotation for the Median
        fig.add_annotation(
            x=median_pace,
            y=1.05,
            yref="paper",
            text=f"<b>Median: {median_str}</b>",
            showarrow=False,
            font=dict(family="IBM Plex Sans", size=12, color=theme.PALETTE["accent"]),
            align="center"
        )

        # 5. Format X-Axis Ticks (Convert decimals like 8.5 to "8:30")
        min_p = int(dist_df["pace_minutes"].min())
        max_p = int(dist_df["pace_minutes"].quantile(0.99))
        tick_vals = list(range(min_p, max_p + 2, 2))
        tick_text = [f"{x}:00" for x in tick_vals]

        fig.update_layout(
            xaxis_title="Pace (min/mi)",
            yaxis_title="Runners",
            title="Runners Grouped by Pace",
            bargap=0.05,
            xaxis=dict(
                tickmode='array',
                tickvals=tick_vals,
                ticktext=tick_text,
                range=[min_p-1, max_p+1]
            )
        )

        chart(fig)

    # --- Competitiveness by Year ---
    section("Yearly Competitiveness")

    col_comp1, col_comp2 = st.columns([1, 3])

    with col_comp1:
        st.markdown("**Filters**")
        comp_gender = st.selectbox("Gender", ["All", "M", "F"], key="comp_gender")
        comp_age = st.slider("Age Range", 0, 100, (0, 100), key="comp_age")

    with col_comp2:
        comp_stats = dq.get_competitiveness_stats(con, gender=comp_gender, age_min=comp_age[0], age_max=comp_age[1])

        if not comp_stats.empty:
            # Convert seconds to datetime for proper formatting
            comp_stats["Top 3 Time"] = pd.to_datetime(comp_stats["time_top_3"], unit='s')
            comp_stats["Top 10 Time"] = pd.to_datetime(comp_stats["time_top_10"], unit='s')

            # Melt for Plotly
            comp_melted = comp_stats.melt(id_vars=["event_year"],
                                          value_vars=["Top 3 Time", "Top 10 Time"],
                                          var_name="Metric", value_name="Time")

            # Create Label for text
            comp_melted["Label"] = comp_melted["Time"].dt.strftime("%M:%S")

            fig_comp = px.line(comp_melted, x="event_year", y="Time", color="Metric", markers=True,
                               title="Time Required to Place (Top 3 vs Top 10)", text="Label",
                               color_discrete_map={
                                   "Top 3 Time": theme.PALETTE["accent"],
                                   "Top 10 Time": theme.PALETTE["accent2"]
                               })

            fig_comp.update_traces(textposition="top center")
            fig_comp.update_layout(
                yaxis_tickformat="%M:%S",
                xaxis_title="Event Year",
                yaxis_title="Finish Time"
            )

            chart(fig_comp)
        else:
            st.warning("No data found for these filters.")

    # --- Advanced Analytics ---
    section("Advanced Analytics")

    col_adv1, col_adv2 = st.columns(2)

    with col_adv1:
        st.markdown("**Division Battle Royale**")

        div_stats = dq.get_division_stats(con)
        if not div_stats.empty:
            # Highlight Most Competitive
            most_competitive = div_stats.sort_values("top_3_spread_seconds").iloc[0]
            spread = int(most_competitive["top_3_spread_seconds"])
            comp_div = most_competitive["Age_Group"]

            stat_tile("Most Competitive Division", comp_div, f"Only {spread}s separates the podium")

            st.markdown("###")

            # Depth Chart
            fig_depth = px.bar(div_stats, x="Age_Group", y="runner_count", title="Field Depth by Division",
                               color_discrete_sequence=[theme.PALETTE["accent2"]])
            fig_depth.update_layout(xaxis_title="Age Group", yaxis_title="Runner Count")
            chart(fig_depth)

            # Competitiveness Table
            st.markdown("**Top 5 Most Competitive Divisions**")
            competitive = div_stats.sort_values("top_3_spread_seconds").head(5)
            competitive["Spread"] = competitive["top_3_spread_seconds"].apply(lambda x: f"{int(x)}s" if pd.notnull(x) else "N/A")
            table(competitive[["Age_Group", "Spread"]])

    with col_adv2:
        st.markdown("**Battle of the Eras**")

        era_stats = dq.get_era_stats(con)
        if not era_stats.empty:
            # Format metrics
            era_stats["Avg Runners"] = era_stats["avg_runners_per_year"].astype(int)
            era_stats["Avg Pace"] = era_stats["avg_pace_seconds"].apply(dq.format_seconds)
            era_stats["Fastest Time"] = era_stats["fastest_time_seconds"].apply(dq.format_seconds)

            # Create Era Label (e.g., "2010-2014")
            era_stats["Era"] = era_stats["Era_Start"].apply(lambda x: f"{x}-{x+4}")

            table(era_stats[["Era", "Avg Runners", "Avg Pace", "Fastest Time"]])
        else:
            st.info("Need data from multiple decades.")

    # --- Returning Runners ---
    section("New vs returning", kicker="Who comes back")
    ret = dq.get_returning_counts(con)
    if not ret.empty and len(ret) > 1:
        melted = ret.melt(id_vars=["event_year"], value_vars=["returning_runners", "new_runners"],
                          var_name="Kind", value_name="Runners")
        melted["Kind"] = melted["Kind"].map({"returning_runners": "Returning", "new_runners": "New"})
        fig = px.bar(melted, x="event_year", y="Runners", color="Kind", barmode="stack",
                     title="Field composition by year",
                     color_discrete_map={"Returning": theme.PALETTE["ink"], "New": theme.PALETTE["highlight"]})
        fig.update_layout(xaxis_title="Edition", yaxis_title="Finishers")
        chart(fig)
        latest = ret.iloc[-1]
        share = 100.0 * latest["returning_runners"] / max(1, latest["returning_runners"] + latest["new_runners"])
        verdict(f"In {int(latest['event_year'])}, {share:.0f}% of finishers had run this race before.")
    else:
        st.info("Load at least two editions to see who comes back.")
