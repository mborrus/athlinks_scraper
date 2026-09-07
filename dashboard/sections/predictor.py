import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def ordinal_suffix(n):
    if 10 <= n % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")


def render(con):
    section("Place Predictor")

    col1, col2 = st.columns([1, 2])

    with col1:
        target_time_input = st.text_input("Target Time (MM:SS)", "25:00")
        runner_history_name = st.text_input("Show My History (Name)", placeholder="e.g. Mr. Gobble")

        st.markdown("**Filters**")
        gender_filter = st.selectbox("Gender", ["All", "M", "F"])
        age_filter = st.slider("Age Range", 0, 100, (0, 100))

        # Parse Input
        target_seconds = None
        if target_time_input:
            try:
                parts = target_time_input.split(':')
                if len(parts) == 2:
                    target_seconds = int(parts[0]) * 60 + int(parts[1])
                elif len(parts) == 3:
                    target_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                else:
                    st.error("Invalid format. Use MM:SS")
            except ValueError:
                st.error("Invalid numbers.")

    if target_seconds:
        raw_times = dq.get_raw_times(con)

        # Apply Filters
        if not raw_times.empty:
            if gender_filter != "All":
                raw_times = raw_times[raw_times["Gender"] == gender_filter]

            raw_times = raw_times[
                (raw_times["Age"] >= age_filter[0]) &
                (raw_times["Age"] <= age_filter[1])
            ]

        # Calculate avg_runners dynamically based on filtered data
        if not raw_times.empty:
            avg_runners = len(raw_times) / raw_times['event_year'].nunique()
        else:
            avg_runners = 0

        if not raw_times.empty and avg_runners > 0:
            all_seconds = raw_times['time_seconds'].sort_values()

            # --- 1. Calculate Prediction ---
            faster_count = (all_seconds < target_seconds).sum()
            total_count = len(all_seconds)
            percentile = faster_count / total_count
            predicted_place = int(percentile * avg_runners) + 1
            pct = 100 - (percentile * 100)

            # Display Prediction Card
            stat_tile("Predicted finish", f"{predicted_place}{ordinal_suffix(predicted_place)}",
                      f"Faster than {pct:.1f}% of a typical field of {int(avg_runners)}")

            # --- 2. Smart Scaling & Slider Logic ---
            abs_min_seconds = all_seconds.min()
            abs_max_seconds = all_seconds.max()

            p98_seconds = all_seconds.quantile(0.98)
            smart_max = max(p98_seconds, target_seconds)

            if runner_history_name:
                 history_df = dq.get_runner_history(con, runner_history_name)
                 if not history_df.empty:
                     smart_max = max(smart_max, history_df['time_seconds'].max())

            # --- 3. Histogram Construction ---
            hist_data = raw_times.copy()
            hist_data['bin'] = hist_data['time_seconds'] // 1 * 1
            bin_counts = hist_data['bin'].value_counts().sort_index()

            df_hist = pd.DataFrame({'Seconds': bin_counts.index, 'Count': bin_counts.values})
            df_hist['TimeStr'] = df_hist['Seconds'].apply(dq.format_seconds)

            # Highlight target bin
            df_hist['Color'] = df_hist['Seconds'].apply(
                lambda x: theme.PALETTE["accent"] if x == target_seconds else theme.PALETTE["highlight"])

            fig = px.bar(df_hist, x='Seconds', y='Count', title="Finish Time Distribution (Filtered)",
                         hover_data=['TimeStr'], color='Color', color_discrete_map="identity")

            # Add vertical line for target
            fig.add_vline(x=target_seconds, line_width=3, line_dash="solid", line_color=theme.PALETTE["accent"],
                          annotation_text="Target", annotation_position="top right")

            # --- 4. Historical Lines Logic ---
            if runner_history_name:
                history_df = dq.get_runner_history(con, runner_history_name)
                if not history_df.empty:
                    history_sorted = history_df.sort_values("time_seconds")
                    y_positions = [1.02, 0.92, 0.82]

                    for i, (_, row) in enumerate(history_sorted.iterrows()):
                        t_sec = row['time_seconds']
                        try:
                            year = str(row['Event Date'])[:4]
                        except Exception:
                            year = "?"

                        fig.add_vline(x=t_sec, line_width=1, line_dash="dot", line_color=theme.PALETTE["ink"], opacity=0.6)

                        y_pos = y_positions[i % len(y_positions)]
                        fig.add_annotation(
                            x=t_sec, y=y_pos, yref="paper", text=f"<b>{year}</b>",
                            showarrow=False, font=dict(family="IBM Plex Sans", size=10, color=theme.PALETTE["ink"]),
                            bgcolor="rgba(255, 255, 255, 0.8)", borderpad=2
                        )

            with col2:
                # Convert to minutes for the slider
                min_min = int(abs_min_seconds // 60)
                max_min = int(abs_max_seconds // 60) + 1
                default_max_min = int(smart_max // 60) + 1

                # Double-ended slider
                slider_range = st.slider(
                    "Zoom to specific finish times (Minutes):",
                    min_value=min_min,
                    max_value=max_min,
                    value=(min_min, default_max_min),
                    step=1
                )

                # Convert slider back to seconds for the chart
                final_min_sec = slider_range[0] * 60
                final_max_sec = slider_range[1] * 60

                # Update Layout with Slider Values
                tick_vals = list(range(900, int(final_max_sec) + 300, 300))
                tick_text = [f"{int(x//60)}:00" for x in tick_vals]

                fig.update_layout(
                    xaxis_title="Finish Time (Seconds)",
                    yaxis_title="Runner Count",
                    bargap=0,
                    xaxis_range=[final_min_sec, final_max_sec]
                )
                fig.update_xaxes(tickmode='array', tickvals=tick_vals, ticktext=tick_text)

                chart(fig)
        else:
            st.warning("Not enough data to predict.")
