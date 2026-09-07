import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def render(con):
    section("Runner report card", kicker="Your history in this race")
    fragment = st.text_input("Find a runner", placeholder="Start typing a name…", key="rc_fragment")
    if not fragment:
        st.info("Type part of a name to pull a runner's card.")
        return

    matches = dq.search_runner_names(con, fragment.strip())
    if not matches:
        st.warning("No finisher matches that name.")
        return
    name = st.selectbox("Pick the runner", matches, key="rc_pick")

    yearly = dq.get_runner_yearly(con, name)
    if yearly.empty:
        st.warning("That runner has no finishes in the primary race type.")
        return

    best_idx = yearly["time_seconds"].idxmin()
    stat_row([
        ("Editions run", str(len(yearly)), f"{int(yearly['event_year'].min())}–{int(yearly['event_year'].max())}"),
        ("Best time", yearly.loc[best_idx, "Time"], f"in {int(yearly.loc[best_idx, 'event_year'])}"),
        ("Best place", f"{int(yearly['place'].min())}", "overall"),
        ("Field beaten", f"{yearly['pct_beaten'].mean():.0f}%", "average across editions"),
    ])

    plot = yearly.copy()
    plot["Runner"] = pd.to_datetime(plot["time_seconds"], unit="s")
    plot["Field median"] = pd.to_datetime(plot["median_seconds"], unit="s")
    # value_name can't be "Time" here: plot still carries the original "Time"
    # string column (used above and in the table below), and pandas rejects
    # a value_name that collides with any existing column on the frame.
    melted = plot.melt(id_vars=["event_year"], value_vars=["Runner", "Field median"],
                       var_name="Series", value_name="Finish Time")
    fig = px.line(melted, x="event_year", y="Finish Time", color="Series", markers=True,
                  title=f"{name.title()} vs the field",
                  color_discrete_map={"Runner": theme.PALETTE["accent"], "Field median": theme.PALETTE["muted"]})
    fig.update_layout(yaxis_tickformat="%M:%S", xaxis_title="Edition", yaxis_title="Finish time")
    chart(fig)

    shown = yearly.rename(columns={"event_year": "Year", "place": "Place", "field_size": "Field",
                                   "pct_beaten": "% beaten"})
    table(shown[["Year", "Time", "Pace", "Place", "Field", "% beaten"]])

    section("Rivals", kicker="Seen them before?")
    rivals = dq.get_nemesis(con, name)
    if rivals.empty:
        st.info("No repeat rivals yet.")
    else:
        rivals["Avg margin"] = rivals["Avg_Time_Diff_Seconds"].apply(
            lambda x: ("+" if x > 0 else "-") + dq.format_seconds(abs(x)))
        table(rivals.rename(columns={"HeadToHead_Count": "Shared editions"})[["Rival", "Shared editions", "Avg margin"]])
