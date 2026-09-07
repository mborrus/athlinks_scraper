import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def render(con):
    section("Hall of Fame")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Fastest Overall by Year**")
        fastest_year = dq.get_fastest_by_year(con)
        if not fastest_year.empty:
            table(fastest_year[["event_year", "Name", "Time", "Pace"]])
        else:
            st.info("No data available.")

    with col2:
        st.markdown("**All-Time Records by Age Group**")
        fastest_demo = dq.get_fastest_by_demographics(con)
        if not fastest_demo.empty:
            table(fastest_demo[["Gender", "Age_Group", "Name", "Time", "event_year"]])
        else:
            st.info("No data available.")

    # --- Fun Stats ---
    section("Fun Stats")

    st.markdown("**Frequent Flyers (Most Races)**")
    hof = dq.get_fun_stats(con)
    if not hof.empty:
        hof = hof.rename(columns={"race_count": "Races Run", "best_pace": "Best Pace"})
        table(hof)
    else:
        st.info("Upload multiple race files to see who runs the most!")
