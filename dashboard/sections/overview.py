import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def render(con):
    stats = dq.get_overview_stats(con)
    if stats.empty:
        st.info("No results loaded for this race yet.")
        return

    total = int(stats["total_runners"][0])
    fastest = stats["fastest_time"][0]
    fastest_runner = stats["fastest_runner"][0]
    fastest_year = stats["fastest_year"][0]
    first_year = stats["first_year"][0]
    slowest = stats["slowest_time"][0]
    avg_pace = dq.format_seconds(stats["avg_pace_seconds"][0])

    since = f"since {int(first_year)}" if pd.notna(first_year) else "over the years"
    record_note = (f"Set in {int(fastest_year)} by {fastest_runner}" if pd.notna(fastest_year)
                   else f"Held by {fastest_runner}")

    section("By the numbers", kicker="Race overview")
    stat_row([
        ("Finishers", f"{total:,}", f"All editions {since}"),
        ("Average pace", f"{avg_pace} /mi", "Across every finisher"),
        ("Course record", fastest, record_note),
        ("Last across the line", slowest, "Every finisher counts"),
    ])
