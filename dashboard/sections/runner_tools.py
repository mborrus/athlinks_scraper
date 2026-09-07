import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict


def render(con):
    # --- Runner Lookup ---
    section("Runner Lookup")

    runner_name = st.text_input("Search for a Runner by Name")
    if runner_name:
        history = dq.get_runner_history(con, runner_name)
        if not history.empty:
            st.success(f"Found {len(history)} results for '{runner_name}'")
            table(history)
        else:
            st.warning(f"No results found for '{runner_name}'")

    # --- Nemesis Finder ---
    section("Nemesis Finder (Rivalry Tracker)")

    rival_search_name = st.text_input("Enter Your Name for Rivalry Check")
    if rival_search_name:
        rivals = dq.get_nemesis(con, rival_search_name)
        if not rivals.empty:
            rivals["Avg Time Diff"] = rivals["Avg_Time_Diff_Seconds"].apply(
                lambda x: f"{'+' if x > 0 else ''}{int(x // 60)}:{int(abs(x) % 60):02d}"
            )
            table(rivals[["Rival", "HeadToHead_Count", "Avg Time Diff"]])
        else:
            st.info("No multi-race rivalries found. Keep racing!")

    # --- Head to Head ---
    section("Head to head", kicker="Settle it")
    col_a, col_b = st.columns(2)
    with col_a:
        name_a = st.text_input("Runner A", key="h2h_a", placeholder="e.g. Alice Fast")
    with col_b:
        name_b = st.text_input("Runner B", key="h2h_b", placeholder="e.g. Bob Mid")
    if name_a and name_b:
        a_norm, b_norm = name_a.strip().upper(), name_b.strip().upper()
        h2h = dq.get_head_to_head(con, a_norm, b_norm)
        if h2h.empty:
            st.info("These two have never finished the same edition.")
        else:
            wins_a = int((h2h["diff_seconds"] < 0).sum())
            wins_b = int((h2h["diff_seconds"] > 0).sum())
            avg_margin = dq.format_seconds(abs(h2h["diff_seconds"].mean()))
            leader = name_a if wins_a >= wins_b else name_b
            verdict(f"{leader} leads {max(wins_a, wins_b)}–{min(wins_a, wins_b)}; average margin {avg_margin}.")
            shown = h2h.rename(columns={"event_year": "Year", "time_a": name_a, "time_b": name_b})
            shown["Margin"] = h2h["diff_seconds"].apply(
                lambda d: ("+" if d > 0 else "-") + dq.format_seconds(abs(d)))
            table(shown[["Year", name_a, name_b, "Margin"]])

    # --- Pace Partners ---
    section("Find Your Pace Partners")

    col1, col2 = st.columns([1, 3])
    with col1:
        search_type = st.radio("Search by:", ["Pace", "Finish Time"])
        label = "Target Pace (MM:SS)" if search_type == "Pace" else "Target Time (HH:MM:SS)"
        default_val = "08:00" if search_type == "Pace" else "25:00"

        target_input = st.text_input(label, default_val, placeholder="e.g. 20:00")
        tolerance = st.slider("Tolerance (seconds)", 5, 60, 15)

    with col2:
        if target_input:
            try:
                partners = dq.get_pace_partners(con, target_input, tolerance, search_type)
                if not partners.empty:
                    table(partners)
                else:
                    st.warning("No runners found within that range.")
            except Exception:
                st.error(f"Invalid format. Please use {label.split('(')[1][:-1]}")
