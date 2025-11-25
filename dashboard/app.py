import streamlit as st
import pandas as pd
import plotly.express as px
from queries import init_db, get_overview_stats, get_pace_partners, get_fun_stats, get_distribution, get_trends, get_runner_history

st.set_page_config(page_title="Athlinks Race Analytics", layout="wide")

st.title("🏃‍♂️ Athlinks Race Analytics Dashboard")

# Sidebar for Data Loading
st.sidebar.header("Data Loading")
uploaded_files = st.sidebar.file_uploader("Upload Race CSVs", accept_multiple_files=True, type="csv")

if not uploaded_files:
    st.info("Please upload one or more race result CSV files to begin.")
    st.stop()

# Initialize Database
con = init_db(uploaded_files)

# --- Overview Section ---
st.header("📊 Race Overview")
stats = get_overview_stats(con)

if not stats.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Runners", stats["total_runners"][0])
    
    avg_pace_sec = stats["avg_pace_seconds"][0]
    avg_min = int(avg_pace_sec // 60)
    avg_sec = int(avg_pace_sec % 60)
    col2.metric("Average Pace", f"{avg_min}:{avg_sec:02d} /mi")
    
    col3.metric("Fastest Time", stats["fastest_time"][0])
    col4.metric("Slowest Time", stats["slowest_time"][0])

# --- Performance Trends ---
st.header("📈 Performance Trends")
trends = get_trends(con)
if not trends.empty and len(trends) > 1:
    st.markdown("Pace trends over the years (Min, Max, and Median).")
    
    # Reshape for plotting
    trends_melted = trends.melt(id_vars=["event_year"], 
                                value_vars=["min_pace_min", "max_pace_min", "median_pace_min"],
                                var_name="Metric", value_name="Pace (min/mi)")
    
    fig_trends = px.line(trends_melted, x="event_year", y="Pace (min/mi)", color="Metric", markers=True,
                         title="Pace Trends Over Time")
    st.plotly_chart(fig_trends, use_container_width=True)
elif not trends.empty:
    st.info("Upload data from multiple years to see performance trends.")

# --- Pace Distribution ---
st.subheader("Pace Distribution")
dist_df = get_distribution(con)
if not dist_df.empty:
    fig = px.histogram(dist_df, x="pace_minutes", nbins=30, title="Distribution of Pace (min/mile)")
    fig.update_layout(xaxis_title="Pace (min/mi)", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

# --- Runner Lookup ---
st.header("🔍 Runner Lookup")
runner_name = st.text_input("Search for a Runner by Name")
if runner_name:
    history = get_runner_history(con, runner_name)
    if not history.empty:
        st.success(f"Found {len(history)} results for '{runner_name}'")
        st.dataframe(history, use_container_width=True)
    else:
        st.warning(f"No results found for '{runner_name}'")

# --- Pace Partners ---
st.header("🤝 Find Your Pace Partners")
st.markdown("Enter your target pace to find runners who finish near you. Watch out for them next year!")

col1, col2 = st.columns([1, 3])
with col1:
    target_pace = st.text_input("Target Pace (MM:SS)", "08:00")
    tolerance = st.slider("Tolerance (seconds)", 5, 60, 15)

with col2:
    if target_pace:
        try:
            partners = get_pace_partners(con, target_pace, tolerance)
            if not partners.empty:
                st.dataframe(partners, use_container_width=True)
            else:
                st.warning("No runners found within that range.")
        except Exception:
            st.error("Invalid pace format. Please use MM:SS (e.g., 08:30)")

# --- Fun Stats ---
st.header("🏆 Hall of Fame & Fun Stats")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Frequent Flyers (Most Races)")
    hof = get_fun_stats(con)
    if not hof.empty:
        st.dataframe(hof, use_container_width=True)
    else:
        st.info("Upload multiple race files to see who runs the most!")

with col2:
    st.subheader("Raw Data Preview")
    st.dataframe(con.execute("SELECT * FROM results LIMIT 100").df(), use_container_width=True)
