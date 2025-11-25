import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Ensure athlinks_scraper is importable
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'athlinks_scraper_project'))

from athlinks_scraper.core import get_results, extract_master_id, extract_event_id, fetch_master_events, fetch_metadata
from athlinks_scraper.core import get_results, extract_master_id, extract_event_id, fetch_master_events, fetch_metadata
from dashboard_queries import init_db, get_event_names, create_enriched_view, get_overview_stats, get_pace_partners, get_fun_stats, get_distribution, get_trends, get_runner_history, get_nemesis, get_retention_data, get_fastest_by_year, get_fastest_by_demographics, get_division_stats, get_era_stats, get_raw_times, get_avg_annual_runners
import plotly.graph_objects as go

st.set_page_config(page_title="Athlinks Race Analytics", layout="wide")

# --- Custom CSS for Editorial Vibe ---
st.markdown("""
    <style>
    /* Import fonts: Lora for headers, Inter for body */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&family=Lora:wght@500;600;700&display=swap');
    
    /* Headers get the Serif treatment */
    h1, h2, h3, h4, h5, h6, .stHeading {
        font-family: 'Lora', serif !important;
        color: #111827;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    
    /* Body and Data get Sans-Serif */
    html, body, p, div, span, button, input, select, textarea, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Magazine Card Container */
    .magazine-card {
        background-color: #FFFFFF;
        padding: 20px;
        text-align: left;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        border: 1px solid #E5E7EB;
        margin-bottom: 1rem;
        transition: transform 0.2s;
    }
    
    .magazine-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    /* Typography inside Card */
    .magazine-label {
        font-family: 'Inter', sans-serif;
        color: #6B7280; /* Muted Grey */
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    
    .magazine-value {
        font-family: 'Lora', serif;
        color: #111827; /* Dark Grey */
        font-size: 2.5rem;
        font-weight: 700;
        line-height: 1.1;
        margin-bottom: 0.5rem;
    }
    
    .magazine-caption {
        font-family: 'Lora', serif;
        font-size: 0.9rem;
        font-style: italic;
        color: #4B5563;
        margin-top: 10px;
        padding-top: 10px;
        border-top: 1px solid #F3F4F6;
        line-height: 1.4;
    }
    </style>
""", unsafe_allow_html=True)

# --- Helper Function for Magazine Cards ---
def display_magazine_card(label, value, caption, color_stripe="#111827"):
    st.markdown(f"""
    <div class="magazine-card" style="border-top: 4px solid {color_stripe};">
        <div class="magazine-label">{label}</div>
        <div class="magazine-value">{value}</div>
        <div class="magazine-caption">{caption}</div>
    </div>
    """, unsafe_allow_html=True)

# --- Chart Style Helper ---
def style_chart(fig):
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_family="Inter",
        title_font_family="Lora",
        title_font_size=20,
        xaxis=dict(
            showgrid=False, 
            showline=True, 
            linecolor='#333', 
            zeroline=False,
            title_font_family="Inter",
            fixedrange=True
        ),
        yaxis=dict(
            showgrid=True, 
            gridcolor='#E5E7EB', 
            gridwidth=0.5,
            zeroline=False,
            title_font_family="Inter",
            fixedrange=True
        ),
        colorway=["#111827", "#EF4444", "#3B82F6", "#10B981", "#F59E0B"],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        dragmode=False
    )
    return fig

def display_chart(fig):
    fig = style_chart(fig)
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False, 'scrollZoom': False})

# --- Sidebar ---
with st.sidebar:
    st.header("Data Management")
    uploaded_files = st.file_uploader("Upload CSV Results", accept_multiple_files=True, type="csv")
    
    st.divider()
    
    st.subheader("Scrape Historical Data")
    st.markdown("""
    <div style="font-size: 0.85rem; color: #6B7280; margin-bottom: 10px;">
    <strong>How to use:</strong><br>
    1. Go to <a href="https://www.athlinks.com" target="_blank">Athlinks.com</a> and search for your event.<br>
    2. Click on the event to view the "Master" page (listing all years).<br>
    3. Copy the URL (e.g., <code>https://www.athlinks.com/event/15776</code>).<br>
    4. Paste it below to scrape all available years.
    </div>
    """, unsafe_allow_html=True)
    # Initialize session state
    if "master_url_input" not in st.session_state:
        st.session_state.master_url_input = ""
    if "trigger_scrape" not in st.session_state:
        st.session_state.trigger_scrape = False

    def set_branford_url():
        st.session_state.master_url_input = "https://www.athlinks.com/event/15776"
        st.session_state.trigger_scrape = True

    st.button("Test with Branford Turkey Trot", on_click=set_branford_url)
    
    master_url = st.text_input("Master Event URL", key="master_url_input", placeholder="https://www.athlinks.com/event/15776")
    
    if st.button("Scrape All Years") or st.session_state.trigger_scrape:
        # Reset trigger
        st.session_state.trigger_scrape = False
        
        if master_url:
            with st.spinner("Fetching metadata..."):
                try:
                    # 1. Extract Master ID
                    master_id = extract_master_id(master_url)
                    if not master_id:
                        # Fallback: Try to get event ID and fetch metadata
                        event_id = extract_event_id(master_url)
                        if event_id:
                            meta = fetch_metadata(event_id)
                            master_id = meta.get('masterId')
                    
                    if master_id:
                        st.info(f"Found Master ID: {master_id}. Fetching events...")
                        events = fetch_master_events(master_id)
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        for i, event in enumerate(events):
                            year = event['date_str'][:4]
                            event_id = event['id']
                            status_text.text(f"Scraping {year}...")
                            
                            df = get_results(event_id)
                            if not df.empty:
                                # Save to data/
                                filename = os.path.join(os.path.dirname(__file__), "data", f"scraped_{master_id}_{year}.parquet")
                                os.makedirs(os.path.dirname(filename), exist_ok=True)
                                df.to_parquet(filename, index=False)
                            
                            progress_bar.progress((i + 1) / len(events))
                        
                        st.success("Scraping Complete! Refreshing...")
                        st.rerun()
                    else:
                        st.error("Could not extract Master ID from URL.")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Please enter a URL.")

# Check for local data
has_local_data = False
# Check if we have data
data_dir = os.path.join(os.path.dirname(__file__), "data")
if os.path.exists(data_dir) and os.listdir(data_dir):
    has_local_data = True

if not uploaded_files and not has_local_data:
    st.info("Please upload race result CSV files or scrape a Master Event to begin.")
    st.stop()

# Initialize Database
con = init_db(uploaded_files)

# Get available events
events = get_event_names(con)
selected_master_id = None

if events:
    st.sidebar.divider()
    st.sidebar.header("Select Event")
    
    # Create mapping for selectbox
    event_map = {e['display_name']: e['master_id'] for e in events}
    event_options = list(event_map.keys())
    
    # Default to the most recent event (or first in list) if available
    selected_name = st.sidebar.selectbox("Choose Race", event_options, index=0)
    selected_master_id = event_map[selected_name]

# Create the view based on selection
create_enriched_view(con, selected_master_id)

# --- Hero Header ---
with st.container():
    st.title("Turkey Trot Analytics")
    st.markdown("*A Moneyball approach to your local 5k: Dive deep into race history, find your rivals, and track the field.*")
    st.divider()

# --- Main Layout ---
tab1, tab2, tab3, tab4 = st.tabs(["Analytics & Trends", "Runner Tools", "Hall of Fame", "Place Predictor"])

with tab1:
    # --- Overview Stats ---
    st.header("Race Overview")
    stats = get_overview_stats(con)

    if not stats.empty:
        # Extract values safely
        total = stats["total_runners"][0]
        fastest = stats["fastest_time"][0]
        fastest_runner = stats["fastest_runner"][0]
        slowest = stats["slowest_time"][0]
        
        # Calculate Pace format
        avg_pace_sec = stats["avg_pace_seconds"][0]
        if pd.notna(avg_pace_sec):
            avg_min = int(avg_pace_sec // 60)
            avg_sec = int(avg_pace_sec % 60)
            avg_pace_fmt = f"{avg_min}:{avg_sec:02d} /mi"
        else:
            avg_pace_fmt = "N/A"

        # Narrative Context
        st.markdown("""
        <div style="font-family: 'Lora', serif; font-size: 1.1rem; line-height: 1.6; color: #374151; margin-bottom: 2rem;">
        The Turkey Trot has evolved from a local gathering to a competitive regional event. 
        While participation has surged, the core spirit of the race remains grounded in community tradition.
        </div>
        """, unsafe_allow_html=True)

        # LAYOUT: Magazine Cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            display_magazine_card("Total Runners", f"{total:,}", "A growing tradition since 2010", "#2563EB")
        with col2:
            display_magazine_card("Average Pace", avg_pace_fmt, "Steady pace despite growth", "#10B981")
        with col3:
            display_magazine_card("Fastest Time", fastest, f"Course record set in 2019 by {fastest_runner}", "#F59E0B")
        with col4:
            display_magazine_card("Slowest Time", slowest, "Every finisher counts", "#EF4444")
            
        st.divider()

    # --- Performance Trends ---
    st.header("Performance Trends")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1.5rem;">
    While elite runners push the pace, the median finish time reflects a widening field of participants.
    </div>
    """, unsafe_allow_html=True)
    
    trends = get_trends(con)
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
            tick_format = "%M:%S"
            # Filter out extreme outliers (> 45 min/mile) for better auto-ranging
            # 45 mins = 2700 seconds
            y_range = None
        else:
            y_col = "Finish Time"
            value_vars = ["min_time_seconds", "p95_time_seconds", "median_time_seconds"]
            metric_map = {
                "min_time_seconds": "Winning Time",
                "p95_time_seconds": "Slowest Time (95th %)",
                "median_time_seconds": "Median Time"
            }
            # Auto format for Time (HH:MM:SS usually)
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
                                 "Winning Pace": "#2563EB", "Winning Time": "#2563EB",
                                 "Slowest Pace (95th %)": "#EF4444", "Slowest Time (95th %)": "#EF4444",
                                 "Median Pace": "#9CA3AF", "Median Time": "#9CA3AF"
                             })
        
        fig_trends.update_traces(textposition="top center")
        
        fig_trends.update_layout(
            yaxis_tickformat=tick_format,
            xaxis_title="Event Year",
            yaxis_title=metric_type
        )
        if y_range:
            fig_trends.update_layout(yaxis_range=y_range)
        
        display_chart(fig_trends)
    elif not trends.empty:
        st.info("Upload data from multiple years to see performance trends.")

    # --- Pace Distribution ---
    st.subheader("Pace Distribution")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1rem;">
    The distribution of finish times reveals the 'heart of the pack'. The vertical line marks the median—the exact middle of the field.
    </div>
    """, unsafe_allow_html=True)
    
    dist_df = get_distribution(con)
    
    if not dist_df.empty:
        # 1. Calculate Statistics for Context
        median_pace = dist_df["pace_minutes"].median()
        median_str = f"{int(median_pace)}:{int((median_pace*60)%60):02d}"
        
        # 2. Create the Chart (Using Editorial Navy)
        fig = px.histogram(dist_df, x="pace_minutes", 
                           color_discrete_sequence=["#e09451"], # Navy
                           nbins=40) # Slightly finer grain than just 1-min bins
        
        # 3. Add the Median Line (Burnt Orange)
        fig.add_vline(x=median_pace, line_width=2, line_dash="dash", line_color="#C2410C")
        
        # 4. Add a clean annotation for the Median
        fig.add_annotation(
            x=median_pace, 
            y=1.05, # Position slightly above the plot area
            yref="paper",
            text=f"<b>Median: {median_str}</b>",
            showarrow=False,
            font=dict(family="Inter", size=12, color="#C2410C"),
            align="center"
        )

        # 5. Format X-Axis Ticks (Convert decimals like 8.5 to "8:30")
        # Generate ticks every 2 minutes for readability
        min_p = int(dist_df["pace_minutes"].min())
        max_p = int(dist_df["pace_minutes"].quantile(0.99)) # Cut off extreme walkers for view
        tick_vals = list(range(min_p, max_p + 2, 2))
        tick_text = [f"{x}:00" for x in tick_vals]

        fig.update_layout(
            xaxis_title="Pace (min/mi)", 
            yaxis_title="Runners",
            title="Runners Grouped by Pace",
            bargap=0.05, # Tiny gap between bars looks cleaner
            xaxis=dict(
                tickmode='array',
                tickvals=tick_vals,
                ticktext=tick_text,
                range=[min_p-1, max_p+1] # Focus on the main pack
            )
        )
        
        # Apply the global style helper you defined earlier
        fig = style_chart(fig)
        st.plotly_chart(fig, use_container_width=True)



    # --- Advanced Analytics ---
    st.header("Advanced Analytics")
    
    col_adv1, col_adv2 = st.columns(2)
    
    with col_adv1:
        st.subheader("Division Battle Royale")
        st.markdown("""
        <div style="font-family: 'Lora', serif; font-size: 0.9rem; font-style: italic; color: #4B5563; margin-bottom: 1rem;">
        Which age groups are the deepest and most competitive? A look at field depth and podium spreads.
        </div>
        """, unsafe_allow_html=True)
        
        div_stats = get_division_stats(con)
        if not div_stats.empty:
            # Highlight Most Competitive
            most_competitive = div_stats.sort_values("top_3_spread_seconds").iloc[0]
            spread = int(most_competitive["top_3_spread_seconds"])
            comp_div = most_competitive["Age_Group"]
            
            display_magazine_card("Most Competitive Division", comp_div, f"Only {spread}s separates the podium", "#EA580C")
            
            st.markdown("###")
            
            # Depth Chart
            fig_depth = px.bar(div_stats, x="Age_Group", y="runner_count", title="Field Depth by Division",
                               color_discrete_sequence=["#2563EB"])
            fig_depth.update_layout(xaxis_title="Age Group", yaxis_title="Runner Count")
            fig_depth.update_layout(xaxis_title="Age Group", yaxis_title="Runner Count")
            display_chart(fig_depth)
            
            # Competitiveness Table
            st.markdown("**Top 5 Most Competitive Divisions**")
            competitive = div_stats.sort_values("top_3_spread_seconds").head(5)
            competitive["Spread"] = competitive["top_3_spread_seconds"].apply(lambda x: f"{int(x)}s" if pd.notnull(x) else "N/A")
            st.dataframe(competitive[["Age_Group", "Spread"]], use_container_width=True)
    
    with col_adv2:
        st.subheader("Battle of the Eras")
        st.markdown("""
        <div style="font-family: 'Lora', serif; font-size: 0.9rem; font-style: italic; color: #4B5563; margin-bottom: 1rem;">
        Comparing race dynamics across 5-year eras. Are we getting faster or just more popular?
        </div>
        """, unsafe_allow_html=True)
        
        era_stats = get_era_stats(con)
        if not era_stats.empty:
            # Format metrics
            era_stats["Avg Runners"] = era_stats["avg_runners_per_year"].astype(int)
            era_stats["Avg Pace"] = era_stats["avg_pace_seconds"].apply(lambda x: f"{int(x//60)}:{int(x%60):02d}")
            era_stats["Fastest Time"] = era_stats["fastest_time_seconds"].apply(lambda x: f"{int(x//60)}:{int(x%60):02d}")
            
            # Create Era Label (e.g., "2010-2014")
            era_stats["Era"] = era_stats["Era_Start"].apply(lambda x: f"{x}-{x+4}")
            
            st.dataframe(era_stats[["Era", "Avg Runners", "Avg Pace", "Fastest Time"]], use_container_width=True)
        else:
            st.info("Need data from multiple decades.")

with tab2:
    # --- Runner Lookup ---
    st.header("Runner Lookup")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1.5rem;">
    Explore the archives. Search for any runner to see their complete history in this event.
    </div>
    """, unsafe_allow_html=True)
    
    runner_name = st.text_input("Search for a Runner by Name")
    if runner_name:
        history = get_runner_history(con, runner_name)
        if not history.empty:
            st.success(f"Found {len(history)} results for '{runner_name}'")
            st.dataframe(history, use_container_width=True)
        else:
            st.warning(f"No results found for '{runner_name}'")

    st.divider()

    # --- Nemesis Finder ---
    st.header("Nemesis Finder (Rivalry Tracker)")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1.5rem;">
    Rivalries fuel competition. Find runners who have raced against you multiple times and see how you stack up.
    </div>
    """, unsafe_allow_html=True)
    
    rival_search_name = st.text_input("Enter Your Name for Rivalry Check")
    if rival_search_name:
        rivals = get_nemesis(con, rival_search_name)
        if not rivals.empty:
            # Format Avg Time Diff
            rivals["Avg Time Diff"] = rivals["Avg_Time_Diff_Seconds"].apply(
                lambda x: f"{'+' if x > 0 else ''}{int(x // 60)}:{int(abs(x) % 60):02d}"
            )
            st.dataframe(rivals[["Rival", "HeadToHead_Count", "Avg Time Diff"]], use_container_width=True)
        else:
            st.info("No multi-race rivalries found. Keep racing!")

    st.divider()

    # --- Pace Partners ---
    st.header("Find Your Pace Partners")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1.5rem;">
    Find your pack. Identify runners who finish near your target time to work together in future races.
    </div>
    """, unsafe_allow_html=True)

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
                partners = get_pace_partners(con, target_input, tolerance, search_type)
                if not partners.empty:
                    st.dataframe(partners, use_container_width=True)
                else:
                    st.warning("No runners found within that range.")
            except Exception:
                st.error(f"Invalid format. Please use {label.split('(')[1][:-1]}")

with tab3:
    st.header("Hall of Fame")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 2rem;">
    Celebrating the fastest runners in the history of this event (5K Only).
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Fastest Overall by Year")
        fastest_year = get_fastest_by_year(con)
        if not fastest_year.empty:
            st.dataframe(fastest_year[["event_year", "Name", "Time", "Pace"]], use_container_width=True)
        else:
            st.info("No data available.")
            
    with col2:
        st.subheader("All-Time Records by Age Group")
        fastest_demo = get_fastest_by_demographics(con)
        if not fastest_demo.empty:
            st.dataframe(fastest_demo[["Gender", "Age_Group", "Name", "Time", "event_year"]], use_container_width=True)
        else:
            st.info("No data available.")

    st.divider()
    
    # --- Fun Stats ---
    st.header("Fun Stats")
    
    st.subheader("Frequent Flyers (Most Races)")
    hof = get_fun_stats(con)
    if not hof.empty:
        hof = hof.rename(columns={"race_count": "Races Run", "best_pace": "Best Pace"})
        st.dataframe(hof, use_container_width=True)
    else:
        st.info("Upload multiple race files to see who runs the most!")

with tab4:
    st.header("Place Predictor")
    st.markdown("""
    <div style="font-family: 'Lora', serif; font-size: 1rem; font-style: italic; color: #4B5563; margin-bottom: 1.5rem;">
    Enter your target finish time to see where you'd stack up in a typical year.
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 2])
    
    with col1:
        target_time_input = st.text_input("Target Time (MM:SS)", "25:00")
        runner_history_name = st.text_input("Show My History (Name)", placeholder="e.g. Mr. Gobble")
        
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
        raw_times = get_raw_times(con)
        avg_runners = get_avg_annual_runners(con)
        
        if not raw_times.empty and avg_runners > 0:
            all_seconds = raw_times['time_seconds'].sort_values()
            
            # --- 1. Calculate Prediction ---
            faster_count = (all_seconds < target_seconds).sum()
            total_count = len(all_seconds)
            percentile = faster_count / total_count
            predicted_place = int(percentile * avg_runners) + 1 
            
            # Display Prediction Card
            st.markdown(f"""
            <div style="background-color: #F3F4F6; padding: 20px; border-radius: 8px; border-left: 5px solid #EA580C; margin-bottom: 20px;">
                <div style="font-family: 'Inter', sans-serif; color: #6B7280; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px;">Predicted Finish</div>
                <div style="font-family: 'Lora', serif; color: #111827; font-size: 2.5rem; font-weight: 700;">{predicted_place}<span style="font-size: 1.5rem; vertical-align: super;">th</span> Place</div>
                <div style="font-family: 'Inter', sans-serif; color: #4B5563; font-size: 1rem; margin-top: 5px;">
                    You would be faster than <strong>{100 - (percentile * 100):.1f}%</strong> of runners in a typical field of {int(avg_runners)}.
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # --- 2. Smart Scaling & Slider Logic ---
            # Calculate absolute data limits
            abs_min_seconds = all_seconds.min()
            abs_max_seconds = all_seconds.max()
            
            # Calculate "Smart" defaults (98th percentile or target)
            p98_seconds = all_seconds.quantile(0.98)
            smart_max = max(p98_seconds, target_seconds)
            
            # Check history to expand smart defaults if needed
            if runner_history_name:
                 history_df = get_runner_history(con, runner_history_name)
                 if not history_df.empty:
                     smart_max = max(smart_max, history_df['time_seconds'].max())

            # --- 3. Histogram Construction ---
            hist_data = raw_times.copy()
            hist_data['bin'] = hist_data['time_seconds'] // 1 * 1 
            bin_counts = hist_data['bin'].value_counts().sort_index()
            
            df_hist = pd.DataFrame({'Seconds': bin_counts.index, 'Count': bin_counts.values})
            df_hist['TimeStr'] = df_hist['Seconds'].apply(lambda x: f"{int(x//60)}:{int(x%60):02d}")
            
            # Highlight target bin
            df_hist['Color'] = df_hist['Seconds'].apply(lambda x: '#EA580C' if x == target_seconds else '#e09451')
            
            fig = px.bar(df_hist, x='Seconds', y='Count', title="Finish Time Distribution",
                         hover_data=['TimeStr'], color='Color', color_discrete_map="identity")
            
            # Add vertical line for target
            fig.add_vline(x=target_seconds, line_width=3, line_dash="solid", line_color="#EA580C", 
                          annotation_text="Target", annotation_position="top right")
            
            # --- 4. Historical Lines Logic ---
            if runner_history_name:
                history_df = get_runner_history(con, runner_history_name)
                if not history_df.empty:
                    history_sorted = history_df.sort_values("time_seconds")
                    y_positions = [1.02, 0.92, 0.82] 
                    
                    for i, (_, row) in enumerate(history_sorted.iterrows()):
                        t_sec = row['time_seconds']
                        try:
                            year = str(row['Event Date'])[:4]
                        except:
                            year = "?"

                        fig.add_vline(x=t_sec, line_width=1, line_dash="dot", line_color="#111827", opacity=0.6)
                        
                        y_pos = y_positions[i % len(y_positions)]
                        fig.add_annotation(
                            x=t_sec, y=y_pos, yref="paper", text=f"<b>{year}</b>",
                            showarrow=False, font=dict(family="Inter", size=10, color="#111827"),
                            bgcolor="rgba(255, 255, 255, 0.8)", borderpad=2
                        )

            with col2:
                # --- INSERT SLIDER HERE ---
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

                # Render Chart
                fig = style_chart(fig) 
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Not enough data to predict.")