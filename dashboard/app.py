import os
import sys

import pandas as pd
import streamlit as st

# Make the scraper package and this folder importable regardless of cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "athlinks_scraper_project"))
sys.path.insert(0, HERE)

import dashboard_queries as dq  # noqa: E402
from athlinks_scraper.providers import detect_provider  # noqa: E402
from sections import hall_of_fame, overview, predictor, report_card, runner_tools, trends  # noqa: E402
from ui import theme  # noqa: E402
from ui.components import masthead  # noqa: E402

st.set_page_config(page_title="Turkey Trot Results Program", layout="wide")
theme.inject()

DATA_DIR = os.path.join(HERE, "data")

EXAMPLES = {
    "Branford Turkey Trot (Athlinks)": "https://www.athlinks.com/event/15776",
    "NYRR Frosty 5K (NYRR)": "https://results.nyrr.org/event/24FROSTY/finishers",
    "#RUNMARANA Turkey Trot (RunSignup)": "https://runsignup.com/Race/Results/100692",
}


def save_event_frame(df, ref):
    year = ref.date_str[:4] if ref.date_str and ref.date_str != "Unknown" else "unknown"
    safe_event = "".join(ch for ch in ref.event_id if ch.isalnum() or ch in "-_")
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"scraped_{ref.source}_{ref.race_group}_{year}_{safe_event}.parquet")
    df.to_parquet(path, index=False)


def scrape_url(url):
    provider = detect_provider(url)
    refs = provider.list_events(url)
    if not refs:
        st.error("No events found at that URL.")
        return
    progress = st.progress(0)
    status = st.empty()
    failed = []
    for i, ref in enumerate(refs):
        status.text(f"Scraping {ref.name} ({ref.date_str})…")
        try:
            df = provider.fetch_event(ref)
            if not df.empty:
                save_event_frame(df, ref)
        except Exception as e:
            failed.append(f"{ref.name}: {e}")
        progress.progress((i + 1) / len(refs))
    if failed:
        st.warning("Finished with errors:\n\n" + "\n".join(f"- {f}" for f in failed))
    else:
        st.success("All editions scraped.")
    st.rerun()


# --- Sidebar -----------------------------------------------------------------
with st.sidebar:
    st.header("Add a race")
    st.caption("Paste a results URL from athlinks.com, results.nyrr.org or runsignup.com. "
               "Every available year is fetched.")
    if "race_url" not in st.session_state:
        st.session_state.race_url = ""
    example = st.selectbox("Or try an example", ["—"] + list(EXAMPLES), key="example_pick")
    if example != "—":
        st.session_state.race_url = EXAMPLES[example]
    url = st.text_input("Results URL", key="race_url")
    if st.button("Fetch all years", type="primary", disabled=not url):
        with st.spinner("Talking to the results provider…"):
            try:
                scrape_url(url.strip())
            except Exception as e:
                st.error(str(e))

    st.divider()
    uploaded_files = st.file_uploader("Or upload CSV results", accept_multiple_files=True, type="csv")

# --- Data ----------------------------------------------------------------------
has_local_data = os.path.isdir(DATA_DIR) and any(
    f.endswith((".parquet", ".csv")) for f in os.listdir(DATA_DIR))
if not uploaded_files and not has_local_data:
    masthead("Turkey Trot Results Program", "No races loaded yet",
             "Add a race from the sidebar to print your program.")
    st.stop()

con = dq.init_db(uploaded_files or [])
groups = dq.get_event_names(con)
selected_group = None
display_name = "Race results"

if groups:
    st.sidebar.divider()
    st.sidebar.header("Choose race")
    labels = {f"{g['display_name']}  ·  {g['n_years']} yr  ·  {g['source_name']}": g for g in groups}
    picked = st.sidebar.selectbox("Race", list(labels), index=0, label_visibility="collapsed")
    selected_group = labels[picked]["group_key"]
    display_name = labels[picked]["display_name"]

    with st.sidebar.expander("Rename this race"):
        new_name = st.text_input("Display name", value=display_name)
        if st.button("Save name") and new_name and new_name != display_name:
            dq.save_custom_event_name(selected_group, new_name)
            st.rerun()

dq.create_enriched_view(con, selected_group)

# --- Masthead ------------------------------------------------------------------
stats = dq.get_overview_stats(con)
years = con.execute("SELECT MIN(event_year), MAX(event_year), COUNT(DISTINCT event_year) FROM results_enriched").fetchone()
if stats.empty or years[0] is None:
    dateline = "No finishers loaded"
else:
    total = int(stats["total_runners"][0])
    dateline = f"{years[0]} – {years[1]}  ·  {years[2]} editions  ·  {total:,} finishers"
masthead(display_name, dateline,
         "To some, the community Turkey Trot is a family tradition. To others, it's the one day a year "
         "to race their seventh-grade English teacher. Here's the whole history — find your rivals, "
         "track the field, and see where you stand.")

# --- Sections ------------------------------------------------------------------
tab_trends, tab_tools, tab_card, tab_hof, tab_predict = st.tabs(
    ["Analytics & Trends", "Runner Tools", "Report Card", "Hall of Fame", "Place Predictor"])

with tab_trends:
    overview.render(con)
    trends.render(con)
with tab_tools:
    runner_tools.render(con)
with tab_card:
    report_card.render(con)
with tab_hof:
    hall_of_fame.render(con)
with tab_predict:
    predictor.render(con)
