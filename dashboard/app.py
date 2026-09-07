import os
import re
import sys

import pandas as pd
import streamlit as st

# Make the scraper package and this folder importable regardless of cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "athlinks_scraper_project"))
sys.path.insert(0, HERE)

import dashboard_queries as dq  # noqa: E402
import storage  # noqa: E402
from athlinks_scraper.providers import detect_provider  # noqa: E402
from sections import hall_of_fame, overview, predictor, report_card, runner_tools, trends  # noqa: E402
from ui import theme  # noqa: E402
from ui.components import masthead  # noqa: E402

st.set_page_config(page_title="Turkey Trot Results Program", layout="wide")
theme.inject()

EXAMPLES = {
    "Branford Turkey Trot (Athlinks)": "https://www.athlinks.com/event/15776",
    "NYRR Frosty 5K (NYRR)": "https://results.nyrr.org/event/24FROSTY/finishers",
    "#RUNMARANA Turkey Trot (RunSignup)": "https://runsignup.com/Race/Results/100692",
}


# --- Store --------------------------------------------------------------------------
def _secrets_dict():
    # st.secrets raises when no secrets.toml exists (the local-dev case).
    try:
        return st.secrets.to_dict()
    except Exception:
        return {}


DSN = storage.resolve_dsn(secrets=_secrets_dict(), env=os.environ)

_TOKEN_RE = re.compile(r"motherduck_token=[^&\s'\"]+")


def _redact(exc):
    return _TOKEN_RE.sub("motherduck_token=***", str(exc))


@st.cache_resource(show_spinner="Connecting to the results store…")
def get_store(dsn):
    return storage.open_store(dsn)


@st.cache_data(ttl=3600, max_entries=2, show_spinner="Loading race…")
def load_group_cached(dsn, group_key):
    # `dsn` is only here so the cache key changes if the store does.
    return storage.load_group(storage.cursor(get_store(dsn)), group_key)


def flash(level, text):
    """
    Queue a message for the next run. st.rerun() throws away the current
    render, so anything written before it is never seen; the sidebar pops
    these on the way back in.
    """
    st.session_state.setdefault("flash", []).append((level, text))


def invalidate_and_rerun():
    load_group_cached.clear()
    st.rerun()


try:
    store = get_store(DSN)
except Exception as e:
    print(f"open_store failed: {_redact(e)}")
    st.error("Could not open the results store. Check the app logs for the DuckDB error.")
    st.caption("If this is the hosted app, check the `motherduck` token in the app's Secrets.")
    st.stop()


def scrape_url(url):
    provider = detect_provider(url)
    refs = provider.list_events(url)
    if not refs:
        st.error("No events found at that URL.")
        return
    progress = st.progress(0)
    status = st.empty()
    failed = []
    cur = storage.cursor(store)
    for i, ref in enumerate(refs):
        status.text(f"Scraping {ref.name} ({ref.date_str})…")
        try:
            df = provider.fetch_event(ref)
            storage.save_event(cur, df, ref)
        except Exception as e:
            failed.append(f"{ref.name}: {e}")
        progress.progress((i + 1) / len(refs))
    if failed:
        flash("warning", "Finished with errors:\n\n" + "\n".join(f"- {f}" for f in failed))
    else:
        flash("success", "All editions scraped.")
    invalidate_and_rerun()


def import_uploads(files):
    cur = storage.cursor(store)
    total = 0
    failed = False
    for f in files:
        try:
            total += storage.save_frame(cur, pd.read_csv(f), f.name)
        except Exception as e:
            failed = True
            flash("error", f"{f.name}: {e}")
    if total > 0:
        flash("success", f"Imported {total:,} rows.")
    elif not failed:
        flash("warning", "No rows imported (files had no resolvable race).")
    invalidate_and_rerun()


# --- Sidebar -----------------------------------------------------------------
with st.sidebar:
    st.header("Add a race")
    for level, text in st.session_state.pop("flash", []):
        {"success": st.success, "warning": st.warning, "error": st.error}[level](text)
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
    if st.button("Import CSVs", disabled=not uploaded_files):
        import_uploads(uploaded_files)

# --- Data ----------------------------------------------------------------------
try:
    groups = dq.get_event_names(storage.cursor(store))
except Exception as e:
    print(f"list races failed: {_redact(e)}")
    st.error("Could not read the results store. Check the app logs for the DuckDB error.")
    st.stop()
if not groups:
    masthead("Turkey Trot Results Program", "No races loaded yet",
             "Add a race from the sidebar to print your program.")
    st.stop()

st.sidebar.divider()
st.sidebar.header("Choose race")
labels = {f"{g['display_name']}  ·  {g['n_years']} yr  ·  {g['source_name']}": g for g in groups}
picked = st.sidebar.selectbox("Race", list(labels), index=0, label_visibility="collapsed")
selected_group = labels[picked]["group_key"]
display_name = labels[picked]["display_name"]

with st.sidebar.expander("Rename this race"):
    new_name = st.text_input("Display name", value=display_name)
    if st.button("Save name") and new_name and new_name != display_name:
        storage.save_custom_event_name(storage.cursor(store), selected_group, new_name)
        invalidate_and_rerun()

con = dq.init_db_from_dataframe(load_group_cached(DSN, selected_group))
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
