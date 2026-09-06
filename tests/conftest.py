"""Shared pytest configuration.

Both components live in subdirectories and the dashboard is not an installable
package, so we put both source directories on sys.path here. This runs before
any test module is imported.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPER_DIR = os.path.join(REPO_ROOT, "athlinks_scraper_project")
DASHBOARD_DIR = os.path.join(REPO_ROOT, "dashboard")

for path in (SCRAPER_DIR, DASHBOARD_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import pandas as pd
import pytest

COLUMNS = [
    "Source", "Race Group", "Event ID", "Event Name", "Event Date", "Race Type", "Name",
    "Gender", "Age", "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status",
]


def _row(event_date, name, gender, age, time_str, pace_str, rank, status="CONF",
         group="111", event_name="Test Trot", race_type="5K"):
    return {
        "Event ID": "1", "Event Name": event_name, "Event Date": event_date,
        "Race Type": race_type, "Name": name, "Gender": gender, "Age": age,
        "Bib": str(rank), "City": "Branford", "State": "CT", "Country": "USA",
        "Time": time_str, "Pace": pace_str, "Overall Rank": rank,
        "Gender Rank": rank, "Division Rank": rank, "Status": status,
        "Source": "athlinks", "Race Group": group,
    }


@pytest.fixture
def sample_results_df():
    """
    Two years of one event (master "111") plus one row from another event
    (master "222"). After create_enriched_view(con, "111") the surviving rows
    are the 7 marked KEEP below; the others are filtered out.
    """
    rows = [
        # --- 2022 ---
        _row("2022-11-24", "Alice Fast", "F", 30, "18:00", "5:48", 1),   # KEEP
        _row("2022-11-24", "Bob Mid", "M", 45, "25:00", "8:03", 2),      # KEEP
        _row("2022-11-24", "Carol Slow", "F", 60, "35:00", "11:16", 3),  # KEEP
        _row("2022-11-24", "Dan Quick", "M", 20, "10:00", "3:13", 4),    # dropped: faster than 12:00
        _row("2022-11-24", "Eve Dnf", "F", 25, "20:00", "6:26", 5, status="DNF"),  # dropped: DNF
        # --- 2023 ---
        _row("2023-11-23", "Alice Fast", "F", 31, "17:30", "5:38", 1),   # KEEP
        _row("2023-11-23", "Bob Mid", "M", 46, "24:00", "7:44", 2),      # KEEP
        _row("2023-11-23", "Frank New", "M", 33, "30:00", "9:40", 3),    # KEEP
        _row("2023-11-23", "Carol Slow", "F", 61, "31:00", "9:59", 4),   # KEEP
        # --- a different event, must be filtered out by master id ---
        _row("2023-11-23", "Zed Other", "M", 40, "22:00", "7:05", 1,
             group="222", event_name="Other Trot"),
    ]
    return pd.DataFrame(rows, columns=COLUMNS)


@pytest.fixture
def db(sample_results_df):
    """DuckDB connection with results_enriched built for master '111'."""
    import dashboard_queries

    con = dashboard_queries.init_db_from_dataframe(sample_results_df)
    dashboard_queries.create_enriched_view(con, "111")
    return con
