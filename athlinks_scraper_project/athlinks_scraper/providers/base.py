"""Canonical results schema and the provider interface."""
from dataclasses import dataclass
from typing import List

import pandas as pd

# Every provider returns exactly these columns in this order. The dashboard's
# SQL refers to them by name, so never rename one.
CANONICAL_COLUMNS = [
    "Source", "Race Group", "Event ID", "Event Name", "Event Date", "Race Type",
    "Name", "Gender", "Age", "Bib", "City", "State", "Country",
    "Time", "Pace", "Overall Rank", "Gender Rank", "Division Rank", "Status",
]


@dataclass
class EventRef:
    """One scrape-able event (one year, one distance) on one provider."""
    source: str
    event_id: str
    name: str
    date_str: str
    race_group: str
    race_type: str = ""


def to_canonical(rows: List[dict]) -> pd.DataFrame:
    """Builds a DataFrame with CANONICAL_COLUMNS from a list of row dicts,
    filling any missing column with NaN and dropping unknown keys."""
    df = pd.DataFrame(rows)
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series([None] * len(df), dtype="object")
    return df[CANONICAL_COLUMNS].reset_index(drop=True)


class RaceProvider:
    """Interface. Subclasses set `name` and implement the three methods."""
    name = ""

    def matches(self, url: str) -> bool:
        raise NotImplementedError

    def list_events(self, url: str, session=None) -> List[EventRef]:
        """All scrape-able events reachable from `url`, newest first."""
        raise NotImplementedError

    def fetch_event(self, ref: EventRef, session=None) -> pd.DataFrame:
        """Results for one event as a canonical DataFrame (may be empty)."""
        raise NotImplementedError
