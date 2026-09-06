# Multi-Source Results + "Race-Day Program" Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Prerequisite:** `docs/superpowers/plans/2026-09-06-site-improvements.md` must be fully executed first. This plan relies on `core.fetch_json`, `core.get_session`, `dashboard_queries.init_db_from_dataframe`, the `to_seconds` macro, `dashboard_queries.format_seconds`, `tests/fakes.py`, and the fixtures in `tests/conftest.py`. Confirm `python -m pytest tests -v` passes (39 tests) before starting Task 1.

**Goal:** Let the dashboard ingest results from Athlinks, New York Road Runners (NYRR), and RunSignup through one "paste a URL" box; restyle the dashboard as a race-day printed results program instead of a generic analytics dashboard; add a Runner Report Card, Head-to-Head, and Returning-Runners features.

**Architecture:** A small `providers` subpackage inside the existing `athlinks_scraper` package defines one canonical results DataFrame schema and a `RaceProvider` interface (`matches(url)`, `list_events(url)`, `fetch_event(ref)`). Three providers implement it. `detect_provider(url)` picks one from the URL host. The dashboard stops keying everything on Athlinks' numeric "Master ID" and instead uses a text `"Race Group"` slug that every provider produces (for Athlinks it is still the master ID, so existing files keep working). The Streamlit app is split into a theme module, a components module, and one module per section; `app.py` becomes a thin shell.

**Tech Stack:** Python 3.9+, `requests`, `pandas`, `duckdb`, `streamlit`, `plotly`, `pytest`. No new third-party dependencies.

**Spec:** No separate spec. The "Design" section below is the spec; each task cites the design items it implements.

## Global Constraints

- Python 3.9 compatible syntax only (use `typing.List`, `typing.Optional`; no `match`, no `X | Y`).
- No new third-party dependencies.
- The canonical CSV/Parquet column names are exactly those in `CANONICAL_COLUMNS` (Task 1). Files written before this plan (`scraped_<masterid>_<year>.parquet`, no `Source`/`Race Group` columns) MUST still load — Task 7 handles the backfill.
- Never hit a real API from a test. Use `tests/fakes.py`.
- Run tests only from the repo root: `python -m pytest tests -v`.
- All HTML/CSS strings go in `dashboard/ui/theme.py` or `dashboard/ui/components.py` — no inline `<div style=...>` blocks in section modules.
- Commit after every task with the exact message given.

---

## Design

### D1. Verified facts about the three APIs (do not re-research; these were probed live on 2026-09-06)

**Athlinks** (already implemented in `core.py`):
- `GET https://reignite-api.athlinks.com/master/{master_id}/metadata` → `{"events": [{"id", "name", "start": {"epoch"}}]}`
- `GET https://reignite-api.athlinks.com/event/{event_id}/metadata` → `{"id", "name", "masterId", "start": {"epoch"}}`
- `GET https://reignite-api.athlinks.com/event/{event_id}/results?from=N&limit=100` → list of course blocks (see `parse_results`).

**NYRR** — base `https://rmsprodapi.nyrr.org/api/v2`, all `POST` with JSON bodies, header `content-type: application/json;charset=UTF-8`. **No auth token, no browser headers required.** Public URLs look like `https://results.nyrr.org/event/24FROSTY/finishers`; the event code is the path segment after `/event/`.
- `POST /events/details` body `{"eventCode": "24FROSTY"}` → `{"eventDetails": {"eventName": "2024 NYRR Frosty 5K", "eventCode": "24FROSTY", "startDateTime": "2024-12-14T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K", "distanceDimension": 0.0, ...}, "success": true}`
- `POST /events/search` body `{"year": null, "searchString": "Frosty", "distance": null, "pageIndex": 1, "pageSize": 100}` → `{"totalItems": 10, "items": [{"eventName", "eventCode", "startDateTime", "distanceName", "distanceUnitCode", ...}]}`. `year: null` searches all years. `pageSize` must be ≤ 100 (larger values return `{"success": false, "message": "The 'pageSize' field is invalid"}`).
- `POST /runners/finishers-filter` body `{"eventCode": "24FROSTY", "pageIndex": 1, "pageSize": 100, "sortColumn": "overallPlace", "sortDescending": false}` → `{"totalItems": 3368, "items": [{"runnerId", "firstName", "lastName", "bib", "age", "gender", "city", "countryCode", "stateProvince", "overallPlace", "overallTime": "0:16:03", "gunTime", "pace": "05:10", "genderPlace", "ageGradePercent", ...}]}`. `pageSize` max is 100. `pageIndex` is 1-based. Times are `H:MM:SS` (hour may be `0`), pace is `MM:SS` with a leading zero.
- Event names embed the year and "NYRR"; the same race in different years has different event codes (`24FROSTY`, `23FROSTY`, `21Seasonal`) so sibling years must be found by *name*, not by code.

**RunSignup** — base `https://runsignup.com/Rest`, all `GET`, add `format=json`. **No API key required for public results.** Public URLs look like `https://runsignup.com/Race/Results/100692` (numeric race id after `/Race/Results/`), or contain `raceId=100692`.
- `GET /race/{race_id}?format=json` → `{"race": {"race_id", "name", "events": [{"event_id": 1024596, "name": "5k Run/Walk", "start_time": "11/15/2025 08:20", "distance": "3.1 Miles"}, ...]}}`. A "race" is the multi-year organisation; each `event` is one distance in one year (future years appear too and simply have no results yet).
- `GET /race/{race_id}/results/get-results?format=json&event_id={event_id}&num=1000&page={n}` → `{"individual_results_sets": [{"individual_result_set_name", "public_results": "T", "results": [{"place": 1, "bib": 517, "first_name", "last_name", "gender": "M", "city", "state", "country_code": "US", "clock_time": "21:53.15", "chip_time": "21:52.05", "pace": "7:03", "age": 44, ...}]}]}`. An event with no results yet returns `{"individual_results_sets": []}`. Times can carry fractional seconds (`21:52.05`) — strip them. There is no total count; page until a page returns fewer than `num` rows.

### D2. Canonical schema and Race Group

Every provider returns a DataFrame with exactly these columns, in this order:

```
Source, Race Group, Event ID, Event Name, Event Date, Race Type, Name, Gender, Age, Bib,
City, State, Country, Time, Pace, Overall Rank, Gender Rank, Division Rank, Status
```

- `Source` ∈ `{"athlinks", "nyrr", "runsignup"}`.
- `Race Group` is the key the dashboard groups years under. Athlinks: the master ID digits (so `"15776"` — unchanged from today's `Master ID`). NYRR and RunSignup: `race_group_key(name)` — lower-case, strip any 4-digit year, strip "NYRR" / "New York Road Runners" / "the", non-alphanumerics → `-`. E.g. `"2024 NYRR Frosty 5K"` → `"frosty-5k"`.
- `Event Date` is `YYYY-MM-DD`. `Time` is `MM:SS` or `H:MM:SS` with no fractional seconds. `Pace` is `M:SS` or `MM:SS` min/mile (NYRR's `"05:10"` is accepted as-is by `to_seconds`).
- The `Master ID` column is retired. Old files with `Master ID` but no `Race Group` get `Race Group = Master ID` and `Source = "athlinks"` at load time.
- Saved file name: `dashboard/data/scraped_{source}_{race_group}_{year}_{event_id}.parquet`. The old pattern `scraped_{masterid}_{year}.parquet` is still read.
- The custom-name file `dashboard/data/event_metadata.json` is keyed by `Race Group` (existing keys like `"15776"` keep working).

### D3. Provider interface (`athlinks_scraper/providers/base.py`)

```python
@dataclass
class EventRef:
    source: str        # "athlinks" | "nyrr" | "runsignup"
    event_id: str      # provider-native id/code, always str
    name: str          # provider's event name, e.g. "2024 NYRR Frosty 5K"
    date_str: str      # "YYYY-MM-DD" or "Unknown"
    race_group: str    # see D2
    race_type: str = ""  # e.g. "5K"; providers fill it when known, else ""

class RaceProvider:
    name = ""                                          # same value as EventRef.source
    def matches(self, url: str) -> bool: ...
    def list_events(self, url: str, session=None) -> List[EventRef]: ...   # newest first
    def fetch_event(self, ref: EventRef, session=None) -> pd.DataFrame: ... # canonical columns
```

`detect_provider(url)` in `providers/registry.py` returns the first provider whose `matches` is true or raises `ValueError("Unsupported URL: ...")`.

### D4. Dashboard data flow

Sidebar "Add a race" text box → `detect_provider(url)` → `provider.list_events(url)` → for each ref: `provider.fetch_event(ref)` inside `try/except` → save parquet → `st.rerun()`. The "Choose Race" selectbox lists `Race Group`s with display names from `race_group_label(FIRST("Event Name"))`, overridden by `event_metadata.json`.

### D5. Visual design — "race-day printed program"

Feel: the stapled results booklet handed out at a small-town Turkey Trot. Warm paper, ink-black type, one accent, rules instead of cards, bib-number-style stat tiles. Specifics (these are the only values to use):

- Palette: paper `#F6EFE3`, paper-2 (secondary bg) `#EFE5D3`, ink `#1B1A17`, muted `#6B6257`, rule `#D9CDB8`, accent (turkey orange) `#C8501B`, accent-2 (pine) `#2F5D50`, highlight (goldenrod) `#E0A526`.
- Type: display/numbers **Barlow Condensed** 700 (Google Fonts); body **IBM Plex Sans** 400/600; times/tables **IBM Plex Mono** 500. Fallbacks: `Impact, "Arial Narrow", sans-serif` / `system-ui, sans-serif` / `ui-monospace, Menlo, monospace`.
- Masthead at top: kicker line `OFFICIAL RESULTS PROGRAM` (mono, letter-spaced, accent), race name in Barlow Condensed at ~4rem, a dateline in muted text: `2010 – 2024 · 14 editions · 12,345 finishers`, then a 3px ink rule.
- Stat tiles ("bib tiles"): paper-2 background, 2px ink border, small uppercase mono label top-left, huge condensed number, optional one-line note. No drop shadows, no hover transforms, no colored top stripes.
- Section headers: a small uppercase mono kicker (accent) above a Barlow Condensed title, with a 1px rule underneath. Kill the identical italic Lora blurb under every header; a single 2-sentence intro lives under the masthead only.
- Charts: transparent background, ink axes, gridlines in rule color, series colors `[ink, accent, accent-2, highlight, muted]`, Barlow Condensed titles, mono tick labels.
- Tabs: keep `st.tabs` but restyle: uppercase condensed labels, accent underline on the active tab, no pill background.
- Tables: `st.dataframe` as-is (Streamlit doesn't allow deep restyling) — but pass `hide_index=True` everywhere.
- Streamlit `config.toml` theme updated to the palette so native widgets match.

### D6. New features

- **Runner Report Card** (new tab "Report Card"): type a name → pick from matching normalized names → tiles (Editions run, Best time, Best year, Avg % of field beaten), line chart of the runner's time per year against the field median, per-year table (year, time, pace, place, field size, % beaten), and the rivals table (reuse `get_nemesis`).
- **Head-to-Head** (inside Runner Tools): two names → table of shared years with both times and the margin, plus a one-line verdict ("Alice leads 2–0, average margin 6:45").
- **Returning Runners** (inside Analytics & Trends): stacked bar of new vs returning runners per year using `get_returning_counts`. The existing unused `get_retention_data` is deleted.

### D7. Out of scope

Strava/Garmin (personal-activity APIs, not race results), Race Roster and ChronoTrack (no keyless results API found), age-grading, splits, mobile-specific layout, authentication, deployment.

---

## File Structure

**Created**
- `athlinks_scraper_project/athlinks_scraper/providers/__init__.py` — re-exports `detect_provider`, `EventRef`, `CANONICAL_COLUMNS`.
- `athlinks_scraper_project/athlinks_scraper/providers/base.py` — `CANONICAL_COLUMNS`, `EventRef`, `RaceProvider`, `to_canonical(rows)`.
- `athlinks_scraper_project/athlinks_scraper/providers/naming.py` — `race_group_key`, `race_group_label`, `strip_fractional_seconds`.
- `athlinks_scraper_project/athlinks_scraper/providers/athlinks.py` — `AthlinksProvider`.
- `athlinks_scraper_project/athlinks_scraper/providers/nyrr.py` — `NyrrProvider`.
- `athlinks_scraper_project/athlinks_scraper/providers/runsignup.py` — `RunSignupProvider`.
- `athlinks_scraper_project/athlinks_scraper/providers/registry.py` — `PROVIDERS`, `detect_provider`.
- `dashboard/ui/__init__.py`, `dashboard/ui/theme.py`, `dashboard/ui/components.py`.
- `dashboard/sections/__init__.py`, `dashboard/sections/overview.py`, `trends.py`, `runner_tools.py`, `hall_of_fame.py`, `predictor.py`, `report_card.py`.
- `tests/test_naming.py`, `tests/test_providers_athlinks.py`, `tests/test_providers_nyrr.py`, `tests/test_providers_runsignup.py`, `tests/test_registry.py`, `tests/test_dashboard_groups.py`, `tests/test_dashboard_features.py`.

**Modified**
- `athlinks_scraper_project/athlinks_scraper/core.py` — add `post_json`.
- `athlinks_scraper_project/athlinks_scraper/cli.py` — route through providers.
- `dashboard/dashboard_queries.py` — Race Group, new queries, delete `get_retention_data`.
- `dashboard/app.py` — becomes a shell.
- `dashboard/.streamlit/config.toml` — palette.
- `tests/conftest.py` — fixture columns.
- `tests/test_dashboard_queries.py` — one renamed test.
- `README.md` — supported sources.

---

### Task 1: Canonical schema, `EventRef`, naming helpers

**Files:**
- Create: `athlinks_scraper_project/athlinks_scraper/providers/__init__.py`
- Create: `athlinks_scraper_project/athlinks_scraper/providers/base.py`
- Create: `athlinks_scraper_project/athlinks_scraper/providers/naming.py`
- Create: `tests/test_naming.py`

**Interfaces:**
- Produces: `CANONICAL_COLUMNS: List[str]`, `EventRef` dataclass, `RaceProvider` base class, `to_canonical(rows: List[dict]) -> pd.DataFrame`, `race_group_key(name) -> str`, `race_group_label(name) -> str`, `strip_fractional_seconds(t) -> str`.

- [ ] **Step 1: Write the failing tests**

`tests/test_naming.py`:

```python
import pandas as pd

from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef, to_canonical
from athlinks_scraper.providers.naming import (
    race_group_key,
    race_group_label,
    strip_fractional_seconds,
)


def test_race_group_key_strips_year_and_nyrr():
    assert race_group_key("2024 NYRR Frosty 5K") == "frosty-5k"
    assert race_group_key("2023 NYRR Frosty 5K") == "frosty-5k"


def test_race_group_key_unifies_nyrr_spelling_variants():
    a = race_group_key("2024 Rising New York Road Runners at the NYRR Frosty 5K - Stage 3")
    b = race_group_key("2025 Rising NYRR at the NYRR Frosty 5K - Stage 3")
    assert a == b == "rising-at-frosty-5k-stage-3"


def test_race_group_key_handles_punctuation():
    assert race_group_key("#RUNMARANA Turkey Trot and Kid’s Free FUN Run") == (
        "runmarana-turkey-trot-and-kid-s-free-fun-run"
    )
    assert race_group_key("Branford Turkey Trot 2023") == "branford-turkey-trot"


def test_race_group_label_keeps_case_drops_year():
    assert race_group_label("2024 NYRR Frosty 5K") == "Frosty 5K"
    assert race_group_label("Branford Turkey Trot 2023") == "Branford Turkey Trot"
    assert race_group_label("TCS New York City Marathon 2025") == "TCS New York City Marathon"


def test_strip_fractional_seconds():
    assert strip_fractional_seconds("21:52.05") == "21:52"
    assert strip_fractional_seconds("1:02:03.9") == "1:02:03"
    assert strip_fractional_seconds("25:00") == "25:00"
    assert strip_fractional_seconds("") == ""
    assert strip_fractional_seconds(None) is None


def test_canonical_columns_order():
    assert CANONICAL_COLUMNS[:2] == ["Source", "Race Group"]
    assert CANONICAL_COLUMNS[-1] == "Status"
    assert "Master ID" not in CANONICAL_COLUMNS


def test_to_canonical_fills_missing_columns_and_orders():
    df = to_canonical([{"Name": "A", "Time": "20:00", "Source": "nyrr", "Race Group": "x"}])

    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.loc[0, "Name"] == "A"
    assert pd.isna(df.loc[0, "Age"])


def test_to_canonical_empty():
    df = to_canonical([])
    assert list(df.columns) == CANONICAL_COLUMNS
    assert len(df) == 0


def test_eventref_defaults():
    ref = EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                   date_str="2024-12-14", race_group="frosty-5k")
    assert ref.race_type == ""
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_naming.py -v`
Expected: collection error `ModuleNotFoundError: No module named 'athlinks_scraper.providers'`.

- [ ] **Step 3: Create `providers/naming.py`**

```python
"""Name normalisation shared by all providers."""
import re

_YEAR = re.compile(r"\b(19|20)\d{2}\b")


def race_group_key(name):
    """
    Stable slug that identifies "the same race" across years and providers.
    '2024 NYRR Frosty 5K' -> 'frosty-5k'.
    """
    s = (name or "").lower()
    s = s.replace("new york road runners", " ")
    s = _YEAR.sub(" ", s)
    s = re.sub(r"\bnyrr\b", " ", s)
    s = re.sub(r"\bthe\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def race_group_label(name):
    """
    Human display name for a race group: original casing, year and 'NYRR' removed.
    '2024 NYRR Frosty 5K' -> 'Frosty 5K'.
    """
    s = re.sub(r"(?i)new york road runners", " ", name or "")
    s = _YEAR.sub(" ", s)
    s = re.sub(r"(?i)\bnyrr\b", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -–")


def strip_fractional_seconds(time_str):
    """'21:52.05' -> '21:52'. Leaves None / '' untouched."""
    if not time_str:
        return time_str
    return re.sub(r"\.\d+$", "", time_str)
```

- [ ] **Step 4: Create `providers/base.py`**

```python
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
```

- [ ] **Step 5: Create `providers/__init__.py`**

```python
from .base import CANONICAL_COLUMNS, EventRef, RaceProvider, to_canonical  # noqa: F401
from .naming import race_group_key, race_group_label  # noqa: F401
```

(`detect_provider` is added to this file in Task 5.)

- [ ] **Step 6: Run the tests**

Run: `python -m pytest tests/test_naming.py -v`
Expected: 9 passed.

- [ ] **Step 7: Commit**

```bash
git add athlinks_scraper_project/athlinks_scraper/providers tests/test_naming.py
git commit -m "feat(providers): canonical schema, EventRef and naming helpers"
```

---

### Task 2: `post_json` in core + Athlinks provider

**Files:**
- Modify: `athlinks_scraper_project/athlinks_scraper/core.py` (add `post_json` directly under `fetch_json`)
- Create: `athlinks_scraper_project/athlinks_scraper/providers/athlinks.py`
- Create: `tests/test_providers_athlinks.py`

**Interfaces:**
- Consumes: `core.fetch_json`, `core.get_session`, `core.fetch_master_events`, `core.fetch_metadata`, `core.get_results`, `core.extract_master_id`, `core.extract_event_id`.
- Produces: `core.post_json(url, body, session=None, timeout=DEFAULT_TIMEOUT)`, `AthlinksProvider`.

- [ ] **Step 1: Write the failing tests**

`tests/test_providers_athlinks.py`:

```python
import pytest
import requests

from athlinks_scraper import core
from athlinks_scraper.providers.athlinks import AthlinksProvider
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

MASTER_PAYLOAD = {
    "events": [
        {"id": 111, "name": "Trot 2021", "start": {"epoch": 1637755200000}},  # 2021-11-24
        {"id": 222, "name": "Trot 2022", "start": {"epoch": 1669291200000}},  # 2022-11-24
    ]
}


def test_post_json_sends_body_and_returns_payload():
    session = FakeSession([FakeResponse({"ok": 1})])

    out = core.post_json("https://x/y", {"a": 1}, session=session)

    assert out == {"ok": 1}
    url, body, timeout = session.calls[0]
    assert url == "https://x/y"
    assert body == {"a": 1}
    assert timeout == core.DEFAULT_TIMEOUT


def test_post_json_raises_on_http_error():
    session = FakeSession([FakeResponse({}, status_code=500)])
    with pytest.raises(requests.HTTPError):
        core.post_json("https://x/y", {}, session=session)


def test_matches():
    p = AthlinksProvider()
    assert p.matches("https://www.athlinks.com/event/15776")
    assert not p.matches("https://results.nyrr.org/event/24FROSTY/finishers")


def test_list_events_from_master_url():
    session = FakeSession([FakeResponse(MASTER_PAYLOAD)])

    refs = AthlinksProvider().list_events("https://www.athlinks.com/event/15776", session=session)

    assert [r.event_id for r in refs] == ["222", "111"]  # newest first, ids are str
    assert refs[0] == EventRef(source="athlinks", event_id="222", name="Trot 2022",
                               date_str="2022-11-24", race_group="15776")


def test_list_events_from_specific_event_url_uses_master_from_metadata():
    meta = {"id": 222, "name": "Trot 2022", "masterId": 15776, "start": {"epoch": 1669291200000}}
    session = FakeSession([FakeResponse(meta), FakeResponse(MASTER_PAYLOAD)])

    refs = AthlinksProvider().list_events(
        "https://www.athlinks.com/event/15776/results/Event/222/Course/1/Results", session=session
    )

    assert [r.event_id for r in refs] == ["222", "111"]
    assert session.calls[0][0].endswith("/event/222/metadata")


def test_fetch_event_returns_canonical_frame(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    meta = {"id": 222, "name": "Trot 2022", "start": {"epoch": 1669291200000}}
    page = [{"race": {"name": "5K"}, "intervals": [{"distance": {"meters": 5000},
             "results": [{"displayName": "Alice Fast", "gender": "F", "age": 30, "bib": "1",
                          "chipTimeInMillis": 1080000, "location": {}, "rankings": {"overall": 1}}]}]}]
    session = FakeSession([FakeResponse(meta), FakeResponse(page), FakeResponse([])])
    ref = EventRef(source="athlinks", event_id="222", name="Trot 2022", date_str="2022-11-24", race_group="15776")

    df = AthlinksProvider().fetch_event(ref, session=session)

    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.loc[0, "Source"] == "athlinks"
    assert df.loc[0, "Race Group"] == "15776"
    assert df.loc[0, "Name"] == "Alice Fast"
    assert df.loc[0, "Time"] == "18:00"
```

- [ ] **Step 2: Update `tests/fakes.py` so `FakeSession` also records POSTs**

Replace the `FakeSession` class with:

```python
class FakeSession:
    """Returns the queued responses in order; records every call made.

    GET calls are recorded as (url, params, timeout).
    POST calls are recorded as (url, json_body, timeout).
    """

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def _next(self):
        if not self.responses:
            raise AssertionError("FakeSession ran out of queued responses")
        return self.responses.pop(0)

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return self._next()

    def post(self, url, json=None, timeout=None):
        self.calls.append((url, json, timeout))
        return self._next()
```

- [ ] **Step 3: Run to verify failure**

Run: `python -m pytest tests/test_providers_athlinks.py -v`
Expected: `ModuleNotFoundError: No module named 'athlinks_scraper.providers.athlinks'` (or `AttributeError: ... 'post_json'`).

- [ ] **Step 4: Add `post_json` to `core.py`** immediately after `fetch_json`:

```python
def post_json(url, body, session=None, timeout=DEFAULT_TIMEOUT):
    """
    POSTs `body` as JSON to `url` and returns the parsed JSON response.
    Raises requests.RequestException on failure (after the session's retries).
    """
    session = session or get_session()
    response = session.post(url, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 5: Create `providers/athlinks.py`**

```python
"""Athlinks provider — thin adapter over the existing core module."""
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical


class AthlinksProvider(RaceProvider):
    name = "athlinks"

    def matches(self, url: str) -> bool:
        return "athlinks.com" in url.lower()

    def list_events(self, url: str, session=None) -> List[EventRef]:
        master_id = core.extract_master_id(url)
        if not master_id:
            # A results URL for one specific year: ask the event for its master.
            event_id = core.extract_event_id(url)
            if not event_id:
                raise ValueError(f"Could not find an Athlinks event id in {url}")
            meta = core.fetch_metadata(event_id, session=session)
            master_id = str(meta.get("masterId") or "")
            if not master_id:
                raise ValueError(f"Athlinks event {event_id} has no master id")

        events = core.fetch_master_events(master_id, session=session)
        return [
            EventRef(
                source=self.name,
                event_id=str(e["id"]),
                name=e.get("name") or "",
                date_str=e.get("date_str") or "Unknown",
                race_group=str(master_id),
            )
            for e in events
        ]

    def fetch_event(self, ref: EventRef, session=None):
        df = core.get_results(ref.event_id, session=session)
        rows = df.to_dict("records")
        for row in rows:
            row["Source"] = self.name
            row["Race Group"] = ref.race_group
        return to_canonical(rows)
```

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest tests -v`
Expected: 39 + 9 + 6 = 54 passed. (The Task-1 fixture and old tests are unaffected by the `FakeSession` change because they only call `.get`.)

- [ ] **Step 7: Commit**

```bash
git add athlinks_scraper_project/athlinks_scraper/core.py athlinks_scraper_project/athlinks_scraper/providers/athlinks.py tests/fakes.py tests/test_providers_athlinks.py
git commit -m "feat(providers): Athlinks provider and core.post_json"
```

---

### Task 3: NYRR provider

**Files:**
- Create: `athlinks_scraper_project/athlinks_scraper/providers/nyrr.py`
- Create: `tests/test_providers_nyrr.py`

**Interfaces:**
- Consumes: `core.post_json`, `core.REQUEST_DELAY_SECONDS`, `core.MAX_PAGES`, `core.time`, `race_group_key`, `EventRef`, `to_canonical`.
- Produces: `NyrrProvider`, module constants `NYRR_API = "https://rmsprodapi.nyrr.org/api/v2"`, `NYRR_PAGE_SIZE = 100`.

- [ ] **Step 1: Write the failing tests**

`tests/test_providers_nyrr.py`:

```python
import pytest

from athlinks_scraper import core
from athlinks_scraper.providers import nyrr
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

DETAILS_24 = {"eventDetails": {"eventName": "2024 NYRR Frosty 5K", "eventCode": "24FROSTY",
                               "startDateTime": "2024-12-14T08:00:00", "distanceName": "5 kilometers",
                               "distanceUnitCode": "5K"}, "success": True}

SEARCH = {"totalItems": 4, "items": [
    {"eventName": "2024 Rising NYRR at the NYRR Frosty 5K - Stage 3", "eventCode": "24YFROSTY",
     "startDateTime": "2024-12-14T08:45:00", "distanceName": "1 mile", "distanceUnitCode": "1M"},
    {"eventName": "2024 NYRR Frosty 5K", "eventCode": "24FROSTY",
     "startDateTime": "2024-12-14T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
    {"eventName": "2023 NYRR Frosty 5K", "eventCode": "23FROSTY",
     "startDateTime": "2023-12-09T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
    {"eventName": "2021 NYRR Frosty 5K", "eventCode": "21Seasonal",
     "startDateTime": "2021-12-11T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
]}


def finisher(i, place):
    return {"runnerId": i, "firstName": f"First{i}", "lastName": f"Last{i}", "bib": str(i), "age": 30,
            "gender": "M", "city": "New York", "countryCode": "USA", "stateProvince": "NY",
            "overallPlace": place, "overallTime": "0:16:03", "gunTime": "0:16:05", "pace": "05:10",
            "genderPlace": place}


def test_matches_and_event_code():
    p = nyrr.NyrrProvider()
    assert p.matches("https://results.nyrr.org/event/24FROSTY/finishers")
    assert not p.matches("https://www.athlinks.com/event/1")
    assert nyrr.extract_event_code("https://results.nyrr.org/event/24FROSTY/finishers") == "24FROSTY"
    assert nyrr.extract_event_code("https://results.nyrr.org/races") is None


def test_list_events_finds_sibling_years_by_name():
    session = FakeSession([FakeResponse(DETAILS_24), FakeResponse(SEARCH)])

    refs = nyrr.NyrrProvider().list_events("https://results.nyrr.org/event/24FROSTY/finishers", session=session)

    # The 1-mile "Rising" event has a different group key and is excluded.
    assert [r.event_id for r in refs] == ["24FROSTY", "23FROSTY", "21Seasonal"]
    assert refs[0] == EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                               date_str="2024-12-14", race_group="frosty-5k", race_type="5K")
    details_call, search_call = session.calls
    assert details_call[0] == nyrr.NYRR_API + "/events/details"
    assert details_call[1] == {"eventCode": "24FROSTY"}
    assert search_call[0] == nyrr.NYRR_API + "/events/search"
    assert search_call[1]["searchString"] == "Frosty 5K"
    assert search_call[1]["year"] is None
    assert search_call[1]["pageSize"] == nyrr.NYRR_PAGE_SIZE


def test_list_events_bad_url_raises():
    with pytest.raises(ValueError):
        nyrr.NyrrProvider().list_events("https://results.nyrr.org/races")


def test_fetch_event_pages_and_maps_fields(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    page1 = {"totalItems": 101, "items": [finisher(i, i + 1) for i in range(100)]}
    page2 = {"totalItems": 101, "items": [finisher(100, 101)]}
    session = FakeSession([FakeResponse(page1), FakeResponse(page2)])
    ref = EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                   date_str="2024-12-14", race_group="frosty-5k", race_type="5K")

    df = nyrr.NyrrProvider().fetch_event(ref, session=session)

    assert list(df.columns) == CANONICAL_COLUMNS
    assert len(df) == 101
    assert [c[1]["pageIndex"] for c in session.calls] == [1, 2]
    assert all(c[1]["eventCode"] == "24FROSTY" for c in session.calls)
    first = df.iloc[0]
    assert first["Source"] == "nyrr"
    assert first["Race Group"] == "frosty-5k"
    assert first["Event ID"] == "24FROSTY"
    assert first["Event Name"] == "2024 NYRR Frosty 5K"
    assert first["Event Date"] == "2024-12-14"
    assert first["Race Type"] == "5K"
    assert first["Name"] == "First0 Last0"
    assert first["Gender"] == "M"
    assert first["Age"] == 30
    assert first["Bib"] == "0"
    assert first["City"] == "New York"
    assert first["State"] == "NY"
    assert first["Country"] == "USA"
    assert first["Time"] == "0:16:03"
    assert first["Pace"] == "05:10"
    assert first["Overall Rank"] == 1
    assert first["Gender Rank"] == 1
    assert first["Status"] == "CONF"


def test_fetch_event_empty(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse({"totalItems": 0, "items": []})])
    ref = EventRef(source="nyrr", event_id="X", name="n", date_str="2020-01-01", race_group="n")

    df = nyrr.NyrrProvider().fetch_event(ref, session=session)

    assert len(df) == 0
    assert list(df.columns) == CANONICAL_COLUMNS


def test_race_type_mapping():
    assert nyrr.race_type_from_unit("MAR", "Marathon") == "Marathon"
    assert nyrr.race_type_from_unit("5K", "5 kilometers") == "5K"
    assert nyrr.race_type_from_unit("HALF", "Half-Marathon") == "Half Marathon"
    assert nyrr.race_type_from_unit("ZZZ", "Something odd") == "Something odd"
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_providers_nyrr.py -v`
Expected: `ModuleNotFoundError: No module named 'athlinks_scraper.providers.nyrr'`.

- [ ] **Step 3: Create `providers/nyrr.py`**

```python
"""New York Road Runners provider (results.nyrr.org).

API facts (verified 2026-09-06): base https://rmsprodapi.nyrr.org/api/v2, all
POST + JSON, no auth. pageSize max 100. Event codes differ per year, so
sibling years are found by searching on the normalised event name.
"""
import re
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical
from .naming import race_group_key, race_group_label

NYRR_API = "https://rmsprodapi.nyrr.org/api/v2"
NYRR_PAGE_SIZE = 100

_EVENT_CODE = re.compile(r"results\.nyrr\.org/event/([A-Za-z0-9]+)", re.IGNORECASE)

_UNIT_TO_RACE_TYPE = {
    "1M": "1 Mile", "2M": "2 Mile", "4M": "4 Mile", "5M": "5 Mile", "10M": "10 Mile",
    "5K": "5K", "8K": "8K", "10K": "10K", "15K": "15K", "20K": "20K",
    "HALF": "Half Marathon", "MAR": "Marathon",
}


def extract_event_code(url):
    match = _EVENT_CODE.search(url or "")
    return match.group(1) if match else None


def race_type_from_unit(unit_code, distance_name):
    """'MAR' -> 'Marathon'; unknown codes fall back to the API's distanceName."""
    return _UNIT_TO_RACE_TYPE.get((unit_code or "").upper(), distance_name or "")


class NyrrProvider(RaceProvider):
    name = "nyrr"

    def matches(self, url: str) -> bool:
        return "results.nyrr.org" in url.lower()

    def _details(self, event_code, session=None):
        data = core.post_json(NYRR_API + "/events/details", {"eventCode": event_code}, session=session)
        return data.get("eventDetails") or {}

    def list_events(self, url: str, session=None) -> List[EventRef]:
        event_code = extract_event_code(url)
        if not event_code:
            raise ValueError(f"Could not find an NYRR event code in {url}")

        details = self._details(event_code, session=session)
        this_name = details.get("eventName") or event_code
        group = race_group_key(this_name)

        search = core.post_json(
            NYRR_API + "/events/search",
            {"year": None, "searchString": race_group_label(this_name), "distance": None,
             "pageIndex": 1, "pageSize": NYRR_PAGE_SIZE},
            session=session,
        )

        refs = []
        for item in search.get("items", []):
            if race_group_key(item.get("eventName", "")) != group:
                continue
            refs.append(EventRef(
                source=self.name,
                event_id=item["eventCode"],
                name=item.get("eventName") or "",
                date_str=(item.get("startDateTime") or "Unknown")[:10],
                race_group=group,
                race_type=race_type_from_unit(item.get("distanceUnitCode"), item.get("distanceName")),
            ))

        refs.sort(key=lambda r: r.date_str, reverse=True)
        return refs

    def fetch_event(self, ref: EventRef, session=None):
        rows = []
        for page_index in range(1, core.MAX_PAGES + 1):
            data = core.post_json(
                NYRR_API + "/runners/finishers-filter",
                {"eventCode": ref.event_id, "pageIndex": page_index, "pageSize": NYRR_PAGE_SIZE,
                 "sortColumn": "overallPlace", "sortDescending": False},
                session=session,
            )
            items = data.get("items") or []
            for f in items:
                rows.append({
                    "Source": self.name,
                    "Race Group": ref.race_group,
                    "Event ID": ref.event_id,
                    "Event Name": ref.name,
                    "Event Date": ref.date_str,
                    "Race Type": ref.race_type,
                    "Name": f"{f.get('firstName', '')} {f.get('lastName', '')}".strip(),
                    "Gender": f.get("gender"),
                    "Age": f.get("age"),
                    "Bib": f.get("bib"),
                    "City": f.get("city"),
                    "State": f.get("stateProvince"),
                    "Country": f.get("countryCode"),
                    "Time": f.get("overallTime"),
                    "Pace": f.get("pace"),
                    "Overall Rank": f.get("overallPlace"),
                    "Gender Rank": f.get("genderPlace"),
                    "Division Rank": None,
                    "Status": "CONF",
                })
            print(f"Fetched {len(items)} NYRR results (page {page_index})")
            if len(items) < NYRR_PAGE_SIZE:
                break
            core.time.sleep(core.REQUEST_DELAY_SECONDS)
        return to_canonical(rows)
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_providers_nyrr.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add athlinks_scraper_project/athlinks_scraper/providers/nyrr.py tests/test_providers_nyrr.py
git commit -m "feat(providers): NYRR provider"
```

---

### Task 4: RunSignup provider

**Files:**
- Create: `athlinks_scraper_project/athlinks_scraper/providers/runsignup.py`
- Create: `tests/test_providers_runsignup.py`

**Interfaces:**
- Consumes: `core.fetch_json`, `core.REQUEST_DELAY_SECONDS`, `core.MAX_PAGES`, `core.time`, `race_group_key`, `strip_fractional_seconds`, `EventRef`, `to_canonical`.
- Produces: `RunSignupProvider`, `RUNSIGNUP_API = "https://runsignup.com/Rest"`, `RUNSIGNUP_PAGE_SIZE = 1000`, `extract_race_id(url)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_providers_runsignup.py`:

```python
import pytest

from athlinks_scraper import core
from athlinks_scraper.providers import runsignup as rs
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

RACE = {"race": {"race_id": 100692, "name": "#RUNMARANA Turkey Trot", "events": [
    {"event_id": 1192487, "name": "5k Run/Walk", "start_time": "11/14/2026 08:20", "distance": "3.1 Miles"},
    {"event_id": 1024596, "name": "5k Run/Walk", "start_time": "11/15/2025 08:20", "distance": "3.1 Miles"},
    {"event_id": 1024595, "name": "10k Run/Walk", "start_time": "11/15/2025 08:15", "distance": "6.2 Miles"},
]}}


def result(i):
    return {"result_id": i, "place": i, "bib": 500 + i, "first_name": f"F{i}", "last_name": f"L{i}",
            "gender": "M", "city": "Marana", "state": "AZ", "country_code": "US",
            "clock_time": "21:53.15", "chip_time": "21:52.05", "pace": "7:03", "age": 44}


def results_payload(n):
    return {"individual_results_sets": [{"individual_result_set_name": "Overall", "public_results": "T",
                                         "results": [result(i + 1) for i in range(n)]}]}


def test_extract_race_id():
    assert rs.extract_race_id("https://runsignup.com/Race/Results/100692") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/Results/100692/#resultSetId-604416") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/Register/?raceId=100692") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/AZ/Marana/RunMarana") is None


def test_matches():
    p = rs.RunSignupProvider()
    assert p.matches("https://runsignup.com/Race/Results/100692")
    assert not p.matches("https://www.athlinks.com/event/1")


def test_list_events_one_ref_per_event_newest_first():
    session = FakeSession([FakeResponse(RACE)])

    refs = rs.RunSignupProvider().list_events("https://runsignup.com/Race/Results/100692", session=session)

    assert session.calls[0][0] == rs.RUNSIGNUP_API + "/race/100692"
    assert session.calls[0][1] == {"format": "json"}
    # event_id carries both ids because the results endpoint needs the race id too.
    assert [r.event_id for r in refs] == ["100692:1192487", "100692:1024596", "100692:1024595"]
    assert refs[1] == EventRef(source="runsignup", event_id="100692:1024596",
                               name="#RUNMARANA Turkey Trot - 5k Run/Walk",
                               date_str="2025-11-15", race_group="runmarana-turkey-trot", race_type="5k Run/Walk")


def test_list_events_bad_url_raises():
    with pytest.raises(ValueError):
        rs.RunSignupProvider().list_events("https://runsignup.com/Race/AZ/Marana/RunMarana")


def test_fetch_event_pages_and_maps_fields(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(rs, "RUNSIGNUP_PAGE_SIZE", 2)
    session = FakeSession([FakeResponse(results_payload(2)), FakeResponse(results_payload(1))])
    ref = EventRef(source="runsignup", event_id="100692:1024596", name="#RUNMARANA Turkey Trot - 5k Run/Walk",
                   date_str="2025-11-15", race_group="runmarana-turkey-trot", race_type="5k Run/Walk")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert len(df) == 3
    assert list(df.columns) == CANONICAL_COLUMNS
    urls = [c[0] for c in session.calls]
    assert urls == [rs.RUNSIGNUP_API + "/race/100692/results/get-results"] * 2
    assert [c[1]["page"] for c in session.calls] == [1, 2]
    assert all(c[1]["event_id"] == "1024596" for c in session.calls)
    first = df.iloc[0]
    assert first["Name"] == "F1 L1"
    assert first["Time"] == "21:52"          # chip time, fraction stripped
    assert first["Pace"] == "7:03"
    assert first["Overall Rank"] == 1
    assert first["Bib"] == "501"
    assert first["State"] == "AZ"
    assert first["Country"] == "US"
    assert first["Race Type"] == "5k Run/Walk"
    assert first["Status"] == "CONF"


def test_fetch_event_no_results_yet(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse({"individual_results_sets": []})])
    ref = EventRef(source="runsignup", event_id="100692:1192487", name="n", date_str="2026-11-14",
                   race_group="g", race_type="5k")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert len(df) == 0


def test_fetch_event_falls_back_to_clock_time(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    row = result(1)
    row["chip_time"] = None
    payload = {"individual_results_sets": [{"public_results": "T", "results": [row]}]}
    session = FakeSession([FakeResponse(payload)])
    ref = EventRef(source="runsignup", event_id="100692:1", name="n", date_str="2025-11-15", race_group="g")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert df.loc[0, "Time"] == "21:53"
```

Design note: RunSignup's results endpoint needs **both** the race id and the event id, so for this provider `EventRef.event_id` is the string `"{race_id}:{event_id}"` (e.g. `"100692:1024596"`). The provider splits it back apart in `fetch_event`.

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_providers_runsignup.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Create `providers/runsignup.py`**

```python
"""RunSignup provider (runsignup.com).

API facts (verified 2026-09-06): base https://runsignup.com/Rest, GET with
format=json, no API key for public results. A RunSignup "race" spans years;
each "event" is one distance in one year. The results endpoint needs both
ids, so EventRef.event_id is "{race_id}:{event_id}".
"""
import re
from datetime import datetime
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical
from .naming import race_group_key, strip_fractional_seconds

RUNSIGNUP_API = "https://runsignup.com/Rest"
RUNSIGNUP_PAGE_SIZE = 1000

_RACE_ID_PATH = re.compile(r"runsignup\.com/Race/Results/(\d+)", re.IGNORECASE)
_RACE_ID_QUERY = re.compile(r"[?&]raceId=(\d+)", re.IGNORECASE)


def extract_race_id(url):
    for pattern in (_RACE_ID_PATH, _RACE_ID_QUERY):
        match = pattern.search(url or "")
        if match:
            return match.group(1)
    return None


def _date_from_start_time(start_time):
    """'11/15/2025 08:20' -> '2025-11-15'; unparseable -> 'Unknown'."""
    if not start_time:
        return "Unknown"
    try:
        return datetime.strptime(start_time.split(" ")[0], "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return "Unknown"


def _split_ids(ref_event_id):
    race_id, _, event_id = ref_event_id.partition(":")
    if not event_id:
        raise ValueError(f"RunSignup event id must look like 'race:event', got {ref_event_id!r}")
    return race_id, event_id


class RunSignupProvider(RaceProvider):
    name = "runsignup"

    def matches(self, url: str) -> bool:
        return "runsignup.com" in url.lower()

    def list_events(self, url: str, session=None) -> List[EventRef]:
        race_id = extract_race_id(url)
        if not race_id:
            raise ValueError(
                f"Could not find a RunSignup race id in {url}. "
                "Use the results page URL, e.g. https://runsignup.com/Race/Results/100692"
            )
        data = core.fetch_json(f"{RUNSIGNUP_API}/race/{race_id}", params={"format": "json"}, session=session)
        race = data.get("race") or {}
        race_name = race.get("name") or f"RunSignup race {race_id}"
        group = race_group_key(race_name)

        refs = []
        for ev in race.get("events") or []:
            refs.append(EventRef(
                source=self.name,
                event_id=f"{race_id}:{ev['event_id']}",
                name=f"{race_name} - {ev.get('name', '')}".strip(" -"),
                date_str=_date_from_start_time(ev.get("start_time")),
                race_group=group,
                race_type=ev.get("name") or "",
            ))
        refs.sort(key=lambda r: r.date_str, reverse=True)
        return refs

    def fetch_event(self, ref: EventRef, session=None):
        race_id, event_id = _split_ids(ref.event_id)
        url = f"{RUNSIGNUP_API}/race/{race_id}/results/get-results"
        rows = []
        for page in range(1, core.MAX_PAGES + 1):
            data = core.fetch_json(
                url,
                params={"format": "json", "event_id": event_id, "num": RUNSIGNUP_PAGE_SIZE, "page": page},
                session=session,
            )
            sets = [s for s in (data.get("individual_results_sets") or []) if s.get("public_results", "T") == "T"]
            results = (sets[0].get("results") or []) if sets else []
            for r in results:
                time_str = strip_fractional_seconds(r.get("chip_time") or r.get("clock_time"))
                rows.append({
                    "Source": self.name,
                    "Race Group": ref.race_group,
                    "Event ID": ref.event_id,
                    "Event Name": ref.name,
                    "Event Date": ref.date_str,
                    "Race Type": ref.race_type,
                    "Name": f"{r.get('first_name', '')} {r.get('last_name', '')}".strip(),
                    "Gender": r.get("gender"),
                    "Age": r.get("age"),
                    "Bib": str(r["bib"]) if r.get("bib") is not None else None,
                    "City": r.get("city"),
                    "State": r.get("state"),
                    "Country": r.get("country_code"),
                    "Time": time_str,
                    "Pace": r.get("pace"),
                    "Overall Rank": r.get("place"),
                    "Gender Rank": None,
                    "Division Rank": None,
                    "Status": "CONF",
                })
            print(f"Fetched {len(results)} RunSignup results (page {page})")
            if len(results) < RUNSIGNUP_PAGE_SIZE:
                break
            core.time.sleep(core.REQUEST_DELAY_SECONDS)
        return to_canonical(rows)
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_providers_runsignup.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add athlinks_scraper_project/athlinks_scraper/providers/runsignup.py tests/test_providers_runsignup.py
git commit -m "feat(providers): RunSignup provider"
```

---

### Task 5: Registry + CLI routing

**Files:**
- Create: `athlinks_scraper_project/athlinks_scraper/providers/registry.py`
- Modify: `athlinks_scraper_project/athlinks_scraper/providers/__init__.py`
- Modify: `athlinks_scraper_project/athlinks_scraper/cli.py`
- Create: `tests/test_registry.py`

**Interfaces:**
- Produces: `detect_provider(url) -> RaceProvider`, `PROVIDERS: List[RaceProvider]`.
- CLI: `athlinks-scraper <url> [-d DIR] [--all-years]` works for all three hosts. `--all-years` scrapes every ref; default scrapes the newest ref only. `-o FILE` still works for a single event.

- [ ] **Step 1: Write the failing tests**

`tests/test_registry.py`:

```python
import pytest

from athlinks_scraper.providers import detect_provider
from athlinks_scraper.providers.athlinks import AthlinksProvider
from athlinks_scraper.providers.nyrr import NyrrProvider
from athlinks_scraper.providers.runsignup import RunSignupProvider


@pytest.mark.parametrize("url, cls", [
    ("https://www.athlinks.com/event/15776", AthlinksProvider),
    ("https://results.nyrr.org/event/24FROSTY/finishers", NyrrProvider),
    ("https://runsignup.com/Race/Results/100692", RunSignupProvider),
])
def test_detect_provider(url, cls):
    assert isinstance(detect_provider(url), cls)


def test_detect_provider_unknown_host():
    with pytest.raises(ValueError):
        detect_provider("https://example.com/results")
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_registry.py -v`
Expected: `ImportError: cannot import name 'detect_provider'`.

- [ ] **Step 3: Create `providers/registry.py`**

```python
from .athlinks import AthlinksProvider
from .base import RaceProvider
from .nyrr import NyrrProvider
from .runsignup import RunSignupProvider

PROVIDERS = [AthlinksProvider(), NyrrProvider(), RunSignupProvider()]


def detect_provider(url: str) -> RaceProvider:
    for provider in PROVIDERS:
        if provider.matches(url):
            return provider
    raise ValueError(
        f"Unsupported URL: {url}. Supported hosts: athlinks.com, results.nyrr.org, runsignup.com"
    )
```

- [ ] **Step 4: Replace `providers/__init__.py` with**

```python
from .base import CANONICAL_COLUMNS, EventRef, RaceProvider, to_canonical  # noqa: F401
from .naming import race_group_key, race_group_label  # noqa: F401
from .registry import PROVIDERS, detect_provider  # noqa: F401
```

- [ ] **Step 5: Rewrite `cli.py`**

Replace the whole file with:

```python
import argparse
import os
import re
import sys

from .providers import detect_provider


def sanitize_filename(name):
    """Removes filesystem-unsafe characters and replaces spaces with underscores."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    return name.replace(' ', '_')


def output_path_for(ref, df, output_dir=None, output_file=None):
    if output_file:
        return output_file
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        year = ref.date_str[:4] if ref.date_str and ref.date_str != "Unknown" else "unknown"
        safe = sanitize_filename(f"{ref.source}_{ref.race_group}_{year}_{ref.event_id}")
        return os.path.join(output_dir, f"{safe}.csv")
    return "results.csv"


def process_ref(provider, ref, output_dir=None, output_file=None):
    print(f"Scraping {ref.name} ({ref.date_str}) from {ref.source}...")
    df = provider.fetch_event(ref)
    if df.empty:
        print(f"No results found for {ref.name}")
        return
    path = output_path_for(ref, df, output_dir, output_file)
    df.to_csv(path, index=False)
    print(f"Successfully saved {len(df)} rows to {path}")


def main():
    parser = argparse.ArgumentParser(description="Scrape race results (Athlinks, NYRR, RunSignup) to CSV.")
    parser.add_argument("url", help="A results URL from athlinks.com, results.nyrr.org or runsignup.com.")
    parser.add_argument("--output", "-o", help="Output CSV filename (single event only).")
    parser.add_argument("--output-dir", "-d", help="Output directory; filenames are generated per event.")
    parser.add_argument("--all-years", action="store_true", help="Scrape every event found, not just the newest.")
    args = parser.parse_args()

    try:
        provider = detect_provider(args.url)
        refs = provider.list_events(args.url)
        if not refs:
            print("No events found for that URL.")
            return

        targets = refs if args.all_years else refs[:1]
        print(f"Found {len(refs)} events on {provider.name}; scraping {len(targets)}.")
        failures = 0
        for ref in targets:
            try:
                process_ref(provider, ref, args.output_dir, args.output)
            except Exception as e:  # keep going; report at the end
                failures += 1
                print(f"Failed to scrape {ref.name}: {e}")
        if failures:
            print(f"Done with {failures} failure(s).")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the suite + compile check**

Run: `python -m pytest tests -v && python -m py_compile athlinks_scraper_project/athlinks_scraper/cli.py && echo OK`
Expected: 71 passed, then `OK`.

- [ ] **Step 7: Manual smoke test (network; skip with a note if offline)**

```bash
athlinks-scraper "https://results.nyrr.org/event/24FROSTY/finishers" -d /tmp/nyrr_smoke
athlinks-scraper "https://runsignup.com/Race/Results/100692" --all-years -d /tmp/rs_smoke
ls /tmp/nyrr_smoke /tmp/rs_smoke
```

Expected: the NYRR run prints `Found N events on nyrr; scraping 1.` then ~34 "Fetched 100 NYRR results" lines and saves one CSV with ~3,368 rows. The RunSignup run saves several CSVs (future events print "No results found").

- [ ] **Step 8: Commit**

```bash
git add athlinks_scraper_project/athlinks_scraper/providers tests/test_registry.py athlinks_scraper_project/athlinks_scraper/cli.py
git commit -m "feat: provider registry and multi-source CLI"
```

---

### Task 6: Dashboard queries — Race Group, robust `to_seconds`, group listing

**Files:**
- Modify: `tests/conftest.py` (fixture columns)
- Modify: `tests/test_dashboard_queries.py` (one test renamed/re-targeted)
- Create: `tests/test_dashboard_groups.py`
- Modify: `dashboard/dashboard_queries.py` — `RESULT_COLUMNS`, `init_db` backfill, `create_enriched_view`, `get_event_names`, delete `get_retention_data`

**Interfaces:**
- Produces:
  - `create_enriched_view(con, selected_group=None)` — filters on `"Race Group"`; accepts keys matching `^[A-Za-z0-9_-]+$`, else `ValueError`.
  - `get_event_names(con) -> List[dict]` with keys `group_key`, `display_name`, `source_name`, `n_years`.
  - `to_seconds` macro handles fractional seconds and `H:MM:SS` with `H = 0`.
  - `backfill_legacy_columns(df, filename) -> df` (adds `Source`/`Race Group` to old files).

- [ ] **Step 1: Update the fixture in `tests/conftest.py`**

Replace `COLUMNS` with:

```python
COLUMNS = [
    "Source", "Race Group", "Event ID", "Event Name", "Event Date", "Race Type", "Name",
    "Gender", "Age", "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status",
]
```

In `_row`, rename the `master_id="111"` parameter to `group="111"` and replace the returned dict's `"Master ID": master_id` entry with two entries `"Source": "athlinks", "Race Group": group`. In `sample_results_df`, change the Zed Other row's `master_id="222"` to `group="222"`.

- [ ] **Step 2: Re-target one existing test in `tests/test_dashboard_queries.py`**

Replace `test_create_enriched_view_rejects_non_numeric_master_id` with:

```python
def test_create_enriched_view_rejects_unsafe_group_key(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)

    with pytest.raises(ValueError):
        dq.create_enriched_view(con, "111' OR '1'='1")


def test_create_enriched_view_accepts_slug_group_key(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)
    dq.create_enriched_view(con, "frosty-5k")  # valid slug, simply matches nothing here

    assert con.execute("SELECT COUNT(*) FROM results_enriched").fetchone()[0] == 0
```

- [ ] **Step 3: Write the new failing tests**

`tests/test_dashboard_groups.py`:

```python
import pandas as pd

import dashboard_queries as dq


def test_to_seconds_handles_fractions_and_zero_hour(db):
    assert db.execute("SELECT to_seconds('21:52.05')").fetchone()[0] == 1312
    assert db.execute("SELECT to_seconds('0:16:03')").fetchone()[0] == 963
    assert db.execute("SELECT to_seconds('05:10')").fetchone()[0] == 310


def test_get_event_names_groups_by_race_group(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)

    groups = dq.get_event_names(con)

    by_key = {g["group_key"]: g for g in groups}
    assert set(by_key) == {"111", "222"}
    assert by_key["111"]["display_name"] == "Test Trot"
    assert by_key["111"]["source_name"] == "athlinks"
    assert by_key["111"]["n_years"] == 2
    assert by_key["222"]["n_years"] == 1


def test_get_event_names_strips_year_from_display(sample_results_df):
    df = sample_results_df.copy()
    df.loc[df["Race Group"] == "222", "Event Name"] = "2023 NYRR Other Trot"
    con = dq.init_db_from_dataframe(df)

    names = {g["group_key"]: g["display_name"] for g in dq.get_event_names(con)}

    assert names["222"] == "Other Trot"


def test_backfill_legacy_columns_from_old_filename():
    old = pd.DataFrame([{"Event Name": "Trot", "Name": "A", "Time": "20:00", "Pace": "6:26",
                         "Event Date": "2022-11-24", "Race Type": "5K", "Master ID": "15776"}])

    df = dq.backfill_legacy_columns(old, "scraped_15776_2022.parquet")

    assert df.loc[0, "Race Group"] == "15776"
    assert df.loc[0, "Source"] == "athlinks"


def test_backfill_legacy_columns_leaves_new_files_alone():
    new = pd.DataFrame([{"Source": "nyrr", "Race Group": "frosty-5k", "Name": "A"}])

    df = dq.backfill_legacy_columns(new, "scraped_nyrr_frosty-5k_2024_24FROSTY.parquet")

    assert df.loc[0, "Race Group"] == "frosty-5k"
    assert df.loc[0, "Source"] == "nyrr"


def test_retention_data_function_removed():
    assert not hasattr(dq, "get_retention_data")
```

- [ ] **Step 4: Run to verify failure**

Run: `python -m pytest tests/test_dashboard_groups.py tests/test_dashboard_queries.py -v`
Expected: failures on `backfill_legacy_columns` (missing), `get_event_names` (KeyError `group_key`), `to_seconds('21:52.05')` (returns None), `get_retention_data` (still present), `accepts_slug_group_key` (ValueError).

- [ ] **Step 5: Edit `dashboard_queries.py`**

5a. Replace the `RESULT_COLUMNS` list with:

```python
RESULT_COLUMNS = [
    "Source", "Race Group", "Event ID", "Event Name", "Event Date", "Race Type", "Name",
    "Gender", "Age", "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status",
]
```

5b. Add this import near the top (after `import pandas as pd`). `app.py` and `conftest.py` both put the scraper package on `sys.path` before importing this module, so the import is safe:

```python
from athlinks_scraper.providers.naming import race_group_label
```

5c. Add `backfill_legacy_columns` right after `extract_master_id_from_filename`:

```python
def backfill_legacy_columns(df, filename):
    """
    Files written before multi-source support have a 'Master ID' column (or
    only the master id in the filename) and no 'Source'/'Race Group'. Fill
    them in so old and new files share one schema.
    """
    if "Race Group" not in df.columns:
        if "Master ID" in df.columns:
            df["Race Group"] = df["Master ID"].astype(str)
        else:
            df["Race Group"] = extract_master_id_from_filename(filename)
    if "Source" not in df.columns:
        df["Source"] = "athlinks"
    return df
```

5d. In `init_db`, replace both occurrences of

```python
            df['Master ID'] = extract_master_id_from_filename(uploaded_file.name)
```
and
```python
                df['Master ID'] = extract_master_id_from_filename(filename)
```

with, respectively,

```python
            df = backfill_legacy_columns(df, uploaded_file.name)
```
and
```python
                df = backfill_legacy_columns(df, filename)
```

5e. Replace the whole `get_event_names` function with:

```python
def get_event_names(con):
    """
    One entry per Race Group: {'group_key', 'display_name', 'source_name', 'n_years'}.
    display_name is the year-stripped event name, overridden by event_metadata.json.
    """
    try:
        df = con.execute("""
            SELECT
                "Race Group" AS group_key,
                FIRST("Event Name") AS event_name,
                FIRST("Source") AS source_name,
                COUNT(DISTINCT YEAR(TRY_CAST("Event Date" AS DATE))) AS n_years
            FROM results
            WHERE "Race Group" IS NOT NULL
            GROUP BY "Race Group"
            ORDER BY event_name ASC
        """).df()
        overrides = load_event_metadata()
        groups = []
        for rec in df.to_dict('records'):
            key = str(rec["group_key"])
            groups.append({
                "group_key": key,
                "display_name": overrides.get(key) or race_group_label(rec["event_name"] or key),
                "source_name": rec["source_name"],
                "n_years": int(rec["n_years"]),
            })
        groups.sort(key=lambda g: g["display_name"].lower())
        return groups
    except Exception as e:
        print(f"Error getting event names: {e}")
        return []
```

5f. In `create_enriched_view`: rename the parameter `selected_master_id` → `selected_group`, replace the macro definition with the fraction-tolerant version, and replace the validation block. The function becomes:

```python
def create_enriched_view(con, selected_group=None):
    """
    Creates or replaces the results_enriched view with parsed seconds, year,
    normalized name and normalized race type. Drops DNFs and impossible times.
    If selected_group is given, only that Race Group is included.

    DuckDB cannot bind parameters inside CREATE VIEW, so the group key is
    validated against a strict slug pattern instead.
    """
    con.execute("""
        CREATE OR REPLACE MACRO to_seconds(txt) AS CAST(
            CASE
                WHEN txt LIKE '%:%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS DOUBLE) * 3600 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS DOUBLE) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 3) AS DOUBLE)
                WHEN txt LIKE '%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS DOUBLE) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS DOUBLE)
                ELSE NULL
            END AS INTEGER)
    """)

    where_clause = """
        "Pace" IS NOT NULL AND "Pace" != ''
        AND "Time" IS NOT NULL
        -- Anyone faster than 12:00 (720 s) is a timing error, not a finisher.
        AND to_seconds("Time") > 720
        AND ("Status" IS NULL OR "Status" != 'DNF')
    """

    if selected_group is not None:
        key = str(selected_group)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", key):
            raise ValueError(f"Race Group key contains unsafe characters: {selected_group!r}")
        where_clause += f" AND \"Race Group\" = '{key}'"

    con.execute(f"""
        CREATE OR REPLACE VIEW results_enriched AS
        SELECT *,
             to_seconds("Pace") as pace_seconds,
             to_seconds("Time") as time_seconds,
             YEAR(TRY_CAST("Event Date" AS DATE)) as event_year,
             CASE
                WHEN TRIM(UPPER("Name")) = 'NESBITT DREW' THEN 'DREW NESBITT'
                ELSE TRIM(UPPER("Name"))
             END as "Name_Normalized",
             CASE
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5k([- ]?(run|walk|run/walk))?$') THEN '5K'
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5[- ]?mil(e|er)([- ]?run)?$') THEN '5 Mile'
                ELSE "Race Type"
             END as "Race Type Normalized"
        FROM results
        WHERE {where_clause}
    """)
```

5g. Delete the entire `get_retention_data` function.

- [ ] **Step 6: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: 78 passed. If `test_enriched_view_filters_fast_dnf_and_other_masters` fails, the fixture edit in Step 1 was incomplete (it must produce `Race Group` values `"111"`/`"222"`).

- [ ] **Step 7: Commit**

```bash
git add tests/conftest.py tests/test_dashboard_queries.py tests/test_dashboard_groups.py dashboard/dashboard_queries.py
git commit -m "feat(dashboard): group by Race Group, legacy backfill, fraction-safe to_seconds"
```

---

### Task 7: New feature queries — report card, head-to-head, returning runners

**Files:**
- Create: `tests/test_dashboard_features.py`
- Modify: `dashboard/dashboard_queries.py` (append four functions)

**Interfaces:**
- Produces:
  - `search_runner_names(con, fragment) -> List[str]` — distinct `Name_Normalized` values containing `fragment` (case-insensitive), sorted.
  - `get_runner_yearly(con, name_norm) -> DataFrame[event_year, Time, Pace, time_seconds, place, field_size, pct_beaten, median_seconds]` for the primary race type.
  - `get_head_to_head(con, name_a, name_b) -> DataFrame[event_year, time_a, time_b, diff_seconds]` (`diff_seconds = a - b`, negative means A was faster).
  - `get_returning_counts(con) -> DataFrame[event_year, new_runners, returning_runners]` (ints).

- [ ] **Step 1: Write the failing tests**

```python
import dashboard_queries as dq


def test_search_runner_names(db):
    assert dq.search_runner_names(db, "al") == ["ALICE FAST"]
    assert dq.search_runner_names(db, "zzz") == []


def test_runner_yearly_alice(db):
    df = dq.get_runner_yearly(db, "ALICE FAST")

    assert df["event_year"].tolist() == [2022, 2023]
    assert df["Time"].tolist() == ["18:00", "17:30"]
    assert df["place"].tolist() == [1, 1]
    assert df["field_size"].tolist() == [3, 4]
    assert df["pct_beaten"].tolist() == [66.7, 75.0]
    assert df["median_seconds"].tolist() == [1500.0, 1620.0]


def test_runner_yearly_last_place_is_zero_pct(db):
    df = dq.get_runner_yearly(db, "CAROL SLOW")
    assert df["place"].tolist() == [3, 4]
    assert df["pct_beaten"].tolist() == [0.0, 0.0]


def test_runner_yearly_unknown_is_empty(db):
    assert dq.get_runner_yearly(db, "NOBODY").empty


def test_head_to_head(db):
    df = dq.get_head_to_head(db, "ALICE FAST", "BOB MID")

    assert df["event_year"].tolist() == [2022, 2023]
    assert df["time_a"].tolist() == ["18:00", "17:30"]
    assert df["time_b"].tolist() == ["25:00", "24:00"]
    assert df["diff_seconds"].tolist() == [-420, -390]


def test_head_to_head_no_shared_years(db):
    assert dq.get_head_to_head(db, "ALICE FAST", "NOBODY").empty


def test_returning_counts(db):
    df = dq.get_returning_counts(db)

    assert df["event_year"].tolist() == [2022, 2023]
    assert df["new_runners"].tolist() == [3, 1]
    assert df["returning_runners"].tolist() == [0, 3]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_dashboard_features.py -v`
Expected: 7 failures, `AttributeError: module 'dashboard_queries' has no attribute 'search_runner_names'` etc.

- [ ] **Step 3: Append to `dashboard_queries.py`**

```python
# --- Feature queries ------------------------------------------------------

_PRIMARY_RACE = """(
    SELECT "Race Type Normalized" FROM results_enriched
    GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
)"""


def search_runner_names(con, fragment):
    """Distinct normalized names containing `fragment` (case-insensitive), sorted."""
    try:
        df = con.execute(
            'SELECT "Name_Normalized" FROM results_enriched WHERE "Name_Normalized" ILIKE ? '
            'GROUP BY "Name_Normalized" ORDER BY "Name_Normalized"',
            [f"%{fragment}%"],
        ).df()
        return df["Name_Normalized"].tolist()
    except Exception as e:
        print(f"Error searching names: {e}")
        return []


def get_runner_yearly(con, name_norm):
    """
    One row per year the runner finished the primary race: their time/pace,
    computed place, field size, % of the field they beat, and the field median.
    """
    try:
        query = f"""
            WITH field AS (
                SELECT event_year, COUNT(*) AS field_size, MEDIAN(time_seconds) AS median_seconds
                FROM results_enriched
                WHERE "Race Type Normalized" = {_PRIMARY_RACE}
                GROUP BY event_year
            ),
            me AS (
                SELECT event_year, "Time", "Pace", time_seconds
                FROM results_enriched
                WHERE "Name_Normalized" = ? AND "Race Type Normalized" = {_PRIMARY_RACE}
            ),
            placed AS (
                SELECT m.*,
                    (SELECT COUNT(*) FROM results_enriched r
                     WHERE r.event_year = m.event_year
                       AND r."Race Type Normalized" = {_PRIMARY_RACE}
                       AND r.time_seconds < m.time_seconds) + 1 AS place
                FROM me m
            )
            SELECT p.event_year, p."Time", p."Pace", p.time_seconds, p.place, f.field_size,
                   ROUND(100.0 * (f.field_size - p.place) / f.field_size, 1) AS pct_beaten,
                   f.median_seconds
            FROM placed p JOIN field f USING (event_year)
            ORDER BY p.event_year
        """
        return con.execute(query, [name_norm]).df()
    except Exception as e:
        print(f"Error getting runner yearly: {e}")
        return pd.DataFrame()


def get_head_to_head(con, name_a, name_b):
    """Years both runners finished the same race type; diff_seconds = A - B."""
    try:
        query = """
            SELECT a.event_year, a."Time" AS time_a, b."Time" AS time_b,
                   a.time_seconds - b.time_seconds AS diff_seconds
            FROM results_enriched a
            JOIN results_enriched b
              ON a.event_year = b.event_year
             AND a."Race Type Normalized" = b."Race Type Normalized"
            WHERE a."Name_Normalized" = ? AND b."Name_Normalized" = ?
            ORDER BY a.event_year
        """
        return con.execute(query, [name_a, name_b]).df()
    except Exception as e:
        print(f"Error getting head to head: {e}")
        return pd.DataFrame()


def get_returning_counts(con):
    """Per year: how many finishers were new vs had raced in an earlier year."""
    try:
        query = """
            WITH first_seen AS (
                SELECT "Name_Normalized", MIN(event_year) AS first_year
                FROM results_enriched GROUP BY "Name_Normalized"
            ),
            appearances AS (
                SELECT DISTINCT "Name_Normalized", event_year FROM results_enriched
            )
            SELECT a.event_year,
                   CAST(SUM(CASE WHEN a.event_year = f.first_year THEN 1 ELSE 0 END) AS INTEGER) AS new_runners,
                   CAST(SUM(CASE WHEN a.event_year > f.first_year THEN 1 ELSE 0 END) AS INTEGER) AS returning_runners
            FROM appearances a JOIN first_seen f USING ("Name_Normalized")
            GROUP BY a.event_year
            ORDER BY a.event_year
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting returning counts: {e}")
        return pd.DataFrame()
```

- [ ] **Step 4: Run the suite**

Run: `python -m pytest tests -v`
Expected: 85 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_dashboard_features.py dashboard/dashboard_queries.py
git commit -m "feat(dashboard): report card, head-to-head and returning-runner queries"
```

---

### Task 8: Theme and components (D5)

No unit tests — these are Streamlit render helpers. Verify with `py_compile` and the manual run in Task 10.

**Files:**
- Create: `dashboard/ui/__init__.py` (empty)
- Create: `dashboard/ui/theme.py`
- Create: `dashboard/ui/components.py`
- Modify: `dashboard/.streamlit/config.toml`

**Interfaces:**
- Produces (all used by Task 9/10):
  - `theme.PALETTE: dict` with keys `paper, paper2, ink, muted, rule, accent, accent2, highlight`.
  - `theme.SERIES: List[str]` — chart colorway.
  - `theme.inject()` — writes the global CSS once.
  - `theme.style_chart(fig) -> fig`.
  - `components.masthead(title, dateline, intro=None)`.
  - `components.section(title, kicker=None)`.
  - `components.stat_tile(label, value, note=None)`.
  - `components.stat_row(items: List[tuple])` — `items` are `(label, value, note)`; renders 2–4 tiles in columns.
  - `components.chart(fig)`.
  - `components.table(df)`.
  - `components.verdict(text)` — one-line callout with an accent left rule.

- [ ] **Step 1: Replace `dashboard/.streamlit/config.toml` with**

```toml
[theme]
primaryColor = "#C8501B"
backgroundColor = "#F6EFE3"
secondaryBackgroundColor = "#EFE5D3"
textColor = "#1B1A17"
font = "sans serif"
```

- [ ] **Step 2: Create `dashboard/ui/theme.py`**

```python
"""Race-day printed program theme: palette, fonts, global CSS, chart styling."""
import streamlit as st

PALETTE = {
    "paper": "#F6EFE3",
    "paper2": "#EFE5D3",
    "ink": "#1B1A17",
    "muted": "#6B6257",
    "rule": "#D9CDB8",
    "accent": "#C8501B",
    "accent2": "#2F5D50",
    "highlight": "#E0A526",
}

SERIES = [PALETTE["ink"], PALETTE["accent"], PALETTE["accent2"], PALETTE["highlight"], PALETTE["muted"]]

FONT_DISPLAY = "'Barlow Condensed', Impact, 'Arial Narrow', sans-serif"
FONT_BODY = "'IBM Plex Sans', system-ui, sans-serif"
FONT_MONO = "'IBM Plex Mono', ui-monospace, Menlo, monospace"

_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=IBM+Plex+Mono:wght@500&family=IBM+Plex+Sans:wght@400;600&display=swap');

html, body, [class*="css"], .stMarkdown, .stDataFrame, .stTextInput, .stSelectbox, .stRadio, .stSlider {{
    font-family: {FONT_BODY};
    color: {PALETTE["ink"]};
}}
h1, h2, h3, h4 {{
    font-family: {FONT_DISPLAY} !important;
    font-weight: 700 !important;
    letter-spacing: 0.01em;
    color: {PALETTE["ink"]};
}}

/* Masthead */
.rp-kicker {{
    font-family: {FONT_MONO};
    font-size: 0.75rem;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: {PALETTE["accent"]};
    margin-bottom: 0.25rem;
}}
.rp-masthead-title {{
    font-family: {FONT_DISPLAY};
    font-size: 4rem;
    line-height: 0.95;
    font-weight: 700;
    color: {PALETTE["ink"]};
    margin: 0;
}}
.rp-dateline {{
    font-family: {FONT_MONO};
    font-size: 0.85rem;
    color: {PALETTE["muted"]};
    margin-top: 0.5rem;
}}
.rp-masthead-rule {{
    border: 0;
    border-top: 3px solid {PALETTE["ink"]};
    margin: 1rem 0 0.5rem 0;
}}
.rp-intro {{
    font-size: 1.05rem;
    line-height: 1.5;
    max-width: 62ch;
    color: {PALETTE["ink"]};
    margin: 0.75rem 0 1.5rem 0;
}}

/* Section headers */
.rp-section {{ margin-top: 2rem; margin-bottom: 0.75rem; }}
.rp-section-title {{
    font-family: {FONT_DISPLAY};
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
    line-height: 1.05;
}}
.rp-section-rule {{
    border: 0;
    border-top: 1px solid {PALETTE["rule"]};
    margin: 0.5rem 0 0 0;
}}

/* Bib-style stat tiles */
.rp-tile {{
    background: {PALETTE["paper2"]};
    border: 2px solid {PALETTE["ink"]};
    padding: 0.9rem 1rem 0.8rem 1rem;
    min-height: 118px;
}}
.rp-tile-label {{
    font-family: {FONT_MONO};
    font-size: 0.7rem;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: {PALETTE["muted"]};
}}
.rp-tile-value {{
    font-family: {FONT_DISPLAY};
    font-size: 3rem;
    font-weight: 700;
    line-height: 1;
    margin: 0.35rem 0 0.2rem 0;
    color: {PALETTE["ink"]};
}}
.rp-tile-note {{
    font-size: 0.85rem;
    color: {PALETTE["muted"]};
}}

/* Verdict callout */
.rp-verdict {{
    border-left: 4px solid {PALETTE["accent"]};
    padding: 0.5rem 0.9rem;
    margin: 0.5rem 0 1rem 0;
    font-size: 1.05rem;
    background: {PALETTE["paper2"]};
}}

/* Tabs: uppercase condensed, accent underline, no pill */
.stTabs [data-baseweb="tab-list"] {{ gap: 1.5rem; border-bottom: 1px solid {PALETTE["rule"]}; }}
.stTabs [data-baseweb="tab"] {{
    font-family: {FONT_DISPLAY};
    font-size: 1.15rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    background: transparent;
    padding: 0.4rem 0;
}}
.stTabs [aria-selected="true"] {{ border-bottom: 3px solid {PALETTE["accent"]}; color: {PALETTE["accent"]}; }}

/* Sidebar */
section[data-testid="stSidebar"] {{ background: {PALETTE["paper2"]}; border-right: 1px solid {PALETTE["rule"]}; }}
</style>
"""


def inject():
    """Writes the global CSS. Call once at the top of app.py."""
    st.markdown(_CSS, unsafe_allow_html=True)


def style_chart(fig):
    """Applies the program look to a Plotly figure and returns it."""
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=PALETTE["ink"]),
        title_font=dict(family="Barlow Condensed", size=22, color=PALETTE["ink"]),
        colorway=SERIES,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(family="IBM Plex Mono", size=11)),
        margin=dict(l=10, r=10, t=50, b=10),
        dragmode=False,
    )
    fig.update_xaxes(showgrid=False, showline=True, linecolor=PALETTE["ink"], zeroline=False,
                     tickfont=dict(family="IBM Plex Mono", size=11), fixedrange=True)
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["rule"], gridwidth=1, zeroline=False,
                     tickfont=dict(family="IBM Plex Mono", size=11), fixedrange=True)
    return fig
```

- [ ] **Step 3: Create `dashboard/ui/components.py`**

```python
"""Reusable render helpers. Every HTML string in the dashboard lives here or in theme.py."""
import html

import streamlit as st

from ui import theme


def _esc(value):
    return html.escape("" if value is None else str(value))


def masthead(title, dateline, intro=None):
    st.markdown(
        f"""
        <div class="rp-kicker">Official results program</div>
        <div class="rp-masthead-title">{_esc(title)}</div>
        <div class="rp-dateline">{_esc(dateline)}</div>
        <hr class="rp-masthead-rule"/>
        """,
        unsafe_allow_html=True,
    )
    if intro:
        st.markdown(f'<div class="rp-intro">{_esc(intro)}</div>', unsafe_allow_html=True)


def section(title, kicker=None):
    kicker_html = f'<div class="rp-kicker">{_esc(kicker)}</div>' if kicker else ""
    st.markdown(
        f"""
        <div class="rp-section">
            {kicker_html}
            <div class="rp-section-title">{_esc(title)}</div>
            <hr class="rp-section-rule"/>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_tile(label, value, note=None):
    note_html = f'<div class="rp-tile-note">{_esc(note)}</div>' if note else ""
    st.markdown(
        f"""
        <div class="rp-tile">
            <div class="rp-tile-label">{_esc(label)}</div>
            <div class="rp-tile-value">{_esc(value)}</div>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_row(items):
    """items: list of (label, value, note) tuples, 2-4 long."""
    cols = st.columns(len(items))
    for col, (label, value, note) in zip(cols, items):
        with col:
            stat_tile(label, value, note)


def verdict(text):
    st.markdown(f'<div class="rp-verdict">{_esc(text)}</div>', unsafe_allow_html=True)


def chart(fig):
    st.plotly_chart(theme.style_chart(fig), use_container_width=True,
                    config={"displayModeBar": False, "scrollZoom": False})


def table(df):
    st.dataframe(df, use_container_width=True, hide_index=True)
```

- [ ] **Step 4: Create empty `dashboard/ui/__init__.py`** (0 bytes).

- [ ] **Step 5: Compile check**

Run: `python -m py_compile dashboard/ui/theme.py dashboard/ui/components.py && echo OK`
Expected: `OK`.

- [ ] **Step 6: Commit**

```bash
git add dashboard/ui dashboard/.streamlit/config.toml
git commit -m "feat(ui): race-day program theme and components"
```

---

### Task 9: Split `app.py` into section modules

Mechanical move. Each section module exposes `render(con)` (report card and predictor also take nothing else). Copy logic from the current `app.py` **as-is** except for the substitutions listed. Locate blocks by the `st.header("...")` / `st.subheader("...")` text they start with, not by line number.

**Substitution rules (apply everywhere while moving):**
1. `st.header("X")` → `section("X")`; `st.subheader("X")` → `section("X")` unless it's inside a two-column layout, where it becomes `st.markdown("**X**")`.
2. Every `st.markdown("""<div style="font-family: 'Lora'...">...</div>""", unsafe_allow_html=True)` intro blurb → **delete it**. (One intro survives, in the masthead.)
3. `display_magazine_card(label, value, caption, color)` → `stat_tile(label, value, caption)` (drop the color).
4. `display_chart(fig)` and `fig = style_chart(fig); st.plotly_chart(fig, use_container_width=True)` → `chart(fig)`.
5. `st.dataframe(df, use_container_width=True)` → `table(df)`.
6. Hardcoded colors in `color_discrete_map` / `color_discrete_sequence` / `line_color` / `font color` → use `theme.PALETTE[...]`: `#2563EB`→`accent2`, `#EF4444`/`#EA580C`/`#C2410C`→`accent`, `#9CA3AF`/`#6B7280`/`#4B5563`→`muted`, `#e09451`→`highlight`, `#111827`→`ink`.
7. The raw-HTML "Predicted Finish" card in the predictor → `stat_tile("Predicted finish", f"{predicted_place}{suffix}", f"Faster than {pct:.1f}% of a typical field of {int(avg_runners)}")` where `suffix` comes from the `ordinal_suffix` helper below.
8. Every query call that `app.py` used to import by bare name (`get_trends(con)`, `get_runner_history(con, x)`, `get_nemesis(...)`, `get_pace_partners(...)`, `get_raw_times(...)`, `get_fastest_by_year(...)`, `get_fastest_by_demographics(...)`, `get_fun_stats(...)`, `get_division_stats(...)`, `get_era_stats(...)`, `get_competitiveness_stats(...)`, `get_distribution(...)`) becomes `dq.<name>(...)`.
9. The "Fastest Time"/"Best Pace" formatting lambdas that do `f"{int(x//60)}:{int(x%60):02d}"` → `dq.format_seconds(x)`.

**Files:**
- Create: `dashboard/sections/__init__.py` (empty)
- Create: `dashboard/sections/overview.py`, `trends.py`, `runner_tools.py`, `hall_of_fame.py`, `predictor.py`, `report_card.py`

Every section module starts with this header:

```python
import pandas as pd
import plotly.express as px
import streamlit as st

import dashboard_queries as dq
from ui import theme
from ui.components import chart, section, stat_row, stat_tile, table, verdict
```

- [ ] **Step 1: `sections/overview.py`**

```python
# header block as above


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
```

- [ ] **Step 2: `sections/trends.py`**

Move the **Performance Trends**, **Pace Distribution**, **Yearly Competitiveness**, and **Advanced Analytics** blocks here inside `def render(con):`, applying the substitution rules. Then add the Returning Runners chart at the end of `render`:

```python
    section("New vs returning", kicker="Who comes back")
    ret = dq.get_returning_counts(con)
    if not ret.empty and len(ret) > 1:
        melted = ret.melt(id_vars=["event_year"], value_vars=["returning_runners", "new_runners"],
                          var_name="Kind", value_name="Runners")
        melted["Kind"] = melted["Kind"].map({"returning_runners": "Returning", "new_runners": "New"})
        fig = px.bar(melted, x="event_year", y="Runners", color="Kind", barmode="stack",
                     title="Field composition by year",
                     color_discrete_map={"Returning": theme.PALETTE["ink"], "New": theme.PALETTE["highlight"]})
        fig.update_layout(xaxis_title="Edition", yaxis_title="Finishers")
        chart(fig)
        latest = ret.iloc[-1]
        share = 100.0 * latest["returning_runners"] / max(1, latest["returning_runners"] + latest["new_runners"])
        verdict(f"In {int(latest['event_year'])}, {share:.0f}% of finishers had run this race before.")
    else:
        st.info("Load at least two editions to see who comes back.")
```

- [ ] **Step 3: `sections/runner_tools.py`**

Move **Runner Lookup**, **Nemesis Finder**, and **Find Your Pace Partners** here inside `render(con)` with the substitutions. Then add Head-to-Head between Nemesis Finder and Pace Partners:

```python
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
```

- [ ] **Step 4: `sections/hall_of_fame.py`**

Move **Hall of Fame** (both tables) and **Fun Stats / Frequent Flyers** here inside `render(con)` with substitutions.

- [ ] **Step 5: `sections/predictor.py`**

Move the whole **Place Predictor** tab body into `render(con)` with substitutions, and add this helper at module top (after the imports):

```python
def ordinal_suffix(n):
    if 10 <= n % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
```

- [ ] **Step 6: `sections/report_card.py`** (new feature, written from scratch)

```python
# header block as above


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
    melted = plot.melt(id_vars=["event_year"], value_vars=["Runner", "Field median"],
                       var_name="Series", value_name="Time")
    fig = px.line(melted, x="event_year", y="Time", color="Series", markers=True,
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
```

- [ ] **Step 7: Compile every section**

Run: `python -m py_compile dashboard/sections/*.py && echo OK`
Expected: `OK`. (These modules import `streamlit`, which is only imported at runtime — `py_compile` does not execute imports, so it works even where Streamlit is broken locally.)

- [ ] **Step 8: Commit**

```bash
git add dashboard/sections
git commit -m "refactor(dashboard): split app into section modules using program components"
```

---

### Task 10: New `app.py` shell with "Add a race" (D4, D5)

**Files:**
- Modify: `dashboard/app.py` (full rewrite)
- Modify: `README.md`

- [ ] **Step 1: Replace `dashboard/app.py` entirely with**

```python
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
```

- [ ] **Step 2: Compile and test**

Run: `python -m py_compile dashboard/app.py && python -m pytest tests -v`
Expected: no traceback; 85 passed.

- [ ] **Step 3: Manual run (required — this is the only end-to-end check of the UI)**

```bash
cd dashboard && streamlit run app.py
```

Walk through and confirm:
1. With existing `scraped_15776_*.parquet` files present, the race picker shows "Branford Turkey Trot · N yr · athlinks" and every tab renders without a red exception box.
2. Sidebar → example "NYRR Frosty 5K" → Fetch all years. Progress runs through 5+ editions; afterwards the picker offers "Frosty 5K · N yr · nyrr" and the masthead dateline shows the year range.
3. Report Card tab: type "kip" or any common fragment → pick a runner → tiles, chart, table, rivals render.
4. Runner Tools → Head to head with two names from the same race.
5. Analytics → "New vs returning" stacked bar appears.
6. Visual check against D5: cream paper background, condensed headline type, bordered tiles with no shadows, accent-underlined tabs, no italic grey blurbs under headers.

Record anything that errored and fix it in this task before committing.

- [ ] **Step 4: Update `README.md`**

Replace the "Quick Start → Scraper" block with:

````markdown
### Scraper
```bash
cd athlinks_scraper_project
pip install -e .
athlinks-scraper "https://www.athlinks.com/event/15776" --all-years -d out/
athlinks-scraper "https://results.nyrr.org/event/24FROSTY/finishers" --all-years -d out/
athlinks-scraper "https://runsignup.com/Race/Results/100692" --all-years -d out/
```

Supported sources: Athlinks (`athlinks.com`), New York Road Runners (`results.nyrr.org`),
RunSignup (`runsignup.com` — use the *Results* page URL, which contains the numeric race id).
None require an API key.
````

- [ ] **Step 5: Commit**

```bash
git add dashboard/app.py README.md
git commit -m "feat(dashboard): add-a-race from any supported URL; program masthead and tabs"
```

---

## Final verification checklist

- [ ] `python -m pytest tests -v` → 85 passed.
- [ ] `python -m py_compile dashboard/app.py dashboard/ui/*.py dashboard/sections/*.py athlinks_scraper_project/athlinks_scraper/**/*.py` → no output.
- [ ] `grep -rn "unsafe_allow_html" dashboard/sections dashboard/app.py` → no matches (all HTML lives in `ui/`).
- [ ] `grep -rn "Master ID" dashboard/app.py dashboard/sections` → no matches.
- [ ] `grep -rn "get_retention_data" dashboard` → no matches.
- [ ] Old file `dashboard/data/scraped_15776_2023.parquet` (if present) still loads and appears as group `15776`.
- [ ] Report which manual steps (Task 5 Step 7, Task 10 Step 3) were run and what they showed.
