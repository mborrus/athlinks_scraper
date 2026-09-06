# Turkey Trot Site Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Athlinks scraper resilient to network problems, fix the known bugs in the Streamlit dashboard, and add a pytest suite so future changes can be verified automatically.

**Architecture:** Two existing Python components stay where they are: the `athlinks_scraper` package (`athlinks_scraper_project/athlinks_scraper/`) fetches JSON from the Athlinks "reignite" API and flattens it to a DataFrame/CSV; the Streamlit dashboard (`dashboard/app.py`) loads CSV/Parquet files into an in-memory DuckDB table and runs SQL in `dashboard/dashboard_queries.py`. We add a root-level `tests/` directory (pytest) that imports both components directly — no Streamlit needed for tests. The scraper gets one shared `requests.Session` with timeouts + retries; the dashboard query layer gets input validation, a reusable SQL macro for time parsing, and the extra fields `app.py` needs for accurate captions.

**Tech Stack:** Python 3.9+ (3.11 in the devcontainer), `requests` + `urllib3.util.retry.Retry`, `pandas`, `duckdb` (1.x), `streamlit`, `plotly`, `pytest`.

**Spec:** There is no separate spec document. The "Background & Findings" section directly below is the spec — every task traces back to a numbered finding there.

## Global Constraints

- Python 3.9 compatible syntax only (no `match` statements, no `X | Y` type unions, no `list[str]` in annotations — use `typing.List` or plain comments).
- Do NOT add new third-party dependencies beyond `pytest` (dev only). `urllib3` is already a transitive dependency of `requests`.
- Do NOT rename or change the signature of any public function that `dashboard/app.py` or `cli.py` already imports: `get_results`, `extract_event_id`, `extract_master_id`, `fetch_master_events`, `fetch_metadata`, and every `get_*`/`init_db`/`create_enriched_view`/`save_custom_event_name` in `dashboard_queries.py`. Adding *optional* keyword arguments is fine.
- Keep the CSV column names exactly as they are (`"Event ID"`, `"Event Name"`, `"Event Date"`, `"Race Type"`, `"Name"`, `"Gender"`, `"Age"`, `"Bib"`, `"City"`, `"State"`, `"Country"`, `"Time"`, `"Pace"`, `"Overall Rank"`, `"Gender Rank"`, `"Division Rank"`, `"Status"`, plus the dashboard-added `"Master ID"`). Users already have CSV/Parquet files in this shape.
- All tests are run from the **repository root** with `python -m pytest tests -v`. Never run pytest from a subdirectory.
- Never hit the real Athlinks API from a test. All HTTP is faked.
- Commit after every task using the exact commit message given in that task.

## Background & Findings

Read this section fully before starting any task. Each finding has an ID that tasks refer to.

**The repo layout:**

```
athlinks_scraper/                       <- repo root (run pytest from here)
├── athlinks_scraper_project/
│   ├── setup.py                        <- pip install -e . installs "athlinks_scraper"
│   └── athlinks_scraper/
│       ├── __init__.py                 <- exports get_results
│       ├── core.py                     <- API calls + parsing (the scraper)
│       └── cli.py                      <- `athlinks-scraper` console command
├── dashboard/
│   ├── app.py                          <- Streamlit UI (imports core.py via sys.path hack)
│   ├── dashboard_queries.py            <- DuckDB SQL; imports only duckdb/pandas/json/os/re
│   ├── requirements.txt                <- streamlit, duckdb, pandas, plotly
│   └── data/                           <- scraped_<masterid>_<year>.parquet files + event_metadata.json
├── test_url.py, test_metadata.py       <- ad-hoc debug scripts, NOT tests. Leave them alone.
└── README.md
```

**How the data flows:** `core.fetch_results(event_id)` pages through `https://reignite-api.athlinks.com/event/{id}/results?from=N&limit=100` until a page contains zero results. Each page is a JSON list of "course" objects; each course has `intervals`, each interval has `distance.meters` and a `results` list. `core.parse_results` flattens that into rows with `Time` formatted as `MM:SS` or `HH:MM:SS` and `Pace` as `M:SS` min/mile. The dashboard's `create_enriched_view` builds a DuckDB view `results_enriched` that parses those strings back into `time_seconds` / `pace_seconds`, drops rows faster than 12:00 (720 s — they're data errors) and DNFs, and filters to one `"Master ID"` (a "master" is the multi-year event; each year is a child "event").

**Findings (the spec):**

- **F1 — No HTTP timeouts.** Every `requests.get(...)` in `core.py` (lines 46, 75, 113) has no `timeout=`. A stalled connection hangs the CLI or the dashboard forever.
- **F2 — No retries.** A single 5xx/429/connection reset aborts the fetch. `fetch_results` swallows the error with `print` + `break` and returns whatever partial pages it got, so the caller silently writes an incomplete CSV.
- **F3 — No pacing.** `fetch_results` loops as fast as possible; a big event is 50+ back-to-back requests. Add a short sleep between pages.
- **F4 — No pagination upper bound.** If the API ever returned the same non-empty page forever, the `while True` loop never exits.
- **F5 — `app.py` duplicated lines.** Line 11 is an exact duplicate of line 10 (the `from athlinks_scraper.core import ...` line). Line 324 duplicates line 323 (`tick_format = "%M:%S"`). Line 522 duplicates line 521 (`fig_depth.update_layout(...)`).
- **F6 — Fake captions.** `app.py:290` shows `"A growing tradition since 2010"` and `app.py:294` shows `f"Course record set in 2019 by {fastest_runner}"` regardless of the data. The year values must come from the query.
- **F7 — SQL string interpolation of user-controlled values.** `dashboard_queries.create_enriched_view` (line 172) interpolates `selected_master_id` into a `CREATE VIEW`; `get_competitiveness_stats` (lines 639–641) interpolates `gender`, `age_min`, `age_max`. **Important constraint discovered during planning:** DuckDB cannot bind parameters inside `CREATE VIEW` — it raises `Binder Error: This type of statement can't be prepared!`. So the view's master-ID filter must be *validated* (digits only) rather than bound. `get_competitiveness_stats` is a plain `SELECT` and *can* use `?` binding, matching the existing pattern in `get_runner_history`.
- **F8 — Wrong "Best Pace" in Frequent Flyers.** `get_fun_stats` uses `MIN("Pace")`, and `"Pace"` is a string. `"10:00" < "9:40"` lexicographically, so anyone who ever ran a ≥10:00 pace is shown that as their "best". Must use `MIN(pace_seconds)` and format it.
- **F9 — Time-parsing SQL repeated 3×.** The same `CASE WHEN x LIKE '%:%:%' THEN ... ELSE ... END` block appears three times in `create_enriched_view` (lines 156–165, 178–186, 189–197). Replace with one DuckDB macro `to_seconds(txt)`.
- **F10 — Dashboard scrape loop aborts entirely on one bad year.** `app.py:183–195` calls `get_results` per event with no per-iteration `try/except`; one failing year loses all subsequent years.
- **F11 — `init_db` is not unit-testable.** It unconditionally reads `dashboard/data/` from disk, so a test can't control its input. It needs an internal `init_db_from_dataframe(df)` that `init_db` calls.
- **F12 — No test suite and no dev requirements.** `test_url.py` / `test_metadata.py` at the root are throwaway scripts with no assertions.
- **F13 — `dashboard/requirements.txt` is missing `requests`** (needed because `app.py` imports `athlinks_scraper.core`) and `pyarrow` (needed by `df.to_parquet` in `app.py:193`). Both happen to arrive transitively via streamlit today; list them explicitly so a future streamlit release can't break the app.

**Out of scope (do not do):** new dashboard features, restyling, changing the CSV schema, timezone handling of event dates, touching `test_url.py`/`test_metadata.py`, adding CI.

---

## File Structure

**Created:**
- `requirements-dev.txt` — pytest + the two components, so one `pip install -r requirements-dev.txt` prepares a dev machine.
- `tests/__init__.py` — empty; makes `tests` a package so pytest's default import mode works.
- `tests/conftest.py` — puts `athlinks_scraper_project/` and `dashboard/` on `sys.path`; provides the `sample_results_df` fixture and a `db` fixture (DuckDB connection with `results_enriched` built for master `"111"`).
- `tests/fakes.py` — `FakeResponse` / `FakeSession` used to simulate the Athlinks API.
- `tests/test_url_parsing.py` — locks `extract_event_id` / `extract_master_id` behaviour.
- `tests/test_parse_results.py` — locks `parse_results` formatting.
- `tests/test_http.py` — session/timeout/retry/pagination tests.
- `tests/test_dashboard_queries.py` — query-layer tests.

**Modified:**
- `athlinks_scraper_project/athlinks_scraper/core.py` — add `build_session`, `get_session`, `fetch_json`; rewrite the three fetch functions to use them; add pacing + page cap.
- `dashboard/dashboard_queries.py` — `init_db_from_dataframe`, `to_seconds` macro, master-ID validation, param binding, `fastest_year`/`first_year`, best-pace fix, `format_seconds` helper.
- `dashboard/app.py` — remove duplicates, real captions, per-event error handling.
- `dashboard/requirements.txt` — add `requests`, `pyarrow`.
- `README.md` — "Running tests" section.

---

### Task 1: Test infrastructure

**Files:**
- Create: `requirements-dev.txt`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_smoke.py`

**Interfaces:**
- Produces: importable modules `athlinks_scraper.core` and `dashboard_queries` inside any test file, with no install step other than `pip install -r requirements-dev.txt`.

- [ ] **Step 1: Create `requirements-dev.txt` at the repo root**

```
# Development / test dependencies. Install from the repo root:
#   pip install -r requirements-dev.txt
pytest>=6.2
-r dashboard/requirements.txt
-e athlinks_scraper_project
```

- [ ] **Step 2: Create `tests/__init__.py`**

Create the file with no content (0 bytes).

- [ ] **Step 3: Create `tests/conftest.py`**

This file only handles imports for now; fixtures are added in later tasks.

```python
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
```

- [ ] **Step 4: Create `tests/test_smoke.py`**

```python
"""Proves the test harness can import both components."""


def test_can_import_scraper():
    from athlinks_scraper import core

    assert callable(core.get_results)


def test_can_import_dashboard_queries():
    import dashboard_queries

    assert callable(dashboard_queries.init_db)
```

- [ ] **Step 5: Install dev requirements and run the smoke tests**

Run from the repo root:

```bash
pip install -r requirements-dev.txt
python -m pytest tests -v
```

Expected: 2 passed. If you see `ModuleNotFoundError: No module named 'dashboard_queries'`, you ran pytest from the wrong directory — `cd` to the repo root.

- [ ] **Step 6: Commit**

```bash
git add requirements-dev.txt tests/__init__.py tests/conftest.py tests/test_smoke.py
git commit -m "test: add pytest harness for scraper and dashboard"
```

---

### Task 2: Lock existing URL-parsing behaviour with tests

These functions already work; we're pinning their behaviour before touching `core.py`. No production code changes in this task.

**Files:**
- Create: `tests/test_url_parsing.py`

**Interfaces:**
- Consumes: `athlinks_scraper.core.extract_event_id(url) -> str | None`, `athlinks_scraper.core.extract_master_id(url) -> str | None`.

- [ ] **Step 1: Write the tests**

```python
from athlinks_scraper.core import extract_event_id, extract_master_id

FULL_RESULTS_URL = (
    "https://www.athlinks.com/event/15776/results/Event/994637/Course/2152769/Results?page=1"
)
MASTER_URL = "https://www.athlinks.com/event/15776"


def test_extract_event_id_from_full_results_url():
    assert extract_event_id(FULL_RESULTS_URL) == "994637"


def test_extract_event_id_returns_none_for_master_url():
    # A bare master URL has no child event id in it.
    assert extract_event_id(MASTER_URL) is None


def test_extract_event_id_lowercase_event_with_results_keyword():
    assert extract_event_id("https://www.athlinks.com/event/994637/results") == "994637"


def test_extract_master_id_from_full_results_url():
    assert extract_master_id(FULL_RESULTS_URL) == "15776"


def test_extract_master_id_from_master_url():
    assert extract_master_id(MASTER_URL) == "15776"


def test_extract_master_id_returns_none_for_non_athlinks_url():
    assert extract_master_id("https://example.com/event/123") is None
```

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/test_url_parsing.py -v`
Expected: 6 passed. (They pass immediately because the code already exists — that's the point: they're a safety net.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_url_parsing.py
git commit -m "test: pin URL parsing behaviour"
```

---

### Task 3: Lock `parse_results` formatting with tests

Again, no production changes — pinning behaviour before the HTTP refactor.

**Files:**
- Create: `tests/test_parse_results.py`

**Interfaces:**
- Consumes: `athlinks_scraper.core.parse_results(data_blocks, metadata=None) -> list[dict]`.

- [ ] **Step 1: Write the tests**

```python
from athlinks_scraper.core import parse_results

# 1669291200000 ms = 2022-11-24 12:00:00 UTC. Noon UTC is chosen so the
# local-time conversion in parse_results lands on 2022-11-24 in every timezone
# from UTC-12 to UTC+11.
METADATA = {"id": 994637, "name": "Test Trot", "start": {"epoch": 1669291200000}}


def make_block(chip_millis, meters=5000, status="CONF"):
    """Builds one 'course' block in the shape the Athlinks results API returns."""
    return {
        "race": {"name": "5K Run"},
        "intervals": [
            {
                "distance": {"meters": meters},
                "results": [
                    {
                        "displayName": "Alice Fast",
                        "gender": "F",
                        "age": 30,
                        "bib": "12",
                        "chipTimeInMillis": chip_millis,
                        "location": {"locality": "Branford", "region": "CT", "country": "USA"},
                        "rankings": {"overall": 1, "gender": 1, "primary": 1},
                        "status": status,
                    }
                ],
            }
        ],
    }


def test_parse_results_basic_row():
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)  # 18:00

    assert len(rows) == 1
    row = rows[0]
    assert row["Event ID"] == 994637
    assert row["Event Name"] == "Test Trot"
    assert row["Event Date"] == "2022-11-24"
    assert row["Race Type"] == "5K Run"
    assert row["Name"] == "Alice Fast"
    assert row["Gender"] == "F"
    assert row["Age"] == 30
    assert row["City"] == "Branford"
    assert row["State"] == "CT"
    assert row["Overall Rank"] == 1
    assert row["Status"] == "CONF"


def test_time_under_one_hour_is_mm_ss():
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)
    assert rows[0]["Time"] == "18:00"


def test_time_over_one_hour_is_hh_mm_ss():
    rows = parse_results([make_block(chip_millis=3_725_000)], METADATA)  # 1:02:05
    assert rows[0]["Time"] == "01:02:05"


def test_pace_is_minutes_per_mile():
    # 18:00 over 5000 m (3.10686 mi) = 5.79 min/mi -> "5:47" (seconds truncated)
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)
    assert rows[0]["Pace"] == "5:47"


def test_missing_distance_gives_empty_pace():
    rows = parse_results([make_block(chip_millis=1080000, meters=None)], METADATA)
    assert rows[0]["Pace"] == ""


def test_no_metadata_gives_empty_event_fields():
    rows = parse_results([make_block(chip_millis=1080000)], metadata=None)
    assert rows[0]["Event ID"] == ""
    assert rows[0]["Event Name"] == ""
    assert rows[0]["Event Date"] == ""


def test_empty_blocks_gives_empty_list():
    assert parse_results([], METADATA) == []
```

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/test_parse_results.py -v`
Expected: 7 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_parse_results.py
git commit -m "test: pin parse_results formatting"
```

---

### Task 4: Shared HTTP session with timeout and retries (F1, F2)

**Files:**
- Create: `tests/fakes.py`
- Create: `tests/test_http.py`
- Modify: `athlinks_scraper_project/athlinks_scraper/core.py` (imports at top; `fetch_master_events` lines 39–67; `fetch_metadata` lines 69–80)

**Interfaces:**
- Produces:
  - `core.DEFAULT_TIMEOUT: int = 15` (seconds)
  - `core.build_session() -> requests.Session` — new session with a `Retry` policy mounted for https.
  - `core.get_session() -> requests.Session` — module-level cached session (lazily built).
  - `core.fetch_json(url, params=None, session=None, timeout=DEFAULT_TIMEOUT) -> Any` — GET, `raise_for_status()`, return parsed JSON. Raises `requests.RequestException` subclasses on failure; does NOT swallow.
  - `fetch_metadata(event_id, session=None)` and `fetch_master_events(master_id, session=None)` gain an optional `session` kwarg (for tests).
- Consumed by Task 5.

- [ ] **Step 1: Create `tests/fakes.py`**

```python
"""Minimal stand-ins for requests.Session / Response used by the HTTP tests.

We fake at the Session level (not with a mocking library) because the code
under test only ever calls session.get(url, params=..., timeout=...) and then
response.raise_for_status() / response.json().
"""
import requests


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("HTTP %d" % self.status_code)

    def json(self):
        return self.payload


class FakeSession:
    """Returns the queued responses in order; records every call made."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []  # list of (url, params, timeout)

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if not self.responses:
            raise AssertionError("FakeSession ran out of queued responses")
        return self.responses.pop(0)
```

- [ ] **Step 2: Write the failing tests for the session helpers**

Create `tests/test_http.py`:

```python
import pytest
import requests

from athlinks_scraper import core
from tests.fakes import FakeResponse, FakeSession


def test_build_session_mounts_retry_policy():
    session = core.build_session()
    adapter = session.get_adapter("https://reignite-api.athlinks.com/")
    retry = adapter.max_retries

    assert retry.total == 3
    assert 503 in retry.status_forcelist
    assert 429 in retry.status_forcelist
    assert retry.backoff_factor > 0


def test_get_session_is_cached():
    assert core.get_session() is core.get_session()


def test_fetch_json_passes_timeout_and_returns_payload():
    session = FakeSession([FakeResponse({"ok": True})])

    result = core.fetch_json("https://x/y", params={"a": 1}, session=session)

    assert result == {"ok": True}
    assert session.calls == [("https://x/y", {"a": 1}, core.DEFAULT_TIMEOUT)]


def test_fetch_json_raises_on_http_error():
    session = FakeSession([FakeResponse({}, status_code=500)])

    with pytest.raises(requests.HTTPError):
        core.fetch_json("https://x/y", session=session)


def test_fetch_metadata_uses_session_and_returns_dict():
    session = FakeSession([FakeResponse({"id": 994637, "name": "Test Trot"})])

    meta = core.fetch_metadata("994637", session=session)

    assert meta == {"id": 994637, "name": "Test Trot"}
    url, _params, timeout = session.calls[0]
    assert url == "https://reignite-api.athlinks.com/event/994637/metadata"
    assert timeout == core.DEFAULT_TIMEOUT


def test_fetch_metadata_returns_empty_dict_on_failure():
    # Metadata is optional enrichment; a failure must not abort the scrape.
    session = FakeSession([FakeResponse({}, status_code=500)])

    assert core.fetch_metadata("994637", session=session) == {}


def test_fetch_master_events_sorts_newest_first():
    payload = {
        "events": [
            {"id": 1, "name": "2021", "start": {"epoch": 1637755200000}},  # 2021-11-24
            {"id": 2, "name": "2022", "start": {"epoch": 1669291200000}},  # 2022-11-24
        ]
    }
    session = FakeSession([FakeResponse(payload)])

    events = core.fetch_master_events("15776", session=session)

    assert [e["id"] for e in events] == [2, 1]
    assert events[0]["date_str"] == "2022-11-24"
    assert session.calls[0][0] == "https://reignite-api.athlinks.com/master/15776/metadata"


def test_fetch_master_events_returns_empty_list_on_failure():
    session = FakeSession([FakeResponse({}, status_code=503)])

    assert core.fetch_master_events("15776", session=session) == []
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python -m pytest tests/test_http.py -v`
Expected: failures with `AttributeError: module 'athlinks_scraper.core' has no attribute 'build_session'` (and similar). If the file fails to import at all, check that `tests/__init__.py` exists so `from tests.fakes import ...` works.

- [ ] **Step 4: Add the session helpers to `core.py`**

Replace the import block at the top of `core.py` (lines 1–5) with:

```python
import time
import requests
import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- HTTP configuration ---------------------------------------------------
# Seconds to wait for the Athlinks API before giving up on one request.
DEFAULT_TIMEOUT = 15
# Seconds to pause between paginated results requests so we don't hammer the API.
REQUEST_DELAY_SECONDS = 0.5
# Hard cap on results pages per event (100 results/page -> 50,000 results).
MAX_PAGES = 500

_SESSION = None


def build_session():
    """
    Creates a requests.Session that automatically retries transient failures.

    Retries up to 3 times on connection errors and on 429/500/502/503/504
    responses, sleeping 1s, 2s, 4s between attempts (backoff_factor=1).
    """
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def get_session():
    """Returns the shared module-level session, creating it on first use."""
    global _SESSION
    if _SESSION is None:
        _SESSION = build_session()
    return _SESSION


def fetch_json(url, params=None, session=None, timeout=DEFAULT_TIMEOUT):
    """
    GETs `url` and returns the parsed JSON body.

    Raises requests.RequestException (or a subclass such as HTTPError) if the
    request ultimately fails after retries. Callers decide whether that is fatal.
    """
    session = session or get_session()
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()
```

Note: `raise_on_status=False` makes urllib3 hand the final 5xx response back to `requests` instead of raising its own `MaxRetryError`, so our `response.raise_for_status()` produces a normal `requests.HTTPError`.

- [ ] **Step 5: Rewrite `fetch_master_events` to use `fetch_json`**

Replace the whole `fetch_master_events` function (currently lines 39–67) with:

```python
def fetch_master_events(master_id, session=None):
    """
    Fetches all child events for a given master event ID.
    Returns a list of event dicts (id, name, date, date_str), newest first.
    Returns [] if the request fails.
    """
    url = f"https://reignite-api.athlinks.com/master/{master_id}/metadata"
    try:
        data = fetch_json(url, session=session)
    except requests.RequestException as e:
        print(f"Error fetching master events: {e}")
        return []

    events = []
    for event in data.get('events', []):
        epoch = (event.get('start') or {}).get('epoch')
        events.append({
            'id': event.get('id'),
            'name': event.get('name'),
            'date': epoch,
            'date_str': pd.to_datetime(epoch, unit='ms').strftime('%Y-%m-%d') if epoch else 'Unknown',
        })

    # Sort by date descending (newest first)
    events.sort(key=lambda x: x['date'] or 0, reverse=True)
    return events
```

- [ ] **Step 6: Rewrite `fetch_metadata` to use `fetch_json`**

Replace the whole `fetch_metadata` function (currently lines 69–80) with:

```python
def fetch_metadata(event_id, session=None):
    """
    Fetches event metadata (name, date, etc.). Returns {} if the request fails,
    because metadata is optional enrichment and should not abort a scrape.
    """
    url = f"https://reignite-api.athlinks.com/event/{event_id}/metadata"
    try:
        return fetch_json(url, session=session)
    except requests.RequestException as e:
        print(f"Warning: Could not fetch metadata: {e}")
        return {}
```

- [ ] **Step 7: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: all tests pass (2 smoke + 6 url + 7 parse + 8 http = 23 passed).

- [ ] **Step 8: Commit**

```bash
git add tests/fakes.py tests/test_http.py athlinks_scraper_project/athlinks_scraper/core.py
git commit -m "feat(scraper): shared session with timeouts and retries"
```

---

### Task 5: Pagination pacing, page cap, and no silent partial results (F2, F3, F4)

**Files:**
- Modify: `tests/test_http.py` (append)
- Modify: `athlinks_scraper_project/athlinks_scraper/core.py` — `fetch_results` (lines 82–137 in the original file) and `get_results` (bottom of file)

**Interfaces:**
- Consumes: `core.fetch_json`, `core.REQUEST_DELAY_SECONDS`, `core.MAX_PAGES` from Task 4.
- Produces: `fetch_results(event_id, session=None) -> list` — raises `requests.RequestException` if a page fails (no more silent `break`). `get_results(url_or_id, session=None)`.

- [ ] **Step 1: Append failing tests to `tests/test_http.py`**

```python
def page(n_results):
    """One results page with n_results runners in a single 5K interval."""
    return [
        {
            "race": {"name": "5K"},
            "intervals": [
                {
                    "distance": {"meters": 5000},
                    "results": [{"displayName": f"R{i}", "chipTimeInMillis": 1_200_000} for i in range(n_results)],
                }
            ],
        }
    ]


def test_fetch_results_pages_until_empty(monkeypatch):
    sleeps = []
    monkeypatch.setattr(core.time, "sleep", lambda s: sleeps.append(s))
    session = FakeSession([FakeResponse(page(100)), FakeResponse(page(7)), FakeResponse(page(0))])

    blocks = core.fetch_results("994637", session=session)

    # Two non-empty pages were kept; the empty page terminated the loop.
    assert len(blocks) == 3  # 3 raw blocks were returned (incl. the empty one); parse_results ignores empties
    assert [c[1]["from"] for c in session.calls] == [0, 100, 200]
    assert all(c[1]["limit"] == 100 for c in session.calls)
    assert all(c[2] == core.DEFAULT_TIMEOUT for c in session.calls)
    # We paused between pages, but not after the final (empty) page.
    assert sleeps == [core.REQUEST_DELAY_SECONDS, core.REQUEST_DELAY_SECONDS]


def test_fetch_results_raises_instead_of_returning_partial(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse(page(100)), FakeResponse({}, status_code=502)])

    with pytest.raises(requests.HTTPError):
        core.fetch_results("994637", session=session)


def test_fetch_results_stops_at_max_pages(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(core, "MAX_PAGES", 3)
    # Every page is non-empty; without the cap this would loop forever.
    session = FakeSession([FakeResponse(page(100)) for _ in range(10)])

    blocks = core.fetch_results("994637", session=session)

    assert len(session.calls) == 3
    assert len(blocks) == 3


def test_get_results_returns_dataframe(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    meta = {"id": 994637, "name": "Test Trot", "start": {"epoch": 1669291200000}}
    session = FakeSession([FakeResponse(meta), FakeResponse(page(2)), FakeResponse(page(0))])

    df = core.get_results("994637", session=session)

    assert list(df["Name"]) == ["R0", "R1"]
    assert df["Event Name"].iloc[0] == "Test Trot"
    assert df["Time"].iloc[0] == "20:00"
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `python -m pytest tests/test_http.py -v -k "fetch_results or get_results"`
Expected: 4 failures (`TypeError: fetch_results() got an unexpected keyword argument 'session'` or an `AssertionError` from `FakeSession` because the old code calls `requests.get` directly).

- [ ] **Step 3: Rewrite `fetch_results`**

Replace the entire `fetch_results` function with:

```python
def fetch_results(event_id, session=None):
    """
    Fetches all results for the given event ID from the Athlinks API,
    following pagination until a page contains zero results.

    Returns the raw list of "course" blocks exactly as the API returned them
    (parse_results flattens them). Pauses REQUEST_DELAY_SECONDS between pages
    and stops after MAX_PAGES as a safety net.

    Raises requests.RequestException if any page cannot be fetched, so callers
    never receive a silently-truncated result set.
    """
    base_url = f"https://reignite-api.athlinks.com/event/{event_id}/results"
    limit = 100
    from_index = 0
    all_data_blocks = []

    print(f"Fetching results for Event ID: {event_id}...")

    for page_number in range(MAX_PAGES):
        params = {"correlationId": "", "from": from_index, "limit": limit}
        data = fetch_json(base_url, params=params, session=session)

        batch_results_count = 0
        if isinstance(data, list):
            all_data_blocks.extend(data)
            for course in data:
                for interval in course.get('intervals', []):
                    batch_results_count += len(interval.get('results', []))

        print(f"Fetched {batch_results_count} results")

        if batch_results_count == 0:
            break

        from_index += limit
        if page_number + 1 < MAX_PAGES:
            time.sleep(REQUEST_DELAY_SECONDS)
    else:
        print(f"Warning: stopped after {MAX_PAGES} pages; results may be incomplete.")

    return all_data_blocks
```

Notes for the implementer:
- The `for ... else` runs the `else` block only when the loop was **not** exited by `break` — i.e. we hit the page cap.
- We sleep *after* deciding to continue, so there is no pause after the final empty page (the test checks this).
- Do not wrap `fetch_json` in `try/except` here. Letting the exception propagate is the fix for F2.

- [ ] **Step 4: Update `get_results` to accept and forward `session`**

Replace the `get_results` function at the bottom of the file with:

```python
def get_results(url_or_id, session=None):
    """
    Main entry point. Takes a URL or Event ID, fetches results, and returns a DataFrame.
    Raises requests.RequestException if the results cannot be fetched.
    """
    if str(url_or_id).isdigit():
        event_id = url_or_id
    else:
        event_id = extract_event_id(url_or_id)

    metadata = fetch_metadata(event_id, session=session)
    raw_data = fetch_results(event_id, session=session)
    return results_to_df(raw_data, metadata)
```

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: 27 passed.

- [ ] **Step 6: Manual smoke test against the real API (one event, once)**

Run from the repo root:

```bash
athlinks-scraper "https://www.athlinks.com/event/15776" -d /tmp/trot_smoke
ls -la /tmp/trot_smoke
```

Expected: console shows `Detected Master Event ID: 15776`, then `Fetched 100 results` lines with ~0.5 s gaps, ending with `Successfully saved N rows to /tmp/trot_smoke/<name>.csv` where N > 100. If the network is unavailable, note that in your report and move on — the unit tests are the gate.

- [ ] **Step 7: Commit**

```bash
git add tests/test_http.py athlinks_scraper_project/athlinks_scraper/core.py
git commit -m "feat(scraper): pace pagination, cap pages, raise on partial fetch"
```

---

### Task 6: Testable DuckDB setup + sample data fixture (F11)

**Files:**
- Modify: `tests/conftest.py` (append fixtures)
- Modify: `dashboard/dashboard_queries.py` — `init_db` (lines 5–83)
- Create: `tests/test_dashboard_queries.py`

**Interfaces:**
- Produces:
  - `dashboard_queries.init_db_from_dataframe(df: pd.DataFrame) -> duckdb.DuckDBPyConnection` — registers `df` as table `results`.
  - `init_db(uploaded_files)` keeps its signature and now ends by calling `init_db_from_dataframe(full_df)`.
  - Fixtures `sample_results_df` and `db` for later tasks. `db` is a connection where `results_enriched` already exists, filtered to master `"111"`.

- [ ] **Step 1: Append the fixtures to `tests/conftest.py`**

Add below the existing `sys.path` code:

```python
import pandas as pd
import pytest

COLUMNS = [
    "Event ID", "Event Name", "Event Date", "Race Type", "Name", "Gender", "Age",
    "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status", "Master ID",
]


def _row(event_date, name, gender, age, time_str, pace_str, rank, status="CONF",
         master_id="111", event_name="Test Trot", race_type="5K"):
    return {
        "Event ID": "1", "Event Name": event_name, "Event Date": event_date,
        "Race Type": race_type, "Name": name, "Gender": gender, "Age": age,
        "Bib": str(rank), "City": "Branford", "State": "CT", "Country": "USA",
        "Time": time_str, "Pace": pace_str, "Overall Rank": rank,
        "Gender Rank": rank, "Division Rank": rank, "Status": status,
        "Master ID": master_id,
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
             master_id="222", event_name="Other Trot"),
    ]
    return pd.DataFrame(rows, columns=COLUMNS)


@pytest.fixture
def db(sample_results_df):
    """DuckDB connection with results_enriched built for master '111'."""
    import dashboard_queries

    con = dashboard_queries.init_db_from_dataframe(sample_results_df)
    dashboard_queries.create_enriched_view(con, "111")
    return con
```

- [ ] **Step 2: Write the failing tests in `tests/test_dashboard_queries.py`**

```python
import dashboard_queries as dq


def test_init_db_from_dataframe_registers_results_table(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)

    count = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    assert count == len(sample_results_df)


def test_enriched_view_filters_fast_dnf_and_other_masters(db):
    names = db.execute(
        'SELECT "Name" FROM results_enriched ORDER BY "Event Date", "Overall Rank"'
    ).df()["Name"].tolist()

    assert names == [
        "Alice Fast", "Bob Mid", "Carol Slow",          # 2022
        "Alice Fast", "Bob Mid", "Frank New", "Carol Slow",  # 2023
    ]


def test_enriched_view_parses_seconds_and_year(db):
    row = db.execute(
        'SELECT time_seconds, pace_seconds, event_year FROM results_enriched '
        'WHERE "Name" = \'Alice Fast\' AND event_year = 2023'
    ).fetchone()

    assert row == (17 * 60 + 30, 5 * 60 + 38, 2023)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python -m pytest tests/test_dashboard_queries.py -v`
Expected: 3 errors/failures with `AttributeError: module 'dashboard_queries' has no attribute 'init_db_from_dataframe'`.

- [ ] **Step 4: Refactor `init_db` in `dashboard_queries.py`**

Replace lines 1–83 (everything from the top of the file through the end of `init_db`) with:

```python
import json
import os
import re

import duckdb
import pandas as pd

# Dashboard Queries Module

RESULT_COLUMNS = [
    "Event ID", "Event Name", "Event Date", "Race Type", "Name", "Gender", "Age",
    "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status", "Master ID",
]


def extract_master_id_from_filename(filename):
    """scraped_15776_2023.parquet -> '15776'; anything else -> None."""
    match = re.search(r'scraped_(\d+)_', filename)
    if match:
        return match.group(1)
    return None


def init_db_from_dataframe(df):
    """
    Creates an in-memory DuckDB connection with `df` registered as the
    `results` table. This is the seam that tests use.
    """
    con = duckdb.connect(database=':memory:')
    con.register('results', df)
    return con


def init_db(uploaded_files):
    """
    Loads uploaded CSVs plus every CSV/Parquet in dashboard/data/ into one
    DataFrame and returns a DuckDB connection with it registered as `results`.
    """
    dfs = []

    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file)
            df.columns = [c.strip() for c in df.columns]
            df['Master ID'] = extract_master_id_from_filename(uploaded_file.name)
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {uploaded_file.name}: {e}")

    data_dir = os.path.join(os.path.dirname(__file__), "data")
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            if not (filename.endswith(".parquet") or filename.endswith(".csv")):
                continue
            try:
                file_path = os.path.join(data_dir, filename)
                if filename.endswith(".parquet"):
                    df = pd.read_parquet(file_path)
                else:
                    df = pd.read_csv(file_path)
                df.columns = [c.strip() for c in df.columns]
                df['Master ID'] = extract_master_id_from_filename(filename)
                dfs.append(df)
            except Exception as e:
                print(f"Error loading local file {filename}: {e}")

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
    else:
        # Empty frame with the expected columns so views can still be created.
        full_df = pd.DataFrame(columns=RESULT_COLUMNS)

    return init_db_from_dataframe(full_df)
```

Then delete the now-redundant `import json` / `import os` lines that sat just above `get_metadata_path` (originally lines 86–87), since they're now at the top of the file.

- [ ] **Step 5: Run the tests**

Run: `python -m pytest tests -v`
Expected: 30 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/conftest.py tests/test_dashboard_queries.py dashboard/dashboard_queries.py
git commit -m "refactor(dashboard): make init_db testable via init_db_from_dataframe"
```

---

### Task 7: `to_seconds` macro, master-ID validation, parameter binding (F7, F9)

**Files:**
- Modify: `tests/test_dashboard_queries.py` (append)
- Modify: `dashboard/dashboard_queries.py` — `create_enriched_view`, `get_competitiveness_stats`

**Interfaces:**
- Produces:
  - DuckDB macro `to_seconds(txt)` available on any connection after `create_enriched_view` runs (returns `INTEGER` seconds or `NULL`).
  - `create_enriched_view(con, selected_master_id=None)` raises `ValueError` when `selected_master_id` is not all digits.
  - `get_competitiveness_stats(con, gender="All", age_min=0, age_max=100)` uses `?` binding.

- [ ] **Step 1: Append failing tests**

```python
import pytest


def test_create_enriched_view_rejects_non_numeric_master_id(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)

    with pytest.raises(ValueError):
        dq.create_enriched_view(con, "111' OR '1'='1")


def test_create_enriched_view_accepts_integer_master_id(sample_results_df):
    # The selectbox may hand us an int rather than a str; both must work.
    con = dq.init_db_from_dataframe(sample_results_df)
    dq.create_enriched_view(con, 111)

    assert con.execute("SELECT COUNT(*) FROM results_enriched").fetchone()[0] == 7


def test_to_seconds_macro_handles_both_formats(db):
    assert db.execute("SELECT to_seconds('01:02:05')").fetchone()[0] == 3725
    assert db.execute("SELECT to_seconds('25:00')").fetchone()[0] == 1500
    assert db.execute("SELECT to_seconds('garbage')").fetchone()[0] is None


def test_competitiveness_stats_all_genders(db):
    stats = dq.get_competitiveness_stats(db)

    # 3rd place: 2022 Carol 35:00, 2023 Frank 30:00. No year has a 10th finisher.
    assert stats["event_year"].tolist() == [2022, 2023]
    assert stats["time_top_3"].tolist() == [2100, 1800]
    assert stats["time_top_10"].isna().all()


def test_competitiveness_stats_gender_is_bound_not_interpolated(db):
    # With interpolation this string would turn the WHERE clause into a tautology
    # and return every year. With binding it matches no gender -> empty frame.
    stats = dq.get_competitiveness_stats(db, gender="x' OR '1'='1")

    assert stats.empty


def test_competitiveness_stats_age_filter(db):
    # Ages 30-50 in 2023: Alice(31), Bob(46), Frank(33) -> 3rd is Frank 30:00.
    # In 2022 only Alice(30) and Bob(45) qualify -> no 3rd place -> no row.
    stats = dq.get_competitiveness_stats(db, age_min=30, age_max=50)

    assert stats["event_year"].tolist() == [2023]
    assert stats["time_top_3"].tolist() == [1800]
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_dashboard_queries.py -v`
Expected: `test_create_enriched_view_rejects_non_numeric_master_id`, `test_to_seconds_macro_handles_both_formats`, and `test_competitiveness_stats_gender_is_bound_not_interpolated` FAIL. The others may pass already; that's fine.

- [ ] **Step 3: Rewrite `create_enriched_view`**

Replace the entire function with:

```python
def create_enriched_view(con, selected_master_id=None):
    """
    Creates or replaces the results_enriched view with parsed seconds, year,
    normalized name and normalized race type. Drops DNFs and impossible times.
    If selected_master_id is given, only that master event is included.

    selected_master_id must be all digits. DuckDB cannot bind parameters
    inside CREATE VIEW, so we validate instead of interpolating blindly.
    """
    # Reusable "MM:SS" / "HH:MM:SS" -> integer seconds parser.
    con.execute("""
        CREATE OR REPLACE MACRO to_seconds(txt) AS
            CASE
                WHEN txt LIKE '%:%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS INTEGER) * 3600 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS INTEGER) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 3) AS INTEGER)
                WHEN txt LIKE '%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS INTEGER) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS INTEGER)
                ELSE NULL
            END
    """)

    where_clause = """
        "Pace" IS NOT NULL AND "Pace" != ''
        AND "Time" IS NOT NULL
        -- Anyone faster than 12:00 (720 s) is a timing error, not a 5K finisher.
        AND to_seconds("Time") > 720
        -- Exclude DNF (Did Not Finish)
        AND ("Status" IS NULL OR "Status" != 'DNF')
    """

    if selected_master_id is not None:
        master_id_str = str(selected_master_id)
        if not master_id_str.isdigit():
            raise ValueError(f"Master ID must be numeric, got {selected_master_id!r}")
        where_clause += f" AND \"Master ID\" = '{master_id_str}'"

    con.execute(f"""
        CREATE OR REPLACE VIEW results_enriched AS
        SELECT *,
             to_seconds("Pace") as pace_seconds,
             to_seconds("Time") as time_seconds,
             YEAR(CAST("Event Date" AS DATE)) as event_year,

             -- Normalize Name (one known data-entry fix kept from the original)
             CASE
                WHEN TRIM(UPPER("Name")) = 'NESBITT DREW' THEN 'DREW NESBITT'
                ELSE TRIM(UPPER("Name"))
             END as "Name_Normalized",

             -- Normalize Race Type (catch variations of 5k and 5 Mile)
             CASE
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5k([- ]?run)?$') THEN '5K'
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5[- ]?mil(e|er)([- ]?run)?$') THEN '5 Mile'
                ELSE "Race Type"
             END as "Race Type Normalized"
        FROM results
        WHERE {where_clause}
    """)
```

- [ ] **Step 4: Rewrite `get_competitiveness_stats` with parameter binding**

Replace the entire function with:

```python
def get_competitiveness_stats(con, gender="All", age_min=0, age_max=100):
    """
    Returns the 3rd and 10th place finish times (seconds) by year for the
    primary race, filtered by gender ("All", "M" or "F") and age range.
    """
    try:
        params = [age_min, age_max]
        gender_clause = ""
        if gender != "All":
            gender_clause = 'AND "Gender" = ?'
            params.append(gender)

        query = f"""
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched
                GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            ranked AS (
                SELECT
                    event_year,
                    time_seconds,
                    ROW_NUMBER() OVER (PARTITION BY event_year ORDER BY time_seconds ASC) as rn
                FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
                  AND "Age" BETWEEN ? AND ?
                  {gender_clause}
            )
            SELECT
                event_year,
                MAX(CASE WHEN rn = 3 THEN time_seconds END) as time_top_3,
                MAX(CASE WHEN rn = 10 THEN time_seconds END) as time_top_10
            FROM ranked
            WHERE rn IN (3, 10)
            GROUP BY event_year
            ORDER BY event_year
        """
        return con.execute(query, params).df()
    except Exception as e:
        print(f"Error getting competitiveness stats: {e}")
        return pd.DataFrame()
```

Note: the `?` placeholders are consumed in order — age_min, age_max, then gender (if present). The `{gender_clause}` f-string insertion is safe because it is one of two constant strings we wrote, never user input.

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: 36 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/test_dashboard_queries.py dashboard/dashboard_queries.py
git commit -m "fix(dashboard): validate master id, bind query params, add to_seconds macro"
```

---

### Task 8: Real caption data and correct best pace (F6 query side, F8)

**Files:**
- Modify: `tests/test_dashboard_queries.py` (append)
- Modify: `dashboard/dashboard_queries.py` — `get_overview_stats`, `get_fun_stats`; add `format_seconds`

**Interfaces:**
- Produces:
  - `dashboard_queries.format_seconds(total_seconds) -> str` — `1500 -> "25:00"`, `3725 -> "1:02:05"`, `None/NaN -> "N/A"`.
  - `get_overview_stats(con)` DataFrame gains two columns: `fastest_year` (int) and `first_year` (int). Existing columns are unchanged.
  - `get_fun_stats(con)` returns columns `Name`, `race_count`, `best_pace` where `best_pace` is a correctly-ordered `M:SS` string.
- Consumed by Task 9 (`app.py`).

- [ ] **Step 1: Append failing tests**

```python
import math


def test_format_seconds():
    assert dq.format_seconds(1500) == "25:00"
    assert dq.format_seconds(3725) == "1:02:05"
    assert dq.format_seconds(65) == "1:05"
    assert dq.format_seconds(None) == "N/A"
    assert dq.format_seconds(float("nan")) == "N/A"


def test_overview_stats_include_record_year_and_first_year(db):
    stats = dq.get_overview_stats(db)

    assert stats["total_runners"][0] == 7
    assert stats["fastest_time"][0] == "17:30"
    assert stats["fastest_runner"][0] == "Alice Fast"
    assert stats["fastest_year"][0] == 2023
    assert stats["first_year"][0] == 2022
    assert stats["slowest_time"][0] == "35:00"


def test_fun_stats_best_pace_is_numeric_not_lexicographic(db):
    hof = dq.get_fun_stats(db)
    by_name = hof.set_index("Name")

    # Carol ran 11:16 then 9:59. String MIN would wrongly pick "11:16".
    assert by_name.loc["Carol Slow", "best_pace"] == "9:59"
    assert by_name.loc["Carol Slow", "race_count"] == 2
    assert by_name.loc["Alice Fast", "best_pace"] == "5:38"
    # Frank raced once -> excluded (HAVING > 1)
    assert "Frank New" not in by_name.index
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_dashboard_queries.py -v -k "format_seconds or overview or fun_stats"`
Expected: 3 failures (`AttributeError: ... 'format_seconds'`, `KeyError: 'fastest_year'`, and Carol's best pace `== "11:16"`).

- [ ] **Step 3: Add `format_seconds` near the top of `dashboard_queries.py`** (right after `RESULT_COLUMNS`)

```python
def format_seconds(total_seconds):
    """
    1500 -> "25:00"; 3725 -> "1:02:05"; None or NaN -> "N/A".
    Used wherever the UI shows a duration computed in SQL.
    """
    if total_seconds is None or (isinstance(total_seconds, float) and math.isnan(total_seconds)):
        return "N/A"
    total_seconds = int(total_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"
```

and add `import math` to the imports at the top of the file.

- [ ] **Step 4: Rewrite `get_overview_stats`**

Replace the entire function with:

```python
def get_overview_stats(con):
    """
    Headline numbers for the primary race type: total finishers, average pace,
    fastest/slowest time, who set the fastest time and in which year, and the
    first year of data.
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched
                GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            primary_rows AS (
                SELECT * FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
            ),
            fastest AS (
                SELECT "Time", "Name", event_year
                FROM primary_rows ORDER BY time_seconds ASC LIMIT 1
            ),
            slowest AS (
                SELECT "Time" FROM primary_rows ORDER BY time_seconds DESC LIMIT 1
            )
            SELECT
                COUNT(*) as total_runners,
                AVG(pace_seconds) as avg_pace_seconds,
                (SELECT "Time" FROM fastest) as fastest_time,
                (SELECT "Name" FROM fastest) as fastest_runner,
                (SELECT event_year FROM fastest) as fastest_year,
                MIN(event_year) as first_year,
                (SELECT "Time" FROM slowest) as slowest_time
            FROM primary_rows
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting overview stats: {e}")
        return pd.DataFrame()
```

- [ ] **Step 5: Rewrite `get_fun_stats`**

Replace the entire function with:

```python
def get_fun_stats(con):
    """
    "Frequent Flyers": runners who appear in more than one year, with their
    best (lowest) pace. Pace is compared numerically via pace_seconds — the
    "Pace" column is a string and would sort "10:00" before "9:40".
    """
    try:
        df = con.execute("""
            SELECT
                "Name",
                COUNT(DISTINCT event_year) as race_count,
                MIN(pace_seconds) as best_pace_seconds
            FROM results_enriched
            WHERE pace_seconds IS NOT NULL
            GROUP BY "Name_Normalized", "Name"
            HAVING COUNT(DISTINCT event_year) > 1
            ORDER BY race_count DESC, best_pace_seconds ASC
            LIMIT 10
        """).df()
        df["best_pace"] = df["best_pace_seconds"].apply(format_seconds)
        return df[["Name", "race_count", "best_pace"]]
    except Exception as e:
        print(f"Error getting fun stats: {e}")
        return pd.DataFrame()
```

- [ ] **Step 6: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: 39 passed.

- [ ] **Step 7: Commit**

```bash
git add tests/test_dashboard_queries.py dashboard/dashboard_queries.py
git commit -m "fix(dashboard): numeric best pace; expose record year and first year"
```

---

### Task 9: `app.py` cleanup, real captions, resilient scrape loop (F5, F6, F10)

`app.py` is Streamlit UI and is not unit-tested. Verification is `py_compile` plus a manual run. Make each edit exactly as shown; line numbers refer to the file **before** any edits in this task, so apply them bottom-to-top or re-locate by content.

**Files:**
- Modify: `dashboard/app.py`

**Interfaces:**
- Consumes: `get_overview_stats` columns `fastest_year`, `first_year` (Task 8); `get_results` raising on failure (Task 5).

- [ ] **Step 1: Remove the duplicated `update_layout` (line 522)**

Find these two consecutive identical lines inside `with col_adv1:`:

```python
            fig_depth.update_layout(xaxis_title="Age Group", yaxis_title="Runner Count")
            fig_depth.update_layout(xaxis_title="Age Group", yaxis_title="Runner Count")
```

Delete one of them so exactly one remains.

- [ ] **Step 2: Remove the duplicated `tick_format` (line 324)**

Find, inside `if metric_type == "Pace":`:

```python
            tick_format = "%M:%S"
            tick_format = "%M:%S"
```

Delete one so exactly one remains.

- [ ] **Step 3: Replace the fake captions (lines 290 and 294)**

Find:

```python
        with col1:
            display_magazine_card("Total Runners", f"{total:,}", "A growing tradition since 2010", "#2563EB")
        with col2:
            display_magazine_card("Average Pace", avg_pace_fmt, "Steady pace despite growth", "#10B981")
        with col3:
            display_magazine_card("Fastest Time", fastest, f"Course record set in 2019 by {fastest_runner}", "#F59E0B")
```

Replace with:

```python
        first_year = stats["first_year"][0]
        fastest_year = stats["fastest_year"][0]
        first_year_txt = f"since {int(first_year)}" if pd.notna(first_year) else "over the years"
        record_txt = (
            f"Course record set in {int(fastest_year)} by {fastest_runner}"
            if pd.notna(fastest_year) else f"Course record held by {fastest_runner}"
        )

        with col1:
            display_magazine_card("Total Runners", f"{total:,}", f"A growing tradition {first_year_txt}", "#2563EB")
        with col2:
            display_magazine_card("Average Pace", avg_pace_fmt, "Steady pace despite growth", "#10B981")
        with col3:
            display_magazine_card("Fastest Time", fastest, record_txt, "#F59E0B")
```

- [ ] **Step 4: Make the scrape loop survive one bad year (lines 183–195)**

Find:

```python
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
```

Replace with:

```python
                        failed_years = []
                        for i, event in enumerate(events):
                            year = event['date_str'][:4]
                            event_id = event['id']
                            status_text.text(f"Scraping {year}...")

                            try:
                                df = get_results(event_id)
                            except Exception as year_error:
                                # One bad year must not abort the rest of the scrape.
                                failed_years.append(year)
                                st.warning(f"Skipped {year}: {year_error}")
                                df = pd.DataFrame()

                            if not df.empty:
                                # Save to data/
                                filename = os.path.join(os.path.dirname(__file__), "data", f"scraped_{master_id}_{year}.parquet")
                                os.makedirs(os.path.dirname(filename), exist_ok=True)
                                df.to_parquet(filename, index=False)

                            progress_bar.progress((i + 1) / len(events))

                        if failed_years:
                            st.warning(f"Finished with errors. Could not scrape: {', '.join(failed_years)}")
                        else:
                            st.success("Scraping Complete! Refreshing...")
                        st.rerun()
```

- [ ] **Step 5: Remove the duplicated import (line 11)**

Find these two identical consecutive lines near the top:

```python
from athlinks_scraper.core import get_results, extract_master_id, extract_event_id, fetch_master_events, fetch_metadata
from athlinks_scraper.core import get_results, extract_master_id, extract_event_id, fetch_master_events, fetch_metadata
```

Delete one so exactly one remains.

- [ ] **Step 6: Compile check**

Run from the repo root: `python -m py_compile dashboard/app.py && echo OK`
Expected: `OK` with no traceback.

- [ ] **Step 7: Confirm no duplicates remain**

Run: `grep -c "from athlinks_scraper.core import" dashboard/app.py` → expected `1`.
Run: `grep -c "Course record set in 2019" dashboard/app.py` → expected `0`.
Run: `grep -c "since 2010" dashboard/app.py` → expected `0`.

- [ ] **Step 8: Manual run**

```bash
cd dashboard && streamlit run app.py
```

In the browser: with data present (there are parquet files in `dashboard/data/` or use the "Test with Branford Turkey Trot" button), confirm the Overview cards read e.g. "A growing tradition since 2010" **derived from data** and "Course record set in <real year> by <name>", and that the "Frequent Flyers" table's Best Pace column is sorted sensibly (no "10:xx" listed above a "9:xx" for the same runner). Stop the server with Ctrl+C. If Streamlit cannot start on your machine, report that and rely on the compile check.

- [ ] **Step 9: Commit**

```bash
git add dashboard/app.py
git commit -m "fix(dashboard): remove duplicate lines, data-driven captions, per-year scrape errors"
```

---

### Task 10: Requirements and README (F12, F13)

**Files:**
- Modify: `dashboard/requirements.txt`
- Modify: `README.md`

- [ ] **Step 1: Replace `dashboard/requirements.txt` contents with**

```
streamlit
duckdb
pandas
plotly
requests
pyarrow
```

- [ ] **Step 2: Add a testing section to the root `README.md`**

Append at the end of `README.md`:

```markdown

## Development

Install everything (both components plus pytest) from the repository root:

```bash
pip install -r requirements-dev.txt
```

Run the test suite from the repository root (not from a subdirectory):

```bash
python -m pytest tests -v
```

The tests never contact Athlinks; HTTP is faked in `tests/fakes.py`. The dashboard
query layer is tested against a small in-memory DuckDB table built in `tests/conftest.py`.

`test_url.py` and `test_metadata.py` at the repo root are ad-hoc debugging scripts that
hit the live API; they are not part of the test suite.
```

- [ ] **Step 3: Verify a clean install still works**

```bash
pip install -r requirements-dev.txt
python -m pytest tests -v
```

Expected: 39 passed.

- [ ] **Step 4: Commit**

```bash
git add dashboard/requirements.txt README.md
git commit -m "docs: dev requirements and test instructions"
```

---

## Final verification checklist (run after Task 10)

- [ ] `python -m pytest tests -v` from repo root → 39 passed, 0 failed.
- [ ] `python -m py_compile dashboard/app.py athlinks_scraper_project/athlinks_scraper/core.py athlinks_scraper_project/athlinks_scraper/cli.py` → no output.
- [ ] `git log --oneline` shows 10 commits from this plan on top of the branch base.
- [ ] `grep -rn "requests.get(" athlinks_scraper_project/athlinks_scraper/core.py` → no matches (everything goes through `fetch_json`).
- [ ] Report which manual steps (Task 5 Step 6, Task 9 Step 8) were actually run and what they showed.
