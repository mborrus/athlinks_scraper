# Durable Storage (MotherDuck) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scraped results survive Streamlit Community Cloud redeploys by living in a MotherDuck database, while every existing dashboard query keeps running unchanged against a local in-memory DuckDB holding one race at a time.

**Architecture:** A new `dashboard/storage.py` is the only module that reads or writes the durable store (MotherDuck in production, a local `.duckdb` file on a laptop, `:memory:` in tests — same code, different DSN). `app.py` loads the selected race group from the store into the existing `init_db_from_dataframe` seam, so `dashboard_queries.py` query functions and every `sections/*.py` module are untouched. The JSON metadata file and the parquet-per-scrape files go away. A GitHub Actions workflow runs the test suite on every push and pull request.

**Tech Stack:** Python 3.9+, `duckdb>=1.4.1,<1.6` (MotherDuck-supported range), `pandas`, `streamlit`, `pytest`, GitHub Actions. No new third-party dependencies.

**Spec:** `docs/superpowers/specs/2026-09-07-durable-storage-design.md` — read it first. This plan implements it section by section.

## Global Constraints

- Python 3.9 compatible syntax only (`typing.List`, `typing.Optional`, `typing.Dict`; no `match`, no `X | Y`).
- No new third-party dependencies. `duckdb` gets pinned, not added.
- `dashboard/storage.py` must never import `streamlit`. Tests exercise it with `open_store(":memory:")`.
- Import direction is one-way: `dashboard_queries` imports `storage`; `storage` imports only `duckdb`, `pandas`, stdlib.
- Every SQL identifier that is a canonical column name is double-quoted (`"Source"` is a DuckDB reserved word). Every user-supplied value is parameter-bound, never f-string formatted into SQL.
- Never hit the network from a test. There is no MotherDuck account in CI.
- Run tests only from the repo root: `python -m pytest tests -v`. Before Task 1, confirm the current suite passes (85 tests).
- Commit after every task with the exact message given. Commit messages end with the trailer lines your session instructions specify.
- Work on branch `feat/durable-storage-plan` (already contains the spec and this plan). Open a PR to `main` at the end.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `dashboard/storage.py` | create | All reads/writes to the durable store; schema; frame conformance; legacy backfill helpers (moved here) |
| `dashboard/migrate_files.py` | create | One-shot CLI: import any parquet/CSV in a directory into the store |
| `dashboard/dashboard_queries.py` | modify | Drop file loaders and JSON metadata; `get_event_names` reads from store; re-export moved helpers |
| `dashboard/app.py` | modify | Resolve DSN, cache store, load one group, write scrapes/uploads/renames through storage |
| `dashboard/requirements.txt` | modify | Pin `duckdb` |
| `.streamlit/secrets.toml.example` | create | Documents the single secret |
| `.gitignore` | modify | Ignore real secrets and local DuckDB files |
| `.github/workflows/tests.yml` | create | CI: pytest on 3.9 and 3.11 |
| `dashboard/README.md`, `README.md` | modify | Local run + Deploy sections |
| `tests/test_storage.py` | create | All storage behaviour |
| `tests/test_smoke.py` | modify | Assert `storage.open_store` instead of `init_db` |

---

### Task 1: storage module skeleton — schema, open_store, conform, moved helpers

**Files:**
- Create: `dashboard/storage.py`
- Modify: `dashboard/dashboard_queries.py` (lines 35-56: remove `extract_master_id_from_filename` and `backfill_legacy_columns`; add re-export import)
- Create: `tests/test_storage.py`

**Interfaces:**
- Produces: `storage.CANONICAL_COLUMNS: List[str]` (19 names, same order as `dashboard_queries.RESULT_COLUMNS`), `storage.INT_COLUMNS`, `storage.RESULT_TYPES: Dict[str, str]`, `storage.DB_NAME = "turkey_trot"`, `storage.open_store(dsn: str) -> duckdb.DuckDBPyConnection`, `storage.ensure_schema(con)`, `storage.conform(df, filename=None) -> pd.DataFrame`, `storage.backfill_legacy_columns(df, filename)`, `storage.extract_master_id_from_filename(filename)`. `dashboard_queries.backfill_legacy_columns` and `dashboard_queries.extract_master_id_from_filename` remain importable (re-exported).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_storage.py`:

```python
import numpy as np
import pandas as pd
import pytest

import storage


@pytest.fixture
def store():
    return storage.open_store(":memory:")


def _tables(con):
    return {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}


def test_open_store_memory_creates_both_tables(store):
    assert {"results", "event_metadata"} <= _tables(store)


def test_ensure_schema_is_idempotent(store):
    storage.ensure_schema(store)
    storage.ensure_schema(store)
    cols = store.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'results'").fetchall()
    assert len(cols) == len(storage.CANONICAL_COLUMNS) + 1  # + scraped_at


def test_open_store_file_creates_parent_dir(tmp_path):
    path = tmp_path / "nested" / "results.duckdb"
    con = storage.open_store(str(path))
    assert path.exists()
    assert "results" in _tables(con)
    con.close()


def test_conform_adds_missing_columns_and_coerces_types():
    df = pd.DataFrame([{"Source": "athlinks", "Race Group": "1", "Event ID": "9",
                        "Name": "A", "Age": "abc", "Overall Rank": "3", "Bib": 12}])
    out = storage.conform(df)
    assert list(out.columns) == storage.CANONICAL_COLUMNS
    assert pd.isna(out.loc[0, "Age"])
    assert out.loc[0, "Overall Rank"] == 3
    assert out.loc[0, "Bib"] == "12"
    assert out.loc[0, "City"] is None


def test_conform_strips_column_whitespace_and_backfills_legacy():
    df = pd.DataFrame([{" Name ": "A", "Master ID": "15776", "Time": "20:00"}])
    out = storage.conform(df, filename="scraped_15776_2022.parquet")
    assert out.loc[0, "Name"] == "A"
    assert out.loc[0, "Race Group"] == "15776"
    assert out.loc[0, "Source"] == "athlinks"


def test_conform_keeps_nan_as_null_not_string():
    df = pd.DataFrame([{"Source": "nyrr", "Race Group": "g", "Event ID": "e", "City": np.nan}])
    out = storage.conform(df)
    assert out.loc[0, "City"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage.py -v`
Expected: FAIL at collection with `ModuleNotFoundError: No module named 'storage'`

- [ ] **Step 3: Create `dashboard/storage.py`**

```python
"""
The only module that talks to the durable results store.

The store is a DuckDB database reached through a DSN:
  - "md:?motherduck_token=..."   MotherDuck (production on Streamlit Cloud)
  - "/some/path/results.duckdb"   a local file (laptop development)
  - ":memory:"                    tests

Everything here is plain duckdb + pandas. No streamlit imports, ever.
"""
import os
import re
from typing import Dict, List, Optional

import duckdb
import pandas as pd

DB_NAME = "turkey_trot"

CANONICAL_COLUMNS = [
    "Source", "Race Group", "Event ID", "Event Name", "Event Date", "Race Type", "Name",
    "Gender", "Age", "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank",
    "Gender Rank", "Division Rank", "Status",
]
INT_COLUMNS = ["Age", "Overall Rank", "Gender Rank", "Division Rank"]

RESULT_TYPES: Dict[str, str] = {
    col: ("INTEGER" if col in INT_COLUMNS else "VARCHAR") for col in CANONICAL_COLUMNS
}


def _q(name: str) -> str:
    """Double-quote an identifier. "Source" is a DuckDB reserved word."""
    return '"' + name.replace('"', '""') + '"'


COLUMN_LIST_SQL = ", ".join(_q(c) for c in CANONICAL_COLUMNS)


# --- legacy helpers (moved from dashboard_queries.py) --------------------------

def extract_master_id_from_filename(filename):
    """scraped_15776_2023.parquet -> '15776'; anything else -> None."""
    match = re.search(r'scraped_(\d+)_', filename or "")
    if match:
        return match.group(1)
    return None


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


# --- schema / connection ---------------------------------------------------------

def ensure_schema(con) -> None:
    cols = ", ".join(f"{_q(c)} {t}" for c, t in RESULT_TYPES.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS results ({cols}, scraped_at TIMESTAMP)")
    con.execute(
        "CREATE TABLE IF NOT EXISTS event_metadata "
        "(race_group VARCHAR PRIMARY KEY, display_name VARCHAR)"
    )


def open_store(dsn: str):
    """Connects to the store named by `dsn` and guarantees the schema exists."""
    if dsn.startswith("md:"):
        con = duckdb.connect(dsn)
        con.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        con.execute(f"USE {DB_NAME}")
    else:
        if dsn != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(dsn)), exist_ok=True)
        con = duckdb.connect(dsn)
    ensure_schema(con)
    return con


# --- frame conformance -----------------------------------------------------------

def _to_str_or_none(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def conform(df: pd.DataFrame, filename: Optional[str] = None) -> pd.DataFrame:
    """
    Returns a new frame with exactly CANONICAL_COLUMNS in order. Missing
    columns become null. INT_COLUMNS are coerced to nullable Int64 (bad
    values -> null); everything else becomes str with nulls preserved.
    """
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out = backfill_legacy_columns(out, filename)
    for col in CANONICAL_COLUMNS:
        if col not in out.columns:
            out[col] = None
    out = out[CANONICAL_COLUMNS].reset_index(drop=True)
    for col in CANONICAL_COLUMNS:
        if col in INT_COLUMNS:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        else:
            out[col] = pd.Series([_to_str_or_none(v) for v in out[col]], dtype="object")
    return out
```

- [ ] **Step 4: Remove the two helpers from `dashboard/dashboard_queries.py` and re-export them**

Delete lines 35-56 (`extract_master_id_from_filename` and `backfill_legacy_columns`, including their docstrings). Add directly under `from athlinks_scraper.providers.naming import race_group_label`:

```python
from storage import backfill_legacy_columns, extract_master_id_from_filename  # noqa: F401  (re-exported)
```

Do NOT touch `init_db` yet (Task 4 removes it); it still calls `backfill_legacy_columns`, which now resolves through the re-export.

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: all previous tests PASS (including `test_backfill_legacy_columns_*` in `tests/test_dashboard_groups.py`, which now hit the re-export) plus 6 new tests PASS.

- [ ] **Step 6: Commit**

```bash
git add dashboard/storage.py dashboard/dashboard_queries.py tests/test_storage.py
git commit -m "feat(storage): add storage module with schema, open_store and frame conformance"
```

---

### Task 2: save_event, load_group, list_groups

**Files:**
- Modify: `dashboard/storage.py` (append)
- Modify: `tests/test_storage.py` (append)

**Interfaces:**
- Consumes: `open_store`, `conform`, `COLUMN_LIST_SQL`, `_q` from Task 1; `EventRef` from `athlinks_scraper.providers.base` (fields `source, event_id, name, date_str, race_group, race_type`).
- Produces: `save_event(con, df, ref) -> int`, `load_group(con, group_key: str) -> pd.DataFrame`, `list_groups(con) -> pd.DataFrame` with columns `group_key, event_name, source_name, n_years`, and private `_replace_rows(con, source, event_id, conformed_df) -> int` reused by Task 3.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
from athlinks_scraper.providers.base import EventRef


def _ref(event_id="e1", group="g1", source="athlinks", date="2023-11-23"):
    return EventRef(source=source, event_id=event_id, name="Trot", date_str=date, race_group=group)


def _frame(names, group="g1", event_id="e1", source="athlinks", date="2023-11-23"):
    return pd.DataFrame([{
        "Source": source, "Race Group": group, "Event ID": event_id, "Event Name": "Trot",
        "Event Date": date, "Race Type": "5K", "Name": n, "Gender": "F", "Age": 30,
        "Time": "20:00", "Pace": "6:26", "Overall Rank": i + 1, "Status": "CONF",
    } for i, n in enumerate(names)])


def test_save_event_inserts_rows_with_scraped_at(store):
    n = storage.save_event(store, _frame(["A", "B"]), _ref())
    assert n == 2
    rows = store.execute('SELECT "Name", scraped_at FROM results ORDER BY "Name"').fetchall()
    assert [r[0] for r in rows] == ["A", "B"]
    assert all(r[1] is not None for r in rows)


def test_save_event_replaces_same_event_instead_of_appending(store):
    storage.save_event(store, _frame(["A", "B", "C"]), _ref())
    storage.save_event(store, _frame(["D"]), _ref())
    names = [r[0] for r in store.execute('SELECT "Name" FROM results').fetchall()]
    assert names == ["D"]


def test_save_event_keeps_other_events_and_sources(store):
    storage.save_event(store, _frame(["A"]), _ref())
    storage.save_event(store, _frame(["B"], event_id="e2"), _ref(event_id="e2"))
    storage.save_event(store, _frame(["C"], source="nyrr", event_id="e1"), _ref(source="nyrr"))
    storage.save_event(store, _frame(["Z"]), _ref())  # replaces only athlinks/e1
    names = sorted(r[0] for r in store.execute('SELECT "Name" FROM results').fetchall())
    assert names == ["B", "C", "Z"]


def test_save_event_empty_frame_is_noop(store):
    storage.save_event(store, _frame(["A"]), _ref())
    n = storage.save_event(store, pd.DataFrame(), _ref())
    assert n == 0
    assert store.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 1


def test_load_group_returns_only_that_group_with_canonical_columns(store):
    storage.save_event(store, _frame(["A", "B"]), _ref())
    storage.save_event(store, _frame(["C"], group="g2", event_id="e9"), _ref(event_id="e9", group="g2"))
    df = storage.load_group(store, "g1")
    assert list(df.columns) == storage.CANONICAL_COLUMNS
    assert sorted(df["Name"]) == ["A", "B"]
    assert storage.load_group(store, "nope").empty


def test_list_groups_counts_distinct_years(store):
    storage.save_event(store, _frame(["A"], date="2022-11-24"), _ref(date="2022-11-24"))
    storage.save_event(store, _frame(["A"], event_id="e2", date="2023-11-23"), _ref(event_id="e2"))
    storage.save_event(store, _frame(["Q"], group="g2", event_id="x", source="nyrr"),
                       _ref(group="g2", event_id="x", source="nyrr"))
    groups = storage.list_groups(store).set_index("group_key")
    assert int(groups.loc["g1", "n_years"]) == 2
    assert groups.loc["g1", "source_name"] == "athlinks"
    assert groups.loc["g2", "event_name"] == "Trot"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage.py -v -k "save_event or load_group or list_groups"`
Expected: FAIL with `AttributeError: module 'storage' has no attribute 'save_event'`

- [ ] **Step 3: Append the implementation to `dashboard/storage.py`**

```python
# --- writes ------------------------------------------------------------------------

def _replace_rows(con, source: str, event_id: str, conformed: pd.DataFrame) -> int:
    """
    Deletes every row for (source, event_id) then inserts `conformed`.
    One transaction: a failure leaves the old rows in place.
    """
    if conformed.empty:
        return 0
    con.register("_incoming", conformed)
    con.begin()
    try:
        con.execute(
            'DELETE FROM results WHERE "Source" = ? AND "Event ID" = ?',
            [source, event_id],
        )
        con.execute(f"INSERT INTO results SELECT {COLUMN_LIST_SQL}, now() FROM _incoming")
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.unregister("_incoming")
    return int(len(conformed))


def save_event(con, df: pd.DataFrame, ref) -> int:
    """
    Stores one scraped event (an EventRef from a provider). Re-scraping the
    same event replaces its rows. Returns rows inserted; 0 for an empty frame.
    """
    if df is None or df.empty:
        return 0
    conformed = conform(df)
    # The provider frame already carries these, but the ref is authoritative.
    conformed["Source"] = ref.source
    conformed["Event ID"] = str(ref.event_id)
    conformed["Race Group"] = ref.race_group
    return _replace_rows(con, ref.source, str(ref.event_id), conformed)


# --- reads -----------------------------------------------------------------------------

def load_group(con, group_key: str) -> pd.DataFrame:
    return con.execute(
        f'SELECT {COLUMN_LIST_SQL} FROM results WHERE "Race Group" = ?', [group_key]
    ).df()


def list_groups(con) -> pd.DataFrame:
    """One row per Race Group: group_key, event_name, source_name, n_years."""
    return con.execute("""
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
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_storage.py -v`
Expected: all PASS. If `test_save_event_inserts_rows_with_scraped_at` fails with a type error on `Age`, the `Int64` column did not bind — cast with `conformed[col].astype("float").astype("Int64")` is NOT the fix; the correct fix is to make sure `conform` ran (it did if `save_event` was used). Investigate rather than widen types.

- [ ] **Step 5: Commit**

```bash
git add dashboard/storage.py tests/test_storage.py
git commit -m "feat(storage): save_event replaces per event; load_group and list_groups"
```

---

### Task 3: save_frame and import_files (uploads and legacy files)

**Files:**
- Modify: `dashboard/storage.py` (append)
- Modify: `tests/test_storage.py` (append)

**Interfaces:**
- Consumes: `conform`, `_replace_rows` from Tasks 1-2.
- Produces: `save_frame(con, df, filename=None) -> int`, `import_files(con, data_dir: str) -> List[Tuple[str, int]]` (`-1` marks a file that failed to load).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
def test_save_frame_splits_by_event_and_replaces_each(store):
    both = pd.concat([_frame(["A"], event_id="e1"), _frame(["B"], event_id="e2")], ignore_index=True)
    assert storage.save_frame(store, both) == 2
    storage.save_frame(store, _frame(["A2"], event_id="e1"))
    names = sorted(r[0] for r in store.execute('SELECT "Name" FROM results').fetchall())
    assert names == ["A2", "B"]


def test_save_frame_uses_race_group_when_event_id_missing(store):
    legacy = pd.DataFrame([{"Master ID": "15776", "Name": "Old", "Time": "20:00",
                            "Event Date": "2019-11-28", "Event Name": "Trot"}])
    storage.save_frame(store, legacy, filename="scraped_15776_2019.parquet")
    row = store.execute('SELECT "Source", "Race Group", "Event ID" FROM results').fetchone()
    assert row == ("athlinks", "15776", "15776")


def test_import_files_reads_parquet_and_csv_and_isolates_failures(store, tmp_path):
    _frame(["P"], event_id="p").to_parquet(tmp_path / "scraped_athlinks_g1_2023_p.parquet", index=False)
    _frame(["C"], event_id="c").to_csv(tmp_path / "upload.csv", index=False)
    (tmp_path / "broken.parquet").write_bytes(b"not a parquet file")
    (tmp_path / "ignore.txt").write_text("x")
    report = dict(storage.import_files(store, str(tmp_path)))
    assert report["scraped_athlinks_g1_2023_p.parquet"] == 1
    assert report["upload.csv"] == 1
    assert report["broken.parquet"] == -1
    assert "ignore.txt" not in report
    assert store.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 2


def test_import_files_missing_dir_returns_empty(store, tmp_path):
    assert storage.import_files(store, str(tmp_path / "nope")) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage.py -v -k "save_frame or import_files"`
Expected: FAIL with `AttributeError: module 'storage' has no attribute 'save_frame'`

- [ ] **Step 3: Append the implementation to `dashboard/storage.py`**

Add `Tuple` to the `typing` import at the top of the file (`from typing import Dict, List, Optional, Tuple`), then append:

```python
def save_frame(con, df: pd.DataFrame, filename: Optional[str] = None) -> int:
    """
    Stores a frame that did not come from a provider (a CSV upload or a
    legacy parquet file). Rows are grouped by (Source, Event ID) and each
    group replaces its predecessor. A null Event ID falls back to the Race
    Group so old single-year files still replace cleanly.
    """
    if df is None or df.empty:
        return 0
    conformed = conform(df, filename)
    missing = conformed["Event ID"].isna()
    conformed.loc[missing, "Event ID"] = conformed.loc[missing, "Race Group"]
    total = 0
    for (source, event_id), part in conformed.groupby(["Source", "Event ID"], dropna=False, sort=False):
        if source is None or event_id is None:
            continue
        total += _replace_rows(con, str(source), str(event_id), part.reset_index(drop=True))
    return total


def import_files(con, data_dir: str) -> List[Tuple[str, int]]:
    """
    Imports every *.parquet / *.csv in data_dir via save_frame. Returns
    (filename, rows_inserted) per file; -1 means the file could not be read
    or stored. Never raises for a single bad file.
    """
    if not os.path.isdir(data_dir):
        return []
    report: List[Tuple[str, int]] = []
    for filename in sorted(os.listdir(data_dir)):
        if not filename.endswith((".parquet", ".csv")):
            continue
        path = os.path.join(data_dir, filename)
        try:
            if filename.endswith(".parquet"):
                df = pd.read_parquet(path)
            else:
                df = pd.read_csv(path)
            report.append((filename, save_frame(con, df, filename)))
        except Exception as e:  # one bad file must not stop the import
            print(f"import_files: {filename}: {e}")
            report.append((filename, -1))
    return report
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_storage.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard/storage.py tests/test_storage.py
git commit -m "feat(storage): save_frame for uploads and import_files for legacy data"
```

---

### Task 4: metadata in the store; get_event_names reads from it; delete file loaders

**Files:**
- Modify: `dashboard/storage.py` (append)
- Modify: `dashboard/dashboard_queries.py` (delete `init_db`, `get_metadata_path`, `load_event_metadata`, `save_custom_event_name`; rewrite `get_event_names`)
- Modify: `tests/test_smoke.py`
- Modify: `tests/test_storage.py` (append)

**Interfaces:**
- Consumes: `list_groups` from Task 2.
- Produces: `storage.load_event_metadata(con) -> Dict[str, str]`, `storage.save_custom_event_name(con, group_key: str, display_name: str) -> None`. `dashboard_queries.get_event_names(con)` keeps its return shape (`list` of dicts with `group_key, display_name, source_name, n_years`) and works on both store connections and `init_db_from_dataframe` connections. `dashboard_queries.init_db` no longer exists.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
import dashboard_queries as dq


def test_metadata_upsert_and_read(store):
    storage.save_custom_event_name(store, "g1", "  Branford Trot ")
    storage.save_custom_event_name(store, "g1", "Branford Turkey Trot")
    assert storage.load_event_metadata(store) == {"g1": "Branford Turkey Trot"}


def test_load_event_metadata_without_table_returns_empty(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)  # has only `results`
    assert storage.load_event_metadata(con) == {}


def test_get_event_names_reads_store_and_honours_override(store):
    storage.save_event(store, _frame(["A"], date="2022-11-24"), _ref(date="2022-11-24"))
    storage.save_event(store, _frame(["A"], event_id="e2"), _ref(event_id="e2"))
    storage.save_custom_event_name(store, "g1", "My Trot")
    groups = dq.get_event_names(store)
    assert groups == [{"group_key": "g1", "display_name": "My Trot",
                       "source_name": "athlinks", "n_years": 2}]


def test_file_based_loaders_are_gone():
    for name in ("init_db", "get_metadata_path"):
        assert not hasattr(dq, name), name
```

Edit `tests/test_smoke.py`: replace the last test with

```python
def test_can_import_dashboard_storage():
    import storage

    assert callable(storage.open_store)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage.py tests/test_smoke.py -v`
Expected: the four new tests FAIL (`AttributeError` / `assert not hasattr`); the smoke test PASSES already.

- [ ] **Step 3: Append metadata functions to `dashboard/storage.py`**

```python
# --- display-name overrides -------------------------------------------------------

def load_event_metadata(con) -> Dict[str, str]:
    """{race_group: display_name}. {} if the table is absent (plain in-memory cons)."""
    try:
        rows = con.execute("SELECT race_group, display_name FROM event_metadata").fetchall()
    except duckdb.CatalogException:
        return {}
    return {str(k): v for k, v in rows if v}


def save_custom_event_name(con, group_key: str, display_name: str) -> None:
    con.execute(
        "INSERT OR REPLACE INTO event_metadata (race_group, display_name) VALUES (?, ?)",
        [str(group_key), display_name.strip()],
    )
```

- [ ] **Step 4: Rewrite the data-loading part of `dashboard/dashboard_queries.py`**

Delete these functions entirely: `init_db` (the one taking `uploaded_files`), `get_metadata_path`, `load_event_metadata`, `save_custom_event_name`. Keep `RESULT_COLUMNS`, `format_seconds`, `init_db_from_dataframe`, and everything from `create_enriched_view` onward.

Change the top imports so `storage` is imported as a module (keep the re-export line from Task 1):

```python
import storage
from storage import backfill_legacy_columns, extract_master_id_from_filename  # noqa: F401  (re-exported)
```

Replace `get_event_names` with:

```python
def get_event_names(con):
    """
    One entry per Race Group: {'group_key', 'display_name', 'source_name', 'n_years'}.
    display_name is the year-stripped event name, overridden by the
    event_metadata table when present. `con` may be the durable store or a
    plain in-memory connection that only has `results`.
    """
    try:
        df = storage.list_groups(con)
        overrides = storage.load_event_metadata(con)
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

Remove `import json` from the top of the file if nothing else uses it (check with `grep -n "json\." dashboard/dashboard_queries.py`).

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest tests -v`
Expected: all PASS. `tests/test_dashboard_groups.py::test_get_event_names_*` still pass because `load_event_metadata` returns `{}` on the plain connection.

- [ ] **Step 6: Commit**

```bash
git add dashboard/storage.py dashboard/dashboard_queries.py tests/test_storage.py tests/test_smoke.py
git commit -m "feat(storage): move race display names into the store; drop file loaders"
```

---

### Task 5: resolve_dsn and MotherDuck connection branch

**Files:**
- Modify: `dashboard/storage.py` (append; `open_store` already handles `md:` from Task 1)
- Modify: `tests/test_storage.py` (append)

**Interfaces:**
- Produces: `storage.resolve_dsn(secrets=None, env=None, data_dir=None) -> str`, `storage.DEFAULT_DATA_DIR` (absolute path of `dashboard/data`), `storage.LOCAL_DB_FILENAME = "results.duckdb"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
def test_resolve_dsn_prefers_secrets_then_env_then_local_file(tmp_path):
    secrets = {"motherduck": {"token": "SECRET"}}
    env = {"MOTHERDUCK_TOKEN": "ENV"}
    assert storage.resolve_dsn(secrets=secrets, env=env) == "md:?motherduck_token=SECRET"
    assert storage.resolve_dsn(secrets={}, env=env) == "md:?motherduck_token=ENV"
    local = storage.resolve_dsn(secrets={}, env={}, data_dir=str(tmp_path))
    assert local == str(tmp_path / "results.duckdb")


def test_resolve_dsn_ignores_blank_tokens(tmp_path):
    dsn = storage.resolve_dsn(secrets={"motherduck": {"token": "  "}}, env={"MOTHERDUCK_TOKEN": ""},
                              data_dir=str(tmp_path))
    assert dsn.endswith("results.duckdb")


def test_resolve_dsn_default_data_dir_is_dashboard_data():
    dsn = storage.resolve_dsn(secrets={}, env={})
    assert dsn == os.path.join(storage.DEFAULT_DATA_DIR, "results.duckdb")
    assert dsn.endswith(os.path.join("dashboard", "data", "results.duckdb"))
```

Add `import os` to the top of `tests/test_storage.py`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage.py -v -k resolve_dsn`
Expected: FAIL with `AttributeError: module 'storage' has no attribute 'resolve_dsn'`

- [ ] **Step 3: Append to `dashboard/storage.py`**

```python
# --- DSN resolution ----------------------------------------------------------------

DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
LOCAL_DB_FILENAME = "results.duckdb"


def _clean(value) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def resolve_dsn(secrets=None, env=None, data_dir: Optional[str] = None) -> str:
    """
    Picks the store: a MotherDuck token from secrets["motherduck"]["token"],
    else from env["MOTHERDUCK_TOKEN"], else a local DuckDB file under data_dir.
    Pure function: pass plain dicts so it needs no streamlit to test.
    """
    secrets = secrets or {}
    env = env or {}
    token = None
    md = secrets.get("motherduck") if hasattr(secrets, "get") else None
    if md is not None and hasattr(md, "get"):
        token = _clean(md.get("token"))
    if token is None:
        token = _clean(env.get("MOTHERDUCK_TOKEN"))
    if token:
        return f"md:?motherduck_token={token}"
    return os.path.join(data_dir or DEFAULT_DATA_DIR, LOCAL_DB_FILENAME)
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard/storage.py tests/test_storage.py
git commit -m "feat(storage): resolve_dsn picks MotherDuck from secrets/env, else a local file"
```

---

### Task 6: app.py on the store; migrate_files CLI; deployment files

**Files:**
- Modify: `dashboard/app.py` (full rewrite of lines 1-111; sections block unchanged)
- Create: `dashboard/migrate_files.py`
- Modify: `dashboard/requirements.txt`
- Create: `.streamlit/secrets.toml.example`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: everything in `storage` from Tasks 1-5; `dq.get_event_names`, `dq.init_db_from_dataframe`, `dq.create_enriched_view`.
- Produces: nothing new for later tasks. This task has no unit test (Streamlit script); it is verified by running the app against a local file store.

- [ ] **Step 1: Replace `dashboard/app.py` lines 1-111 with the following**

Everything from `# --- Masthead ---` to the end of the file stays exactly as it is.

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


@st.cache_resource(show_spinner="Connecting to the results store…")
def get_store(dsn):
    return storage.open_store(dsn)


@st.cache_data(ttl=3600, show_spinner="Loading race…")
def load_group_cached(dsn, group_key):
    # `dsn` is only here so the cache key changes if the store does.
    return storage.load_group(get_store(dsn).cursor(), group_key)


def invalidate_and_rerun():
    load_group_cached.clear()
    st.rerun()


try:
    store = get_store(DSN)
except Exception as e:
    st.error(f"Could not open the results store: {e}")
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
    cur = store.cursor()
    for i, ref in enumerate(refs):
        status.text(f"Scraping {ref.name} ({ref.date_str})…")
        try:
            df = provider.fetch_event(ref)
            storage.save_event(cur, df, ref)
        except Exception as e:
            failed.append(f"{ref.name}: {e}")
        progress.progress((i + 1) / len(refs))
    if failed:
        st.warning("Finished with errors:\n\n" + "\n".join(f"- {f}" for f in failed))
    else:
        st.success("All editions scraped.")
    invalidate_and_rerun()


def import_uploads(files):
    cur = store.cursor()
    total = 0
    for f in files:
        try:
            total += storage.save_frame(cur, pd.read_csv(f), f.name)
        except Exception as e:
            st.error(f"{f.name}: {e}")
    st.success(f"Imported {total:,} rows.")
    invalidate_and_rerun()


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
    if st.button("Import CSVs", disabled=not uploaded_files):
        import_uploads(uploaded_files)

# --- Data ----------------------------------------------------------------------
groups = dq.get_event_names(store.cursor())
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
        storage.save_custom_event_name(store.cursor(), selected_group, new_name)
        invalidate_and_rerun()

con = dq.init_db_from_dataframe(load_group_cached(DSN, selected_group))
dq.create_enriched_view(con, selected_group)
```

- [ ] **Step 2: Create `dashboard/migrate_files.py`**

```python
"""
One-shot import of legacy scraped files into the results store.

    python dashboard/migrate_files.py                 # imports dashboard/data/*.parquet|csv
    python dashboard/migrate_files.py --data-dir PATH

The store is chosen the same way the app chooses it: MOTHERDUCK_TOKEN in the
environment (or [motherduck] token in .streamlit/secrets.toml) means
MotherDuck; otherwise the local file dashboard/data/results.duckdb.
Exit status is 1 if any file failed.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import storage  # noqa: E402


def _secrets_from_toml():
    path = os.path.join(HERE, "..", ".streamlit", "secrets.toml")
    if not os.path.exists(path):
        return {}
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore
        except ImportError:
            return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default=storage.DEFAULT_DATA_DIR)
    args = parser.parse_args(argv)

    dsn = storage.resolve_dsn(secrets=_secrets_from_toml(), env=os.environ, data_dir=args.data_dir)
    print("store:", "MotherDuck" if dsn.startswith("md:") else dsn)
    con = storage.open_store(dsn)
    report = storage.import_files(con, args.data_dir)
    if not report:
        print("no .parquet/.csv files found in", args.data_dir)
        return 0
    failed = 0
    for filename, rows in report:
        print(f"{filename}: {'FAILED' if rows < 0 else f'{rows} rows'}")
        failed += rows < 0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Pin duckdb in `dashboard/requirements.txt`**

Replace the line `duckdb` with `duckdb>=1.4.1,<1.6`.

- [ ] **Step 4: Create `.streamlit/secrets.toml.example`**

```toml
# Copy to .streamlit/secrets.toml for local use, or paste into the
# "Secrets" panel of the Streamlit Community Cloud app settings.
# Without a token the app falls back to dashboard/data/results.duckdb.
[motherduck]
token = "paste-your-motherduck-token-here"
```

- [ ] **Step 5: Add to `.gitignore`**

Append these lines:

```
.streamlit/secrets.toml
*.duckdb
*.duckdb.wal
.venv*/
```

- [ ] **Step 6: Verify against a local file store (manual)**

Run from the repo root, in the project venv:

```bash
python -m pytest tests -v                       # still all green
python dashboard/migrate_files.py               # imports any parquet in dashboard/data, prints per-file rows
streamlit run dashboard/app.py --server.port 8765 --server.headless true
```

In the browser at http://localhost:8765 check: (1) the imported race appears in "Choose race" and its tabs render; (2) "Rename this race" persists after a full browser reload; (3) uploading a CSV and clicking "Import CSVs" adds a race; (4) stop and restart streamlit — the races are still there. Confirm `dashboard/data/results.duckdb` exists and `git status` does not list it.

- [ ] **Step 7: Commit**

```bash
git add dashboard/app.py dashboard/migrate_files.py dashboard/requirements.txt .streamlit/secrets.toml.example .gitignore
git commit -m "feat(dashboard): load one race at a time from the durable store; add migrate_files CLI"
```

---

### Task 7: CI workflow and README

**Files:**
- Create: `.github/workflows/tests.yml`
- Modify: `dashboard/README.md` (replace "Installation" and "Usage"; add "Deploy")
- Modify: `README.md` ("Development" section: mention CI)

- [ ] **Step 1: Create `.github/workflows/tests.yml`**

```yaml
name: tests

on:
  push:
  pull_request:

jobs:
  pytest:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.9", "3.11"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: pip
          cache-dependency-path: |
            requirements-dev.txt
            dashboard/requirements.txt
      - run: python -m pip install --upgrade pip
      - run: pip install -r requirements-dev.txt
      - run: python -m pytest tests -v
```

- [ ] **Step 2: Rewrite the "Installation" and "Usage" sections of `dashboard/README.md` and add "Deploy"**

Replace everything from `## Installation` up to (not including) `## Tech Stack` with:

```markdown
## Run locally

From the repository root:

```bash
pip install -r requirements-dev.txt
streamlit run dashboard/app.py
```

With no MotherDuck token configured, results are stored in
`dashboard/data/results.duckdb` (git-ignored). Add a race from the sidebar by
pasting an Athlinks, NYRR, or RunSignup results URL, or upload CSVs and click
**Import CSVs**. Scraped data persists across restarts.

Have old `scraped_*.parquet` files? Import them once:

```bash
python dashboard/migrate_files.py            # reads dashboard/data/*.parquet|csv
```

## Deploy (Streamlit Community Cloud + MotherDuck)

The hosted container has no durable disk, so results live in a free
MotherDuck database (10 GB on the Lite plan). The app pulls one race at a time
into local DuckDB, so MotherDuck compute stays near zero.

1. Create a MotherDuck account and an access token (Settings → Access Tokens).
2. In Streamlit Community Cloud, deploy this repo with main file
   `dashboard/app.py`.
3. In the app's **Settings → Secrets**, paste:

   ```toml
   [motherduck]
   token = "your-token"
   ```

4. To seed the cloud database with files from your laptop, run once:

   ```bash
   MOTHERDUCK_TOKEN=your-token python dashboard/migrate_files.py
   ```

Anyone with the app URL can scrape or rename races; there is no login.
```

- [ ] **Step 3: Add a CI line to the root `README.md` "Development" section**

Append to the end of that section:

```markdown
Every push and pull request runs the test suite on Python 3.9 and 3.11 via
GitHub Actions (`.github/workflows/tests.yml`).
```

- [ ] **Step 4: Run the suite one last time, commit, push, watch CI**

```bash
python -m pytest tests -v
git add .github/workflows/tests.yml dashboard/README.md README.md
git commit -m "ci: run pytest on push and PR; document local run and MotherDuck deploy"
git push -u origin feat/durable-storage-plan
gh run watch --exit-status   # wait for the workflow; must be green on both Python versions
```

If CI fails on 3.9 only, the most likely cause is a syntax feature newer than 3.9 — fix it, do not drop 3.9 from the matrix.

- [ ] **Step 5: Open the PR**

```bash
gh pr create --base main --title "Durable storage: MotherDuck-backed results store, CI" \
  --body-file - <<'EOF'
Scraped results now live in a durable DuckDB store (MotherDuck in production, a local file in development) instead of parquet files on the container disk, so they survive Streamlit Community Cloud redeploys. The dashboard loads one race group at a time into in-memory DuckDB; every query and section is unchanged.

- New `dashboard/storage.py`: schema, `save_event` (replace-per-event), `load_group`, `list_groups`, metadata table, `import_files`, `resolve_dsn`.
- `app.py` reads/writes through storage; CSV upload imports into the store.
- `migrate_files.py` one-shot import of legacy files.
- GitHub Actions runs pytest on 3.9 and 3.11.

Spec: docs/superpowers/specs/2026-09-07-durable-storage-design.md
Plan: docs/superpowers/plans/2026-09-07-durable-storage.md
EOF
```

Append the PR-description trailer lines your session instructions specify.

---

## Self-review against the spec

- Spec §storage.py: every listed function has a task (`resolve_dsn` T5, `open_store`/`ensure_schema`/`conform` T1, `save_event`/`load_group`/`list_groups` T2, `save_frame`/`import_files` T3, metadata T4). Moved helpers and one-way import direction: T1 and T4.
- Spec §dashboard_queries: deletions and `get_event_names` rewrite in T4.
- Spec §app.py: all eight steps of the run order appear in T6's file, including the secrets fallback, cursor-per-use, cache clear on write, the upload import button, and `st.error` + `st.stop()` on store failure.
- Spec §migrate_files, §Deployment files, §CI, README: T6 and T7.
- Spec §Testing: every bullet maps to a named test in T1-T5; smoke test updated in T4; existing group tests untouched.
- Type consistency: `save_event(con, df, ref) -> int`, `save_frame(con, df, filename=None) -> int`, `import_files(con, data_dir) -> List[Tuple[str,int]]`, `load_group(con, group_key) -> DataFrame`, `resolve_dsn(secrets, env, data_dir) -> str` are used with the same shapes in T6's `app.py` and `migrate_files.py`.
