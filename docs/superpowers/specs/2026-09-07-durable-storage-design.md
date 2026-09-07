# Durable Storage for Scraped Results — Design

**Date:** 2026-09-07
**Status:** approved for planning
**Builds on:** `docs/superpowers/plans/2026-09-06-multi-source-and-redesign.md` (merged in PR #1)

## Problem

The dashboard persists scraped results as loose `.parquet` files in
`dashboard/data/` and reads all of them into an in-memory DuckDB on every
script run. Two things break when the app is hosted on Streamlit Community
Cloud:

1. The container filesystem is ephemeral. Every redeploy, reboot, or wake from
   sleep discards every scraped file.
2. The host has roughly 1 GB of RAM. Loading every race ever scraped into
   memory at once will not scale past a handful of large races.

## Decisions (verified 2026-09-07)

| Decision | Choice | Why |
|---|---|---|
| Hosting | Streamlit Community Cloud | Free; deploys from the GitHub repo |
| Durable store | MotherDuck (Lite plan) | 10 GB free storage; DuckDB is already a dependency; no new client library |
| Query engine | Local in-memory DuckDB, one race group at a time | Keeps every existing query untouched; keeps MotherDuck compute near zero (10 Pulse hours/month on the free plan); bounds memory on the host |
| Write access | Open — any visitor may scrape or rename | User's explicit choice; risk recorded below |
| Local development | Same code path against a local DuckDB file | No account needed to run or test |
| CI | GitHub Actions runs `pytest` on every push and pull request | Requested by user |

Rejected: Neon Postgres (free tier caps at 0.5 GB per project and fails
writes above it; user expects more data than that). Supabase Postgres (same
size cap, and free projects pause after seven idle days). MotherDuck as the
live query engine (fifteen remote queries per rerun would burn the free
compute allowance and add latency to every widget change).

MotherDuck facts the implementation relies on: DuckDB client versions 1.4.1
through 1.5.5 are supported; Python 3.9 is supported; the connection string is
`md:?motherduck_token=<token>`; a database is created with
`CREATE DATABASE IF NOT EXISTS <name>` and selected with `USE <name>`.

## Architecture

```
                         ┌──────────────────────┐
 sidebar "Fetch all      │ dashboard/storage.py │   MotherDuck  (prod)
 years" / CSV upload ───►│  save_event()        │──► local .duckdb (dev)
                         │  load_group()        │    :memory:   (tests)
 race picker ───────────►│  list_groups()       │
                         │  metadata get/set    │
                         └──────────┬───────────┘
                                    │ one DataFrame (one race group)
                                    ▼
                      dq.init_db_from_dataframe()  ─► in-memory DuckDB
                      dq.create_enriched_view()       (unchanged queries,
                      sections/*.render(con)           unchanged sections)
```

`dashboard/storage.py` is the only module that talks to the durable store. It
imports `duckdb` and `pandas` only — never `streamlit` — so tests exercise it
with `open_store(":memory:")`.

## Components

### storage.py

Constants:

- `DB_NAME = "turkey_trot"` — the MotherDuck database name.
- `RESULT_TYPES`: ordered mapping of the 19 canonical columns to DuckDB types.
  `Age`, `Overall Rank`, `Gender Rank`, `Division Rank` are `INTEGER`; all
  others are `VARCHAR`. Plus `scraped_at TIMESTAMP`.

Functions (all take an open connection `con` unless noted):

- `resolve_dsn(secrets=None, env=None, data_dir=None) -> str`
  Pure function. Returns `md:?motherduck_token=<t>` if a token is found in
  `secrets["motherduck"]["token"]` or `env["MOTHERDUCK_TOKEN"]` (secrets win);
  otherwise the path `<data_dir>/results.duckdb`. `data_dir` defaults to
  `dashboard/data`. Takes plain dicts so it is testable without Streamlit.
- `open_store(dsn) -> duckdb.DuckDBPyConnection`
  Connects. If `dsn` starts with `md:`, runs `CREATE DATABASE IF NOT EXISTS
  turkey_trot` then `USE turkey_trot`. If `dsn` is a file path, creates its
  parent directory. Always calls `ensure_schema`.
- `ensure_schema(con)`
  `CREATE TABLE IF NOT EXISTS results (...)` from `RESULT_TYPES`, and
  `CREATE TABLE IF NOT EXISTS event_metadata (race_group VARCHAR PRIMARY KEY,
  display_name VARCHAR)`. Idempotent.
- `conform(df) -> DataFrame`
  Reindexes to the 19 canonical columns (adding missing ones as null), casts
  the four integer columns with `pd.to_numeric(errors="coerce")` to nullable
  `Int64`, casts everything else to string with nulls preserved, strips
  column-name whitespace first. Calls `backfill_legacy_columns(df, filename)`
  first so old frames gain `Source` and `Race Group`.
- `extract_master_id_from_filename` and `backfill_legacy_columns` **move**
  from `dashboard_queries.py` into `storage.py` unchanged. `dashboard_queries`
  re-exports both names (`from storage import backfill_legacy_columns,
  extract_master_id_from_filename`) so existing tests keep passing. The
  import direction is therefore one-way: `dashboard_queries` imports
  `storage`; `storage` imports nothing from the dashboard.
- `save_event(con, df, ref)`
  In one transaction: `DELETE FROM results WHERE "Source" = ? AND "Event ID"
  = ?`, then `INSERT INTO results SELECT <19 cols>, now() FROM _incoming`,
  where `_incoming` is the conformed frame registered as a temporary view.
  Empty frames are a no-op (no delete either). Returns the number of rows
  inserted.
- `save_frame(con, df, filename=None)`
  For CSV uploads and legacy files, which have no `EventRef`. Conforms the
  frame, then groups by (`Source`, `Event ID`) and calls the same
  delete-then-insert per group. Rows whose `Event ID` is null get `Event ID`
  set to the `Race Group` value so they still replace cleanly. Returns total
  rows inserted.
- `import_files(con, data_dir) -> list[tuple[str, int]]`
  For every `*.parquet` / `*.csv` in `data_dir`, read it and call
  `save_frame`. Returns `(filename, rows)` pairs. Never raises on one bad
  file; records `(filename, -1)` and continues.
- `list_groups(con) -> DataFrame`
  Columns `group_key, event_name, source_name, n_years` — the same SQL that
  `get_event_names` runs today, executed against the store.
- `load_group(con, group_key) -> DataFrame`
  `SELECT <19 canonical cols> FROM results WHERE "Race Group" = ?`. Parameter
  bound; never string-formatted.
- `load_event_metadata(con) -> dict[str, str]`. Returns `{}` when the
  `event_metadata` table does not exist (an in-memory connection built by
  `init_db_from_dataframe` has only `results`), so `get_event_names` keeps
  working on test connections.
- `save_custom_event_name(con, group_key, display_name)` — `INSERT OR
  REPLACE`.

### dashboard_queries.py changes

- `get_event_names(con)` keeps its signature and return shape but calls
  `storage.list_groups(con)` and `storage.load_event_metadata(con)` instead
  of the JSON file. It must work when `con` is the store connection.
- Delete `init_db`, `get_metadata_path`, the file-based
  `load_event_metadata`, and the file-based `save_custom_event_name`.
  `init_db_from_dataframe` and every query function stay as they are.

### app.py changes

Order of operations on each script run:

1. `dsn = storage.resolve_dsn(secrets=st.secrets, env=os.environ)`, wrapped
   so that a missing `secrets.toml` (Streamlit raises when no secrets file
   exists) falls through to env/local.
2. `store = get_store(dsn)` — `@st.cache_resource` around `open_store`.
   Every use of `store` goes through `store.cursor()` so concurrent Streamlit
   sessions do not share one cursor.
3. Sidebar scrape: per event, `storage.save_event(store.cursor(), df, ref)`.
   After the loop, `load_group_cached.clear()` and `st.rerun()`.
4. Sidebar CSV upload: on a new "Import" button, `storage.save_frame(...)`
   per uploaded file, then clear cache and rerun. The uploaded frame is no
   longer merged transiently.
5. `groups = dq.get_event_names(store.cursor())`. If empty, show the
   no-races masthead and stop.
6. Race picker as today. Rename writes through
   `storage.save_custom_event_name`, clears cache, reruns.
7. `df = load_group_cached(dsn, group_key)` — `@st.cache_data(ttl=3600)`;
   opens its own cursor from the cached store; the `dsn` argument only
   exists to key the cache correctly.
8. `con = dq.init_db_from_dataframe(df)`; `dq.create_enriched_view(con,
   group_key)`; render sections exactly as today.

`DATA_DIR`, `save_event_frame`, and `has_local_data` are deleted.

### migrate_files.py

`python dashboard/migrate_files.py [--data-dir PATH]`. Resolves the DSN the
same way the app does, opens the store, calls `import_files`, prints one line
per file with the row count, exits non-zero if any file failed. Run once by
hand from a laptop to move existing parquet files into MotherDuck.

### Deployment files

- `dashboard/requirements.txt`: `duckdb>=1.4.1,<1.6` (the rest unchanged).
- `.streamlit/secrets.toml.example`:
  ```toml
  [motherduck]
  token = "paste-your-token-here"
  ```
- `.gitignore` additions: `.streamlit/secrets.toml`, `*.duckdb`,
  `*.duckdb.wal`.
- README "Deploy" section: create a MotherDuck account and token, add the
  secret in the Streamlit Cloud app settings, set the app's main file to
  `dashboard/app.py`, run the migration once.

### CI

`.github/workflows/tests.yml`: triggers on `push` to any branch and on
`pull_request`. Matrix over Python 3.9 and 3.11 on `ubuntu-latest`. Steps:
checkout, setup-python with pip cache, `pip install -r requirements-dev.txt`,
`python -m pytest tests -v`. No secrets; tests never need the network.

## Data flow for one scrape

1. Visitor pastes a URL and clicks Fetch. `detect_provider` lists events.
2. For each event, `fetch_event` returns a canonical frame; `save_event`
   deletes any prior rows for that (Source, Event ID) and inserts the new
   ones with `scraped_at = now()`.
3. Cache cleared; rerun. `list_groups` now includes the race; picking it
   pulls its rows once into local DuckDB; every section renders from that.
4. The rows are in MotherDuck. They survive redeploys, sleeps, and reboots.

## Error handling

- Provider errors during scrape: unchanged (collected per event, shown as a
  warning, other events continue).
- Store unavailable at startup (bad token, network): `open_store` raises;
  app.py catches it and shows `st.error` with the message and a hint to check
  the `motherduck` secret, then `st.stop()`.
- A `save_event` transaction that fails rolls back; nothing half-written.
- `import_files` isolates per-file failures.

## Testing

`tests/test_storage.py`, all against `open_store(":memory:")`:

- `ensure_schema` is idempotent (call twice, tables exist once).
- `save_event` inserts N rows; calling again with the same ref and a
  different frame leaves exactly the new rows (replace, not append).
- `save_event` with an empty frame does not delete existing rows.
- `load_group` returns only the requested group's rows and exactly the 19
  canonical columns.
- `conform` on a frame missing columns and with `"Age"` of `"abc"` yields
  nulls, not exceptions; a legacy frame with `Master ID` gets `Race Group`
  and `Source` backfilled.
- `save_frame` with two event IDs in one frame writes both groups and
  replaces each independently.
- `import_files` on a `tmp_path` holding one parquet, one CSV, and one
  unreadable file returns three tuples, one with `-1`.
- `save_custom_event_name` upserts; `load_event_metadata` reads it back.
- `resolve_dsn` prefers secrets over env over local file.
- `get_event_names` against a store connection returns the same shape as
  before and honours a metadata override.

Existing tests in `tests/test_dashboard_groups.py` keep passing unchanged:
the backfill helpers are re-exported and `get_event_names` tolerates a
connection with no metadata table. `tests/test_smoke.py` stops
asserting `init_db` exists and asserts `storage.open_store` instead.

The full suite must pass on Python 3.9 and 3.11 in CI.

## Accepted risks

- **Open writes.** Anyone with the URL can scrape into the shared database or
  rename a race. The user chose this. If it becomes a problem, the cheapest
  fix is a passphrase in `st.secrets` gating the sidebar write controls.
- **One huge race group.** Memory is bounded to one race at a time, but a
  single group with millions of rows could still exceed the host's ~1 GB.
  The fix, if ever needed, is to load by year range — the store design does
  not change.
- **Cache staleness.** Another visitor's scrape appears for you after at most
  one hour, or immediately after your own write.

## Out of scope

Authentication, scrape rate limiting, moving the scraper CLI to write to the
store, historical snapshots, any change to queries or sections.
