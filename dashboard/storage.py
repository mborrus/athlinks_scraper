"""
The only module that talks to the durable results store.

The store is a DuckDB database reached through a DSN:
  - "md:?motherduck_token=..."   MotherDuck (production on Streamlit Cloud)
  - "/some/path/results.duckdb"   a local file (laptop development)
  - ":memory:"                    tests

Everything here is plain duckdb + pandas. No streamlit imports, ever.
"""
import json
import os
import re
from typing import Dict, List, Optional, Tuple

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


# --- imports of files (uploads and legacy data dirs) ------------------------------------

def save_frame(con, df: pd.DataFrame, filename: Optional[str] = None) -> int:
    """
    Stores a frame that did not come from a provider (a CSV upload or a
    legacy parquet file). Rows are grouped by (Source, Event ID) and each
    group replaces its predecessor. A null Event ID falls back to the Race
    Group so old single-year files still replace cleanly.

    Rows with no resolvable Source or Event ID are dropped and not counted:
    without a key they could never be replaced on a later import, so storing
    them would accumulate a fresh orphan row every time the file is read.
    """
    if df is None or df.empty:
        return 0
    conformed = conform(df, filename)
    missing = conformed["Event ID"].isna()
    conformed.loc[missing, "Event ID"] = conformed.loc[missing, "Race Group"]
    conformed = conformed[conformed["Source"].notna() & conformed["Event ID"].notna()]
    total = 0
    for (source, event_id), part in conformed.groupby(["Source", "Event ID"], sort=False):
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


def import_metadata_json(con, path: str) -> int:
    """
    One-shot import of the legacy event_metadata.json ({race_group: name}).
    Upserts every entry; returns how many. 0 if the file is missing or unreadable.
    """
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"import_metadata_json: {path}: {e}")
        return 0
    count = 0
    for key, name in (data or {}).items():
        if name and str(name).strip():
            save_custom_event_name(con, str(key), str(name))
            count += 1
    return count


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
