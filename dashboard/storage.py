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
