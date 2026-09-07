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
