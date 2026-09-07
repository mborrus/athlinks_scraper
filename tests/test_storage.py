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
