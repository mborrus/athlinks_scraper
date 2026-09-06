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
