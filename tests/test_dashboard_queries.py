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
