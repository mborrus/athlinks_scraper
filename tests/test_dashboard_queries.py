import pytest

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
