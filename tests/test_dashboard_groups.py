import pandas as pd

import dashboard_queries as dq


def test_to_seconds_handles_fractions_and_zero_hour(db):
    assert db.execute("SELECT to_seconds('21:52.05')").fetchone()[0] == 1312
    assert db.execute("SELECT to_seconds('0:16:03')").fetchone()[0] == 963
    assert db.execute("SELECT to_seconds('05:10')").fetchone()[0] == 310


def test_get_event_names_groups_by_race_group(sample_results_df):
    con = dq.init_db_from_dataframe(sample_results_df)

    groups = dq.get_event_names(con)

    by_key = {g["group_key"]: g for g in groups}
    assert set(by_key) == {"111", "222"}
    assert by_key["111"]["display_name"] == "Test Trot"
    assert by_key["111"]["source_name"] == "athlinks"
    assert by_key["111"]["n_years"] == 2
    assert by_key["222"]["n_years"] == 1


def test_get_event_names_strips_year_from_display(sample_results_df):
    df = sample_results_df.copy()
    df.loc[df["Race Group"] == "222", "Event Name"] = "2023 NYRR Other Trot"
    con = dq.init_db_from_dataframe(df)

    names = {g["group_key"]: g["display_name"] for g in dq.get_event_names(con)}

    assert names["222"] == "Other Trot"


def test_backfill_legacy_columns_from_old_filename():
    old = pd.DataFrame([{"Event Name": "Trot", "Name": "A", "Time": "20:00", "Pace": "6:26",
                         "Event Date": "2022-11-24", "Race Type": "5K", "Master ID": "15776"}])

    df = dq.backfill_legacy_columns(old, "scraped_15776_2022.parquet")

    assert df.loc[0, "Race Group"] == "15776"
    assert df.loc[0, "Source"] == "athlinks"


def test_backfill_legacy_columns_leaves_new_files_alone():
    new = pd.DataFrame([{"Source": "nyrr", "Race Group": "frosty-5k", "Name": "A"}])

    df = dq.backfill_legacy_columns(new, "scraped_nyrr_frosty-5k_2024_24FROSTY.parquet")

    assert df.loc[0, "Race Group"] == "frosty-5k"
    assert df.loc[0, "Source"] == "nyrr"


def test_retention_data_function_removed():
    assert not hasattr(dq, "get_retention_data")
