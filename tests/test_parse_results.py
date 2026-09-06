from athlinks_scraper.core import parse_results

# 1669291200000 ms = 2022-11-24 12:00:00 UTC. Noon UTC is chosen so the
# local-time conversion in parse_results lands on 2022-11-24 in every timezone
# from UTC-12 to UTC+11.
METADATA = {"id": 994637, "name": "Test Trot", "start": {"epoch": 1669291200000}}


def make_block(chip_millis, meters=5000, status="CONF"):
    """Builds one 'course' block in the shape the Athlinks results API returns."""
    return {
        "race": {"name": "5K Run"},
        "intervals": [
            {
                "distance": {"meters": meters},
                "results": [
                    {
                        "displayName": "Alice Fast",
                        "gender": "F",
                        "age": 30,
                        "bib": "12",
                        "chipTimeInMillis": chip_millis,
                        "location": {"locality": "Branford", "region": "CT", "country": "USA"},
                        "rankings": {"overall": 1, "gender": 1, "primary": 1},
                        "status": status,
                    }
                ],
            }
        ],
    }


def test_parse_results_basic_row():
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)  # 18:00

    assert len(rows) == 1
    row = rows[0]
    assert row["Event ID"] == 994637
    assert row["Event Name"] == "Test Trot"
    assert row["Event Date"] == "2022-11-24"
    assert row["Race Type"] == "5K Run"
    assert row["Name"] == "Alice Fast"
    assert row["Gender"] == "F"
    assert row["Age"] == 30
    assert row["City"] == "Branford"
    assert row["State"] == "CT"
    assert row["Overall Rank"] == 1
    assert row["Status"] == "CONF"


def test_time_under_one_hour_is_mm_ss():
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)
    assert rows[0]["Time"] == "18:00"


def test_time_over_one_hour_is_hh_mm_ss():
    rows = parse_results([make_block(chip_millis=3_725_000)], METADATA)  # 1:02:05
    assert rows[0]["Time"] == "01:02:05"


def test_pace_is_minutes_per_mile():
    # 18:00 over 5000 m (3.10686 mi) = 5.79 min/mi -> "5:47" (seconds truncated)
    rows = parse_results([make_block(chip_millis=1080000)], METADATA)
    assert rows[0]["Pace"] == "5:47"


def test_missing_distance_gives_empty_pace():
    rows = parse_results([make_block(chip_millis=1080000, meters=None)], METADATA)
    assert rows[0]["Pace"] == ""


def test_no_metadata_gives_empty_event_fields():
    rows = parse_results([make_block(chip_millis=1080000)], metadata=None)
    assert rows[0]["Event ID"] == ""
    assert rows[0]["Event Name"] == ""
    assert rows[0]["Event Date"] == ""


def test_empty_blocks_gives_empty_list():
    assert parse_results([], METADATA) == []
