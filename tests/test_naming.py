import pandas as pd

from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef, to_canonical
from athlinks_scraper.providers.naming import (
    race_group_key,
    race_group_label,
    strip_fractional_seconds,
)


def test_race_group_key_strips_year_and_nyrr():
    assert race_group_key("2024 NYRR Frosty 5K") == "frosty-5k"
    assert race_group_key("2023 NYRR Frosty 5K") == "frosty-5k"


def test_race_group_key_unifies_nyrr_spelling_variants():
    a = race_group_key("2024 Rising New York Road Runners at the NYRR Frosty 5K - Stage 3")
    b = race_group_key("2025 Rising NYRR at the NYRR Frosty 5K - Stage 3")
    assert a == b == "rising-at-frosty-5k-stage-3"


def test_race_group_key_handles_punctuation():
    assert race_group_key("#RUNMARANA Turkey Trot and Kid’s Free FUN Run") == (
        "runmarana-turkey-trot-and-kid-s-free-fun-run"
    )
    assert race_group_key("Branford Turkey Trot 2023") == "branford-turkey-trot"


def test_race_group_label_keeps_case_drops_year():
    assert race_group_label("2024 NYRR Frosty 5K") == "Frosty 5K"
    assert race_group_label("Branford Turkey Trot 2023") == "Branford Turkey Trot"
    assert race_group_label("TCS New York City Marathon 2025") == "TCS New York City Marathon"


def test_strip_fractional_seconds():
    assert strip_fractional_seconds("21:52.05") == "21:52"
    assert strip_fractional_seconds("1:02:03.9") == "1:02:03"
    assert strip_fractional_seconds("25:00") == "25:00"
    assert strip_fractional_seconds("") == ""
    assert strip_fractional_seconds(None) is None


def test_canonical_columns_order():
    assert CANONICAL_COLUMNS[:2] == ["Source", "Race Group"]
    assert CANONICAL_COLUMNS[-1] == "Status"
    assert "Master ID" not in CANONICAL_COLUMNS


def test_to_canonical_fills_missing_columns_and_orders():
    df = to_canonical([{"Name": "A", "Time": "20:00", "Source": "nyrr", "Race Group": "x"}])

    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.loc[0, "Name"] == "A"
    assert pd.isna(df.loc[0, "Age"])


def test_to_canonical_empty():
    df = to_canonical([])
    assert list(df.columns) == CANONICAL_COLUMNS
    assert len(df) == 0


def test_eventref_defaults():
    ref = EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                   date_str="2024-12-14", race_group="frosty-5k")
    assert ref.race_type == ""
