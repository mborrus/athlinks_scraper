import pytest

from athlinks_scraper import core
from athlinks_scraper.providers import nyrr
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

DETAILS_24 = {"eventDetails": {"eventName": "2024 NYRR Frosty 5K", "eventCode": "24FROSTY",
                               "startDateTime": "2024-12-14T08:00:00", "distanceName": "5 kilometers",
                               "distanceUnitCode": "5K"}, "success": True}

SEARCH = {"totalItems": 4, "items": [
    {"eventName": "2024 Rising NYRR at the NYRR Frosty 5K - Stage 3", "eventCode": "24YFROSTY",
     "startDateTime": "2024-12-14T08:45:00", "distanceName": "1 mile", "distanceUnitCode": "1M"},
    {"eventName": "2024 NYRR Frosty 5K", "eventCode": "24FROSTY",
     "startDateTime": "2024-12-14T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
    {"eventName": "2023 NYRR Frosty 5K", "eventCode": "23FROSTY",
     "startDateTime": "2023-12-09T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
    {"eventName": "2021 NYRR Frosty 5K", "eventCode": "21Seasonal",
     "startDateTime": "2021-12-11T08:00:00", "distanceName": "5 kilometers", "distanceUnitCode": "5K"},
]}


def finisher(i, place):
    return {"runnerId": i, "firstName": f"First{i}", "lastName": f"Last{i}", "bib": str(i), "age": 30,
            "gender": "M", "city": "New York", "countryCode": "USA", "stateProvince": "NY",
            "overallPlace": place, "overallTime": "0:16:03", "gunTime": "0:16:05", "pace": "05:10",
            "genderPlace": place}


def test_matches_and_event_code():
    p = nyrr.NyrrProvider()
    assert p.matches("https://results.nyrr.org/event/24FROSTY/finishers")
    assert not p.matches("https://www.athlinks.com/event/1")
    assert nyrr.extract_event_code("https://results.nyrr.org/event/24FROSTY/finishers") == "24FROSTY"
    assert nyrr.extract_event_code("https://results.nyrr.org/races") is None


def test_list_events_finds_sibling_years_by_name():
    session = FakeSession([FakeResponse(DETAILS_24), FakeResponse(SEARCH)])

    refs = nyrr.NyrrProvider().list_events("https://results.nyrr.org/event/24FROSTY/finishers", session=session)

    # The 1-mile "Rising" event has a different group key and is excluded.
    assert [r.event_id for r in refs] == ["24FROSTY", "23FROSTY", "21Seasonal"]
    assert refs[0] == EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                               date_str="2024-12-14", race_group="frosty-5k", race_type="5K")
    details_call, search_call = session.calls
    assert details_call[0] == nyrr.NYRR_API + "/events/details"
    assert details_call[1] == {"eventCode": "24FROSTY"}
    assert search_call[0] == nyrr.NYRR_API + "/events/search"
    assert search_call[1]["searchString"] == "Frosty 5K"
    assert search_call[1]["year"] is None
    assert search_call[1]["pageSize"] == nyrr.NYRR_PAGE_SIZE


def test_list_events_bad_url_raises():
    with pytest.raises(ValueError):
        nyrr.NyrrProvider().list_events("https://results.nyrr.org/races")


def test_fetch_event_pages_and_maps_fields(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    page1 = {"totalItems": 101, "items": [finisher(i, i + 1) for i in range(100)]}
    page2 = {"totalItems": 101, "items": [finisher(100, 101)]}
    session = FakeSession([FakeResponse(page1), FakeResponse(page2)])
    ref = EventRef(source="nyrr", event_id="24FROSTY", name="2024 NYRR Frosty 5K",
                   date_str="2024-12-14", race_group="frosty-5k", race_type="5K")

    df = nyrr.NyrrProvider().fetch_event(ref, session=session)

    assert list(df.columns) == CANONICAL_COLUMNS
    assert len(df) == 101
    assert [c[1]["pageIndex"] for c in session.calls] == [1, 2]
    assert all(c[1]["eventCode"] == "24FROSTY" for c in session.calls)
    first = df.iloc[0]
    assert first["Source"] == "nyrr"
    assert first["Race Group"] == "frosty-5k"
    assert first["Event ID"] == "24FROSTY"
    assert first["Event Name"] == "2024 NYRR Frosty 5K"
    assert first["Event Date"] == "2024-12-14"
    assert first["Race Type"] == "5K"
    assert first["Name"] == "First0 Last0"
    assert first["Gender"] == "M"
    assert first["Age"] == 30
    assert first["Bib"] == "0"
    assert first["City"] == "New York"
    assert first["State"] == "NY"
    assert first["Country"] == "USA"
    assert first["Time"] == "0:16:03"
    assert first["Pace"] == "05:10"
    assert first["Overall Rank"] == 1
    assert first["Gender Rank"] == 1
    assert first["Status"] == "CONF"


def test_fetch_event_empty(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse({"totalItems": 0, "items": []})])
    ref = EventRef(source="nyrr", event_id="X", name="n", date_str="2020-01-01", race_group="n")

    df = nyrr.NyrrProvider().fetch_event(ref, session=session)

    assert len(df) == 0
    assert list(df.columns) == CANONICAL_COLUMNS


def test_race_type_mapping():
    assert nyrr.race_type_from_unit("MAR", "Marathon") == "Marathon"
    assert nyrr.race_type_from_unit("5K", "5 kilometers") == "5K"
    assert nyrr.race_type_from_unit("HALF", "Half-Marathon") == "Half Marathon"
    assert nyrr.race_type_from_unit("ZZZ", "Something odd") == "Something odd"
