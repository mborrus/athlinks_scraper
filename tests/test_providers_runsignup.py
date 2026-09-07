import pytest

from athlinks_scraper import core
from athlinks_scraper.providers import runsignup as rs
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

RACE = {"race": {"race_id": 100692, "name": "#RUNMARANA Turkey Trot", "events": [
    {"event_id": 1192487, "name": "5k Run/Walk", "start_time": "11/14/2026 08:20", "distance": "3.1 Miles"},
    {"event_id": 1024596, "name": "5k Run/Walk", "start_time": "11/15/2025 08:20", "distance": "3.1 Miles"},
    {"event_id": 1024595, "name": "10k Run/Walk", "start_time": "11/15/2025 08:15", "distance": "6.2 Miles"},
]}}


def result(i):
    return {"result_id": i, "place": i, "bib": 500 + i, "first_name": f"F{i}", "last_name": f"L{i}",
            "gender": "M", "city": "Marana", "state": "AZ", "country_code": "US",
            "clock_time": "21:53.15", "chip_time": "21:52.05", "pace": "7:03", "age": 44}


def results_payload(n):
    return {"individual_results_sets": [{"individual_result_set_name": "Overall", "public_results": "T",
                                         "results": [result(i + 1) for i in range(n)]}]}


def test_extract_race_id():
    assert rs.extract_race_id("https://runsignup.com/Race/Results/100692") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/Results/100692/#resultSetId-604416") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/Register/?raceId=100692") == "100692"
    assert rs.extract_race_id("https://runsignup.com/Race/AZ/Marana/RunMarana") is None


def test_matches():
    p = rs.RunSignupProvider()
    assert p.matches("https://runsignup.com/Race/Results/100692")
    assert not p.matches("https://www.athlinks.com/event/1")


def test_list_events_one_ref_per_event_newest_first():
    session = FakeSession([FakeResponse(RACE)])

    refs = rs.RunSignupProvider().list_events("https://runsignup.com/Race/Results/100692", session=session)

    assert session.calls[0][0] == rs.RUNSIGNUP_API + "/race/100692"
    assert session.calls[0][1] == {"format": "json"}
    # event_id carries both ids because the results endpoint needs the race id too.
    assert [r.event_id for r in refs] == ["100692:1192487", "100692:1024596", "100692:1024595"]
    assert refs[1] == EventRef(source="runsignup", event_id="100692:1024596",
                               name="#RUNMARANA Turkey Trot - 5k Run/Walk",
                               date_str="2025-11-15", race_group="runmarana-turkey-trot", race_type="5k Run/Walk")


def test_list_events_bad_url_raises():
    with pytest.raises(ValueError):
        rs.RunSignupProvider().list_events("https://runsignup.com/Race/AZ/Marana/RunMarana")


def test_fetch_event_pages_and_maps_fields(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(rs, "RUNSIGNUP_PAGE_SIZE", 2)
    session = FakeSession([FakeResponse(results_payload(2)), FakeResponse(results_payload(1))])
    ref = EventRef(source="runsignup", event_id="100692:1024596", name="#RUNMARANA Turkey Trot - 5k Run/Walk",
                   date_str="2025-11-15", race_group="runmarana-turkey-trot", race_type="5k Run/Walk")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert len(df) == 3
    assert list(df.columns) == CANONICAL_COLUMNS
    urls = [c[0] for c in session.calls]
    assert urls == [rs.RUNSIGNUP_API + "/race/100692/results/get-results"] * 2
    assert [c[1]["page"] for c in session.calls] == [1, 2]
    assert all(c[1]["event_id"] == "1024596" for c in session.calls)
    first = df.iloc[0]
    assert first["Name"] == "F1 L1"
    assert first["Time"] == "21:52"          # chip time, fraction stripped
    assert first["Pace"] == "7:03"
    assert first["Overall Rank"] == 1
    assert first["Bib"] == "501"
    assert first["State"] == "AZ"
    assert first["Country"] == "US"
    assert first["Race Type"] == "5k Run/Walk"
    assert first["Status"] == "CONF"


def test_fetch_event_no_results_yet(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse({"individual_results_sets": []})])
    ref = EventRef(source="runsignup", event_id="100692:1192487", name="n", date_str="2026-11-14",
                   race_group="g", race_type="5k")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert len(df) == 0


def test_fetch_event_falls_back_to_clock_time(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    row = result(1)
    row["chip_time"] = None
    payload = {"individual_results_sets": [{"public_results": "T", "results": [row]}]}
    session = FakeSession([FakeResponse(payload)])
    ref = EventRef(source="runsignup", event_id="100692:1", name="n", date_str="2025-11-15", race_group="g")

    df = rs.RunSignupProvider().fetch_event(ref, session=session)

    assert df.loc[0, "Time"] == "21:53"
