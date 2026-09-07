import pytest
import requests

from athlinks_scraper import core
from athlinks_scraper.providers.athlinks import AthlinksProvider
from athlinks_scraper.providers.base import CANONICAL_COLUMNS, EventRef
from tests.fakes import FakeResponse, FakeSession

MASTER_PAYLOAD = {
    "events": [
        {"id": 111, "name": "Trot 2021", "start": {"epoch": 1637755200000}},  # 2021-11-24
        {"id": 222, "name": "Trot 2022", "start": {"epoch": 1669291200000}},  # 2022-11-24
    ]
}


def test_post_json_sends_body_and_returns_payload():
    session = FakeSession([FakeResponse({"ok": 1})])

    out = core.post_json("https://x/y", {"a": 1}, session=session)

    assert out == {"ok": 1}
    url, body, timeout = session.calls[0]
    assert url == "https://x/y"
    assert body == {"a": 1}
    assert timeout == core.DEFAULT_TIMEOUT


def test_post_json_raises_on_http_error():
    session = FakeSession([FakeResponse({}, status_code=500)])
    with pytest.raises(requests.HTTPError):
        core.post_json("https://x/y", {}, session=session)


def test_matches():
    p = AthlinksProvider()
    assert p.matches("https://www.athlinks.com/event/15776")
    assert not p.matches("https://results.nyrr.org/event/24FROSTY/finishers")


def test_list_events_from_master_url():
    session = FakeSession([FakeResponse(MASTER_PAYLOAD)])

    refs = AthlinksProvider().list_events("https://www.athlinks.com/event/15776", session=session)

    assert [r.event_id for r in refs] == ["222", "111"]  # newest first, ids are str
    assert refs[0] == EventRef(source="athlinks", event_id="222", name="Trot 2022",
                               date_str="2022-11-24", race_group="15776")


def test_list_events_from_specific_event_url_uses_master_from_metadata():
    meta = {"id": 222, "name": "Trot 2022", "masterId": 15776, "start": {"epoch": 1669291200000}}
    session = FakeSession([FakeResponse(meta), FakeResponse(MASTER_PAYLOAD)])

    refs = AthlinksProvider().list_events(
        "https://www.athlinks.com/event/15776/results/Event/222/Course/1/Results", session=session
    )

    assert [r.event_id for r in refs] == ["222", "111"]
    assert session.calls[0][0].endswith("/event/222/metadata")


def test_fetch_event_returns_canonical_frame(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    meta = {"id": 222, "name": "Trot 2022", "start": {"epoch": 1669291200000}}
    page = [{"race": {"name": "5K"}, "intervals": [{"distance": {"meters": 5000},
             "results": [{"displayName": "Alice Fast", "gender": "F", "age": 30, "bib": "1",
                          "chipTimeInMillis": 1080000, "location": {}, "rankings": {"overall": 1}}]}]}]
    session = FakeSession([FakeResponse(meta), FakeResponse(page), FakeResponse([])])
    ref = EventRef(source="athlinks", event_id="222", name="Trot 2022", date_str="2022-11-24", race_group="15776")

    df = AthlinksProvider().fetch_event(ref, session=session)

    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.loc[0, "Source"] == "athlinks"
    assert df.loc[0, "Race Group"] == "15776"
    assert df.loc[0, "Name"] == "Alice Fast"
    assert df.loc[0, "Time"] == "18:00"
