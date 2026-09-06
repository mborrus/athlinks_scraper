import pytest
import requests

from athlinks_scraper import core
from tests.fakes import FakeResponse, FakeSession


def test_build_session_mounts_retry_policy():
    session = core.build_session()
    adapter = session.get_adapter("https://reignite-api.athlinks.com/")
    retry = adapter.max_retries

    assert retry.total == 3
    assert 503 in retry.status_forcelist
    assert 429 in retry.status_forcelist
    assert retry.backoff_factor > 0


def test_get_session_is_cached():
    assert core.get_session() is core.get_session()


def test_fetch_json_passes_timeout_and_returns_payload():
    session = FakeSession([FakeResponse({"ok": True})])

    result = core.fetch_json("https://x/y", params={"a": 1}, session=session)

    assert result == {"ok": True}
    assert session.calls == [("https://x/y", {"a": 1}, core.DEFAULT_TIMEOUT)]


def test_fetch_json_raises_on_http_error():
    session = FakeSession([FakeResponse({}, status_code=500)])

    with pytest.raises(requests.HTTPError):
        core.fetch_json("https://x/y", session=session)


def test_fetch_metadata_uses_session_and_returns_dict():
    session = FakeSession([FakeResponse({"id": 994637, "name": "Test Trot"})])

    meta = core.fetch_metadata("994637", session=session)

    assert meta == {"id": 994637, "name": "Test Trot"}
    url, _params, timeout = session.calls[0]
    assert url == "https://reignite-api.athlinks.com/event/994637/metadata"
    assert timeout == core.DEFAULT_TIMEOUT


def test_fetch_metadata_returns_empty_dict_on_failure():
    # Metadata is optional enrichment; a failure must not abort the scrape.
    session = FakeSession([FakeResponse({}, status_code=500)])

    assert core.fetch_metadata("994637", session=session) == {}


def test_fetch_master_events_sorts_newest_first():
    payload = {
        "events": [
            {"id": 1, "name": "2021", "start": {"epoch": 1637755200000}},  # 2021-11-24
            {"id": 2, "name": "2022", "start": {"epoch": 1669291200000}},  # 2022-11-24
        ]
    }
    session = FakeSession([FakeResponse(payload)])

    events = core.fetch_master_events("15776", session=session)

    assert [e["id"] for e in events] == [2, 1]
    assert events[0]["date_str"] == "2022-11-24"
    assert session.calls[0][0] == "https://reignite-api.athlinks.com/master/15776/metadata"


def test_fetch_master_events_returns_empty_list_on_failure():
    session = FakeSession([FakeResponse({}, status_code=503)])

    assert core.fetch_master_events("15776", session=session) == []
