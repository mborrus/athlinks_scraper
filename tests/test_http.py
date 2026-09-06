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


def page(n_results):
    """One results page with n_results runners in a single 5K interval."""
    return [
        {
            "race": {"name": "5K"},
            "intervals": [
                {
                    "distance": {"meters": 5000},
                    "results": [{"displayName": f"R{i}", "chipTimeInMillis": 1_200_000} for i in range(n_results)],
                }
            ],
        }
    ]


def test_fetch_results_pages_until_empty(monkeypatch):
    sleeps = []
    monkeypatch.setattr(core.time, "sleep", lambda s: sleeps.append(s))
    session = FakeSession([FakeResponse(page(100)), FakeResponse(page(7)), FakeResponse(page(0))])

    blocks = core.fetch_results("994637", session=session)

    # Two non-empty pages were kept; the empty page terminated the loop.
    assert len(blocks) == 3  # 3 raw blocks were returned (incl. the empty one); parse_results ignores empties
    assert [c[1]["from"] for c in session.calls] == [0, 100, 200]
    assert all(c[1]["limit"] == 100 for c in session.calls)
    assert all(c[2] == core.DEFAULT_TIMEOUT for c in session.calls)
    # We paused between pages, but not after the final (empty) page.
    assert sleeps == [core.REQUEST_DELAY_SECONDS, core.REQUEST_DELAY_SECONDS]


def test_fetch_results_raises_instead_of_returning_partial(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse(page(100)), FakeResponse({}, status_code=502)])

    with pytest.raises(requests.HTTPError):
        core.fetch_results("994637", session=session)


def test_fetch_results_stops_at_max_pages(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(core, "MAX_PAGES", 3)
    # Every page is non-empty; without the cap this would loop forever.
    session = FakeSession([FakeResponse(page(100)) for _ in range(10)])

    blocks = core.fetch_results("994637", session=session)

    assert len(session.calls) == 3
    assert len(blocks) == 3


def test_get_results_returns_dataframe(monkeypatch):
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    meta = {"id": 994637, "name": "Test Trot", "start": {"epoch": 1669291200000}}
    session = FakeSession([FakeResponse(meta), FakeResponse(page(2)), FakeResponse(page(0))])

    df = core.get_results("994637", session=session)

    assert list(df["Name"]) == ["R0", "R1"]
    assert df["Event Name"].iloc[0] == "Test Trot"
    assert df["Time"].iloc[0] == "20:00"
