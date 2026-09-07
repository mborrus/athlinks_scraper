from athlinks_scraper.core import extract_event_id, extract_master_id

FULL_RESULTS_URL = (
    "https://www.athlinks.com/event/15776/results/Event/994637/Course/2152769/Results?page=1"
)
MASTER_URL = "https://www.athlinks.com/event/15776"


def test_extract_event_id_from_full_results_url():
    assert extract_event_id(FULL_RESULTS_URL) == "994637"


def test_extract_event_id_returns_none_for_master_url():
    # A bare master URL has no child event id in it.
    assert extract_event_id(MASTER_URL) is None


def test_extract_event_id_lowercase_event_with_results_keyword():
    assert extract_event_id("https://www.athlinks.com/event/994637/results") == "994637"


def test_extract_master_id_from_full_results_url():
    assert extract_master_id(FULL_RESULTS_URL) == "15776"


def test_extract_master_id_from_master_url():
    assert extract_master_id(MASTER_URL) == "15776"


def test_extract_master_id_returns_none_for_non_athlinks_url():
    assert extract_master_id("https://example.com/event/123") is None
