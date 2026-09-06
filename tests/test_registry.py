import pytest

from athlinks_scraper.providers import detect_provider
from athlinks_scraper.providers.athlinks import AthlinksProvider
from athlinks_scraper.providers.nyrr import NyrrProvider
from athlinks_scraper.providers.runsignup import RunSignupProvider


@pytest.mark.parametrize("url, cls", [
    ("https://www.athlinks.com/event/15776", AthlinksProvider),
    ("https://results.nyrr.org/event/24FROSTY/finishers", NyrrProvider),
    ("https://runsignup.com/Race/Results/100692", RunSignupProvider),
])
def test_detect_provider(url, cls):
    assert isinstance(detect_provider(url), cls)


def test_detect_provider_unknown_host():
    with pytest.raises(ValueError):
        detect_provider("https://example.com/results")
