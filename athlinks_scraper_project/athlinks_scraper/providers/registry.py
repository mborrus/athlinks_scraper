from .athlinks import AthlinksProvider
from .base import RaceProvider
from .nyrr import NyrrProvider
from .runsignup import RunSignupProvider

PROVIDERS = [AthlinksProvider(), NyrrProvider(), RunSignupProvider()]


def detect_provider(url: str) -> RaceProvider:
    for provider in PROVIDERS:
        if provider.matches(url):
            return provider
    raise ValueError(
        f"Unsupported URL: {url}. Supported hosts: athlinks.com, results.nyrr.org, runsignup.com"
    )
