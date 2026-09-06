"""Minimal stand-ins for requests.Session / Response used by the HTTP tests.

We fake at the Session level (not with a mocking library) because the code
under test only ever calls session.get(url, params=..., timeout=...) and then
response.raise_for_status() / response.json().
"""
import requests


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("HTTP %d" % self.status_code)

    def json(self):
        return self.payload


class FakeSession:
    """Returns the queued responses in order; records every call made."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []  # list of (url, params, timeout)

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if not self.responses:
            raise AssertionError("FakeSession ran out of queued responses")
        return self.responses.pop(0)
