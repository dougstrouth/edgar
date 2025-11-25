from datetime import date
import requests
import time
from unittest import mock

from utils.polygon_client import PolygonClient, PolygonRateLimiter


class DummyResponse:
    def __init__(self, status_code=200, headers=None, json_data=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._json = json_data or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_make_request_honors_retry_after_and_retries(monkeypatch):
    client = PolygonClient(api_key="DUMMY", rate_limiter=PolygonRateLimiter(calls_per_minute=60), max_retries=3, retry_delay=0.01)

    # First response is 429 with Retry-After header, second is success
    resp_429 = DummyResponse(status_code=429, headers={"Retry-After": "0.01"}, json_data={})
    success_payload = {"resultsCount": 1, "results": [{"v": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1, "t": 1690000000000}]}
    resp_200 = DummyResponse(status_code=200, headers={}, json_data=success_payload)

    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(1)
        # Return 429 once, then 200
        if len(calls) == 1:
            return resp_429
        return resp_200

    monkeypatch.setattr(client.session, 'get', fake_get)

    # Patch time.sleep to avoid slowing tests; record calls
    slept = []

    def fake_sleep(sec):
        slept.append(sec)

    monkeypatch.setattr(time, 'sleep', fake_sleep)

    # Call get_aggregates which uses _make_request internally
    result = client.get_aggregates('AAPL', date(2024, 1, 1), date(2024, 1, 2))

    assert result is not None
    # Ensure we attempted at least one sleep for the 429 backoff
    assert len(slept) >= 1
