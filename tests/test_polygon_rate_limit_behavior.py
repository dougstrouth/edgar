"""Tests for enhanced Polygon/Massive client rate limiting and error classification.

Focus areas:
1. 429 backoff increases internal min_interval to >= 15.0 after first rate limit.
2. Exceptions raised in _make_request propagate and produce 'error' status in fetch_worker.
3. Empty resultsCount path returns 'empty' status (not mis-counted as error).

These tests avoid real network calls by monkeypatching requests.Session.get and time.sleep.
"""

from datetime import date
import time
import requests
from typing import List

from utils.polygon_client import PolygonClient, PolygonRateLimiter
from data_gathering.stock_data_gatherer_polygon import fetch_worker


class DummyResponse:
    def __init__(self, status_code=200, headers=None, json_data=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._json = json_data or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 429:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_rate_limit_backoff_increases_min_interval(monkeypatch):
    """First 429 response should trigger min_interval bump to >= 15.0."""
    rate_limiter = PolygonRateLimiter(calls_per_minute=10)  # initial min_interval = 6s
    client = PolygonClient(api_key="DUMMY", rate_limiter=rate_limiter, max_retries=3, retry_delay=0.01)

    resp_429 = DummyResponse(status_code=429, headers={"Retry-After": "0.01"}, json_data={})
    success_payload = {"resultsCount": 1, "results": [{"v": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1, "t": 1690000000000}]}
    resp_success = DummyResponse(status_code=200, json_data=success_payload)

    calls: List[int] = []

    def fake_get(url, params=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return resp_429
        return resp_success

    monkeypatch.setattr(client.session, 'get', fake_get)

    # Avoid real sleeping
    monkeypatch.setattr(time, 'sleep', lambda s: None)

    result = client.get_aggregates('TEST', date(2024, 1, 1), date(2024, 1, 2))
    assert result is not None
    # min_interval should have been bumped
    assert client.rate_limiter.min_interval >= 15.0, "min_interval did not increase after 429"


def test_fetch_worker_error_on_exception(monkeypatch):
    """Simulate repeated 429s leading to final failure -> fetch_worker returns status 'error'."""
    rate_limiter = PolygonRateLimiter(calls_per_minute=100)
    client = PolygonClient(api_key="DUMMY", rate_limiter=rate_limiter, max_retries=3, retry_delay=0.001)

    resp_429 = DummyResponse(status_code=429, headers={}, json_data={})

    def fake_get(url, params=None, timeout=None):
        return resp_429  # Always rate limited

    monkeypatch.setattr(client.session, 'get', fake_get)
    monkeypatch.setattr(time, 'sleep', lambda s: None)

    # Patch PolygonClient used inside fetch_worker to our prepared instance to control behavior
    def fake_client_init(self, api_key, rate_limiter=None, max_retries=3, retry_delay=5.0):
        # Reuse our configured client internals
        self.api_key = client.api_key
        self.rate_limiter = client.rate_limiter
        self.max_retries = client.max_retries
        self.retry_delay = client.retry_delay
        self.session = client.session

    monkeypatch.setattr('utils.polygon_client.PolygonClient.__init__', fake_client_init)

    job = {
        'ticker': 'FAIL',
        'start_date': date(2024, 1, 1),
        'end_date': date(2024, 1, 2),
        'api_key': 'DUMMY'
    }

    result = fetch_worker(job)
    assert result['status'] == 'error'
    assert 'ticker' in result and result['ticker'] == 'FAIL'


def test_fetch_worker_empty_on_no_results(monkeypatch):
    """When API returns resultsCount=0, fetch_worker should classify as 'empty'."""
    rate_limiter = PolygonRateLimiter(calls_per_minute=100)
    client = PolygonClient(api_key="DUMMY", rate_limiter=rate_limiter, max_retries=1, retry_delay=0.001)

    empty_payload = {"resultsCount": 0, "results": []}
    resp_empty = DummyResponse(status_code=200, json_data=empty_payload)

    def fake_get(url, params=None, timeout=None):
        return resp_empty

    monkeypatch.setattr(client.session, 'get', fake_get)
    monkeypatch.setattr(time, 'sleep', lambda s: None)

    def fake_client_init(self, api_key, rate_limiter=None, max_retries=3, retry_delay=5.0):
        self.api_key = client.api_key
        self.rate_limiter = client.rate_limiter
        self.max_retries = client.max_retries
        self.retry_delay = client.retry_delay
        self.session = client.session

    monkeypatch.setattr('utils.polygon_client.PolygonClient.__init__', fake_client_init)

    job = {
        'ticker': 'EMPTY',
        'start_date': date(2024, 1, 1),
        'end_date': date(2024, 1, 2),
        'api_key': 'DUMMY'
    }

    result = fetch_worker(job)
    assert result['status'] == 'empty'
    assert result['ticker'] == 'EMPTY'
    assert 'message' in result
