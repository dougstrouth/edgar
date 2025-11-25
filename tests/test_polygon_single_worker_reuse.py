"""Ensure stock gatherer single-worker path reuses a shared client so
rate limiter/backoff state persists across jobs, matching ticker info gatherer.

This test avoids real network by monkeypatching requests and time.sleep.
"""

from datetime import date
import time
import requests

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


def test_single_worker_reuses_rate_limiter_state(monkeypatch):
    """First job triggers 429 then success, bumping min_interval.
    Second job, using same shared client, should see the bumped min_interval
    and proceed without recreating limiter state.
    """
    # Start with faster nominal CPM to emphasize bump effect
    limiter = PolygonRateLimiter(calls_per_minute=10)
    client = PolygonClient(api_key="DUMMY", rate_limiter=limiter, max_retries=3, retry_delay=0.001)

    # Sequence: 429 -> success (job1), then success (job2)
    resp_429 = DummyResponse(status_code=429, headers={"Retry-After": "0.001"}, json_data={})
    payload = {"resultsCount": 1, "results": [{"v": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1, "t": 1690000000000}]}
    resp_ok = DummyResponse(status_code=200, json_data=payload)

    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        # 1st call -> 429, 2nd -> success, 3rd -> success
        if calls["n"] == 1:
            return resp_429
        return resp_ok

    monkeypatch.setattr(client.session, "get", fake_get)
    monkeypatch.setattr(time, "sleep", lambda s: None)

    job1 = {
        "ticker": "AAA",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 2),
        "api_key": "DUMMY",
    }
    job2 = {
        "ticker": "BBB",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 2),
        "api_key": "DUMMY",
    }

    # First call should succeed but bump min_interval due to initial 429
    r1 = fetch_worker(job1, client=client)
    assert r1["status"] == "success"
    assert client.rate_limiter.min_interval >= 15.0

    # Second call reuses same client and should still succeed
    prior_min = client.rate_limiter.min_interval
    r2 = fetch_worker(job2, client=client)
    assert r2["status"] == "success"
    # Limiter state persists (no reset)
    assert client.rate_limiter.min_interval == prior_min
    # Total HTTP calls: 3 (429+success for job1, success for job2)
    assert calls["n"] == 3
