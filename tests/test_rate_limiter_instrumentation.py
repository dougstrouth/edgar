"""Instrumentation test for PolygonRateLimiter spacing behavior.

This test simulates a sequence of requests under the unified wait logic
without incurring real wall clock delay by monkeypatching time.time and time.sleep.

We validate:
- Intervals between recorded timestamps are never less than min_interval - epsilon.
- Average interval converges near min_interval for calls_per_minute sizing.
- Window logic prevents > calls_per_minute in any 60s simulated span.
"""

from typing import List

from utils.polygon_client import PolygonRateLimiter

EPSILON = 0.001  # floating tolerance


def test_rate_limiter_spacing_instrumentation(monkeypatch):
    calls_per_minute = 4  # => min_interval = 15.0s
    limiter = PolygonRateLimiter(calls_per_minute=calls_per_minute)
    expected_min_interval = 60.0 / calls_per_minute

    # Simulated time state
    simulated_time = 1_000_000.0  # arbitrary epoch baseline
    sleep_calls: List[float] = []

    def fake_time():
        return simulated_time

    def fake_sleep(duration: float):
        nonlocal simulated_time
        # Advance simulated time instead of real sleeping
        simulated_time += duration
        sleep_calls.append(duration)

    monkeypatch.setattr("time.time", fake_time)
    monkeypatch.setattr("time.sleep", fake_sleep)

    # Perform a series of API wait cycles
    request_timestamps: List[float] = []
    total_requests = 10
    for _ in range(total_requests):
        limiter.wait_if_needed()
        # Capture last appended timestamp (already recorded inside limiter)
        request_timestamps.append(limiter.request_timestamps[-1])

    # Compute intervals between consecutive requests
    intervals = [b - a for a, b in zip(request_timestamps[:-1], request_timestamps[1:])]

    # Basic assertions
    assert len(intervals) == total_requests - 1
    assert all(iv >= expected_min_interval - EPSILON for iv in intervals), (
        f"Observed interval below expected min_interval: {intervals}" )

    avg_interval = sum(intervals) / len(intervals)
    # Allow small drift upward because window full condition may extend waits
    assert avg_interval >= expected_min_interval - 0.1, (
        f"Average interval too low: {avg_interval} vs {expected_min_interval}" )

    # Ensure we did not exceed calls_per_minute in any sliding 60s span
    # Using simulated timestamps window check
    for idx in range(len(request_timestamps)):
        window_start = request_timestamps[idx]
        window_end = window_start + 60.0 + EPSILON
        in_window = [t for t in request_timestamps if window_start <= t <= window_end]
        assert len(in_window) <= calls_per_minute, (
            f"Exceeded {calls_per_minute} requests in 60s window starting at {window_start}" )

    # Instrumentation output (helpful if test fails)
    print("Simulated intervals:", intervals)
    print("Sleep durations invoked:", sleep_calls)
    print(f"Average interval: {avg_interval:.4f}s (expected ~{expected_min_interval:.2f}s)")
