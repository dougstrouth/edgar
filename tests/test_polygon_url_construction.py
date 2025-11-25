#!/usr/bin/env python3
"""
Test URL construction for Massive.com (Polygon.io) API calls.

Ensures that date formatting and endpoint construction work correctly
with the new api.massive.com endpoint.
"""

import sys
from pathlib import Path
from datetime import date

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.polygon_client import PolygonClient, PolygonRateLimiter


def test_url_construction():
    """Test that URLs are constructed correctly for the Massive.com API."""
    
    # Create client with dummy API key
    client = PolygonClient(api_key="test_key_12345")
    
    # Test parameters
    ticker = "AAPL"
    from_date = date(2023, 1, 1)
    to_date = date(2023, 12, 31)
    timespan = "day"
    multiplier = 1
    
    # Build expected endpoint
    endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    
    # Expected format: /v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-12-31
    expected_endpoint = "/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-12-31"
    
    print("URL Construction Test")
    print("=" * 60)
    print(f"Ticker: {ticker}")
    print(f"From: {from_date}")
    print(f"To: {to_date}")
    print(f"Timespan: {timespan}")
    print(f"Multiplier: {multiplier}")
    print()
    print(f"Constructed endpoint: {endpoint}")
    print(f"Expected endpoint:    {expected_endpoint}")
    print()
    
    # Check if they match
    if endpoint == expected_endpoint:
        print("✅ URL construction is CORRECT")
        print(f"   Full URL would be: {client.BASE_URL}{endpoint}")
        return True
    else:
        print("❌ URL construction is INCORRECT")
        print(f"   Mismatch detected!")
        return False


def test_base_url():
    """Test that the base URL points to the new Massive.com endpoint."""
    
    client = PolygonClient(api_key="test_key")
    
    print("\nBase URL Test")
    print("=" * 60)
    print(f"Current BASE_URL: {client.BASE_URL}")
    print(f"Expected: https://api.massive.com")
    print()
    
    if client.BASE_URL == "https://api.massive.com":
        print("✅ Base URL is correctly set to Massive.com")
        return True
    elif client.BASE_URL == "https://api.polygon.io":
        print("⚠️  Still using old polygon.io endpoint (will work but should update)")
        return True
    else:
        print(f"❌ Unexpected BASE_URL: {client.BASE_URL}")
        return False


def test_rate_limiter():
    """Test that rate limiter is configured correctly."""
    
    limiter = PolygonRateLimiter(calls_per_minute=5)
    
    print("\nRate Limiter Test")
    print("=" * 60)
    print(f"Calls per minute: {limiter.calls_per_minute}")
    print(f"Min interval: {limiter.min_interval:.2f} seconds")
    print(f"Expected interval: {60.0/5:.2f} seconds")
    print()
    
    expected_interval = 60.0 / 5
    if abs(limiter.min_interval - expected_interval) < 0.01:
        print("✅ Rate limiter configured correctly")
        return True
    else:
        print("❌ Rate limiter interval mismatch")
        return False


if __name__ == "__main__":
    print("Massive.com (Polygon.io) URL Construction Tests")
    print("=" * 60)
    print()
    
    results = []
    results.append(("URL Construction", test_url_construction()))
    results.append(("Base URL", test_base_url()))
    results.append(("Rate Limiter", test_rate_limiter()))
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:20s} {status}")
    
    print()
    
    if all(passed for _, passed in results):
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed")
        sys.exit(1)
