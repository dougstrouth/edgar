#!/usr/bin/env python3
"""
Polygon.io Capacity and Connectivity Test

Tests the Polygon.io API to verify:
1. API key is valid
2. Rate limiting is working correctly  
3. Data can be fetched successfully
4. Estimate gathering capacity

Usage:
    # First, add your API key to .env:
    # POLYGON_API_KEY=your_key_here
    
    python test_polygon_capacity.py
"""

import sys
import time
from pathlib import Path
from datetime import date, timedelta

sys.path.append(str(Path(__file__).resolve().parent))

from utils.config_utils import AppConfig
from utils.polygon_client import PolygonClient, PolygonRateLimiter
from utils.database_conn import ManagedDatabaseConnection

print("=" * 80)
print("Polygon.io API Capacity Test")
print("=" * 80)
print()

# Load config
try:
    config = AppConfig(calling_script_path=Path(__file__))
except SystemExit as e:
    print(f"❌ Configuration failed: {e}")
    sys.exit(1)

# Get API key
api_key = config.get_optional_var("POLYGON_API_KEY")
if not api_key or api_key == "your_polygon_api_key_here":
    print("❌ POLYGON_API_KEY not set in .env file")
    print()
    print("To fix:")
    print("  1. Sign up for free at: https://polygon.io/")
    print("  2. Get your API key from the dashboard")
    print("  3. Add to .env: POLYGON_API_KEY=your_key_here")
    print()
    sys.exit(1)

print(f"✅ API key found: {api_key[:8]}...")
print()

# Create client
client = PolygonClient(api_key)

# Test 1: Connectivity
print("Test 1: API Connectivity")
print("-" * 40)
if client.check_connectivity():
    print("✅ API is accessible and key is valid")
else:
    print("❌ Cannot connect to Polygon API")
    print("   Check your internet connection and API key")
    sys.exit(1)
print()

# Test 2: Fetch sample tickers
print("Test 2: Fetching Sample Data")
print("-" * 40)

# Get a few tickers from the database
tickers_to_test = []
try:
    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as conn:
        if conn:
            result = conn.execute("SELECT DISTINCT ticker FROM tickers LIMIT 10").fetchall()
            tickers_to_test = [row[0] for row in result]
except Exception as e:
    print(f"⚠️  Could not get tickers from database: {e}")
    print("Using default test tickers instead")
    tickers_to_test = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

print(f"Testing with {len(tickers_to_test)} tickers: {', '.join(tickers_to_test[:5])}")
print()

# Test fetching
end_date = date.today() - timedelta(days=1)  # Yesterday (free tier is 1-day delayed)
start_date = end_date - timedelta(days=30)   # Last 30 days

successes = 0
failures = 0
total_records = 0
test_start = time.time()

for i, ticker in enumerate(tickers_to_test, 1):
    print(f"[{i}/{len(tickers_to_test)}] {ticker}...", end=" ", flush=True)
    
    tick_start = time.time()
    results = client.get_aggregates(ticker, start_date, end_date)
    tick_elapsed = time.time() - tick_start
    
    if results:
        print(f"✅ {len(results)} days ({tick_elapsed:.2f}s)")
        successes += 1
        total_records += len(results)
    else:
        print(f"❌ No data ({tick_elapsed:.2f}s)")
        failures += 1

test_elapsed = time.time() - test_start

print()
print("=" * 80)
print("Test Results")
print("=" * 80)
print(f"Tickers tested: {len(tickers_to_test)}")
print(f"Successful: {successes}")
print(f"Failed: {failures}")
print(f"Total records: {total_records}")
print(f"Total time: {test_elapsed:.1f}s")
print(f"Average per ticker: {test_elapsed/len(tickers_to_test):.2f}s")
print()

# Calculate capacity
if successes > 0:
    calls_per_minute = 5  # Free tier limit
    seconds_per_call = test_elapsed / len(tickers_to_test)
    
    # Account for rate limiting
    effective_rate = min(60 / seconds_per_call, calls_per_minute)
    
    print("=" * 80)
    print("Capacity Estimates")
    print("=" * 80)
    print(f"Rate limit: {calls_per_minute} calls/minute")
    print(f"Actual rate: ~{effective_rate:.1f} calls/minute")
    print()
    print(f"Estimated capacity:")
    print(f"  Per hour: ~{int(effective_rate * 60)} tickers")
    print(f"  Per day (24/7): ~{int(effective_rate * 60 * 24)} tickers")
    print(f"  Per day (8hr run): ~{int(effective_rate * 60 * 8)} tickers")
    print()
    
    # Get total ticker count
    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as conn:
            if conn:
                total_tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM tickers").fetchone()[0]
                days_to_complete = total_tickers / (effective_rate * 60 * 8)
                print(f"Your database has {total_tickers} tickers")
                print(f"Estimated time to fetch all: {days_to_complete:.1f} days (running 8hrs/day)")
    except Exception:
        pass
    
    print()
    print("=" * 80)
    print("✅ Polygon.io is working!")
    print("=" * 80)
    print()
    print("Recommended next steps:")
    print("  1. Start with a test run:")
    print("     python data_gathering/stock_data_gatherer_polygon.py --mode initial_load --limit 50")
    print()
    print("  2. Or use the orchestrator:")
    print("     python main.py gather-stocks-polygon --limit 50")
    print()
    print("  3. Monitor logs:")
    print("     tail -f data_gathering/logs/stock_data_gatherer_polygon*.log")
else:
    print("=" * 80)
    print("❌ All tests failed")
    print("=" * 80)
    print()
    print("Possible issues:")
    print("  - Invalid API key")
    print("  - Network connectivity problems")
    print("  - Polygon.io service outage")
    print("  - Rate limit already hit")
