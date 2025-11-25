#!/usr/bin/env python3
"""
Quick YFinance diagnostic - tests connectivity and error details.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import yfinance as yf  # type: ignore

print("=" * 80)
print("YFinance Diagnostic Test")
print("=" * 80)
print()

# Test 1: Simple AAPL query
print("Test 1: Fetching AAPL 5-day history...")
try:
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="5d")
    if df.empty:
        print("  ❌ FAILED: Empty dataframe returned")
    else:
        print(f"  ✅ SUCCESS: Got {len(df)} days of data")
        print(f"  Latest close: ${df['Close'].iloc[-1]:.2f}")
except Exception as e:
    print(f"  ❌ ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print()

# Test 2: Try AEE (the ticker that failed)
print("Test 2: Fetching AEE 5-day history...")
try:
    ticker = yf.Ticker("AEE")
    df = ticker.history(period="5d")
    if df.empty:
        print("  ❌ FAILED: Empty dataframe returned")
        print("  This could mean:")
        print("    - Ticker not found on YFinance")
        print("    - Rate limit hit")
        print("    - YFinance API issue")
    else:
        print(f"  ✅ SUCCESS: Got {len(df)} days of data")
        print(f"  Latest close: ${df['Close'].iloc[-1]:.2f}")
except Exception as e:
    print(f"  ❌ ERROR: {type(e).__name__}: {e}")
    error_str = str(e).lower()
    if "429" in error_str or "too many" in error_str:
        print("  >> This is a RATE LIMIT error")
    elif "404" in error_str or "not found" in error_str:
        print("  >> This ticker may not exist on YFinance")
    print()
    import traceback
    traceback.print_exc()

print()

# Test 3: Check yfinance version
print("Test 3: Environment info...")
print(f"  yfinance version: {yf.__version__}")
import requests
print(f"  requests version: {requests.__version__}")

print()
print("=" * 80)
print("Diagnostic complete")
print("=" * 80)
