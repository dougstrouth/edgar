#!/usr/bin/env python3
"""
Simple sequential test of your 9 tickers to verify capacity.
"""
import time
import yfinance as yf  # type: ignore

tickers = ['AEE', 'CMS', 'CMS-PC', 'CMSA', 'CMSC', 'CMSD', 'EIX', 'ES', 'TXNM']

print("=" * 80)
print(f"Testing {len(tickers)} tickers sequentially with 0.1s delay")
print("=" * 80)
print()

successes = 0
failures = 0
start_time = time.time()

for i, ticker in enumerate(tickers, 1):
    print(f"[{i}/{len(tickers)}] Testing {ticker}...", end=" ", flush=True)
    tick_start = time.time()
    
    try:
        df = yf.Ticker(ticker).history(period="5d")
        tick_elapsed = time.time() - tick_start
        
        if df.empty:
            print(f"❌ Empty ({tick_elapsed:.2f}s)")
            failures += 1
        else:
            close_price = df['Close'].iloc[-1]
            print(f"✅ Success - ${close_price:.2f} ({tick_elapsed:.2f}s)")
            successes += 1
    except Exception as e:
        tick_elapsed = time.time() - tick_start
        print(f"❌ Error: {e} ({tick_elapsed:.2f}s)")
        failures += 1
    
    # Small delay between requests
    if i < len(tickers):
        time.sleep(0.1)

total_elapsed = time.time() - start_time

print()
print("=" * 80)
print(f"Results: {successes}/{len(tickers)} successful ({successes/len(tickers)*100:.1f}%)")
print(f"Total time: {total_elapsed:.1f}s")
print(f"Average per ticker: {total_elapsed/len(tickers):.2f}s")
print("=" * 80)

if successes == len(tickers):
    print("\n✅ ALL TESTS PASSED - YFinance is working well!")
    print("\nRecommended settings for .env:")
    print("  YFINANCE_DISABLED=0")
    print("  YFINANCE_MAX_RETRIES=5")
    print("  YFINANCE_BASE_DELAY=15.0")
    print(f"\nEstimated capacity: ~{int(3600 / (total_elapsed/len(tickers)))} tickers/hour")
elif successes > 0:
    print(f"\n⚠️  PARTIAL SUCCESS - {failures} failures")
    print("Some tickers may not exist on YFinance or data is unavailable")
else:
    print("\n❌ ALL TESTS FAILED")
    print("Either YFinance is down or there's a rate limit issue")
