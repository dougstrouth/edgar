# YFinance Capacity Testing Guide

This guide explains how to test YFinance API capacity to determine optimal settings before running full data gathering operations.

## Background

Yahoo Finance has strict rate limits that aren't publicly documented. The `tests/test_yfinance_capacity.py` script helps you:

1. Test how many tickers you can fetch before hitting rate limits
2. Determine optimal batch sizes and delays
3. Estimate total gathering time for your dataset
4. Get recommended configuration settings

## Prerequisites

1. You must have already run `main.py fetch` to populate the EDGAR data
2. Your database should have tickers loaded from SEC filings
3. YFinance package should be installed (already in project dependencies)

## Running the Capacity Test

### Quick Test (Recommended First Step)

Tests 3 small batches (5, 10, 15 tickers) with varying delays:

```bash
python tests/test_yfinance_capacity.py --test-type quick
```

**Use when:** You want a fast sanity check (~1-2 minutes)

### Medium Test

Tests 4 batches (10, 20, 30, 50 tickers) with progressive delays:

```bash
python tests/test_yfinance_capacity.py --test-type medium
```

**Use when:** You want better data without a long wait (~5-10 minutes)

### Extensive Test

Tests 7 batches from 10 to 200 tickers with adaptive delays:

```bash
python tests/test_yfinance_capacity.py --test-type extensive
```

**Use when:** You need comprehensive data for production settings (~30-60 minutes)

### Custom Ticker Limit

Limit the number of tickers loaded from the database:

```bash
python tests/test_yfinance_capacity.py --test-type medium --max-tickers 200
```

## Understanding the Output

### During Testing

The script will log:
- Batch progress with success/failure counts
- Average time per ticker
- Any rate limit warnings
- Adaptive behavior (e.g., increasing delays)

### Summary Report

At the end, you'll get:

```
================================================================================
YFINANCE CAPACITY TEST SUMMARY
================================================================================
Total tickers tested: 110
Total successes: 105
Overall success rate: 95.5%
Total time: 245.3s
Average time per ticker: 2.23s
Rate limit hit: False

Per-batch breakdown:
--------------------------------------------------------------------------------
Batch 1: 10 tickers, 100.0% success, 0.10s delay, 2.15s avg
Batch 2: 20 tickers, 95.0% success, 0.20s delay, 2.28s avg
...

RECOMMENDATIONS:
--------------------------------------------------------------------------------
1. Maximum reliable batch size: 50 tickers
2. Average time per ticker: 2.23s
3. Estimated tickers per hour: 1613
4. Rate limit NOT hit - can potentially go faster
   Recommended: Test with slightly larger batches
```

## Interpreting Results

### Success Rate
- **>90%**: Excellent, current settings are good
- **70-90%**: Acceptable, but may want to increase delays
- **<70%**: Too aggressive, increase delays significantly

### Rate Limit Hit
- **False**: Good! You have headroom to potentially go faster
- **True**: You've hit the limit. Use more conservative settings

### Recommendations

Based on your results:

1. **If rate limit NOT hit and success >90%:**
   - You can likely increase batch size or reduce delays
   - Update `.env` with slightly more aggressive settings

2. **If rate limit hit:**
   - Use the maximum successful batch size from results
   - Add 20-30% more delay to be safe
   - Consider running overnight with conservative settings

3. **If success rate is poor (<70%):**
   - YFinance may be experiencing issues
   - Try again later or use more conservative settings

## Applying Results to Your Configuration

After testing, update your `.env` file:

```bash
# Enable YFinance gathering
YFINANCE_DISABLED=0

# Based on test results showing avg 2.23s per ticker:
YFINANCE_MAX_RETRIES=5
YFINANCE_BASE_DELAY=30.0  # Conservative for rate limit recovery

# For gathering in batches (not directly tested, but informed by results):
MAX_CPU_IO_WORKERS=2  # Low for API-bound work
```

## Example Workflow

```bash
# 1. Run quick test to verify connectivity
python test_yfinance_capacity.py --test-type quick

# 2. If successful, run medium test for better data
python test_yfinance_capacity.py --test-type medium

# 3. Review results and update .env file
vim .env  # Set YFINANCE_DISABLED=0 and adjust other settings

# 4. Run actual data gathering with conservative settings
python main.py gather-stocks --mode append

# 5. Monitor logs for rate limits, adjust as needed
tail -f data_gathering/logs/stock_data_gatherer_*.log
```

## Estimating Full Gathering Time

From your test results:

```
Tickers in database: 5,000
Average time per ticker: 2.5s
Success rate: 95%

Estimated time:
- Ideal: 5,000 × 2.5s = 12,500s = 3.5 hours
- With failures/retries: 3.5 hours × 1.25 = ~4.4 hours
- Add overhead: ~5 hours total
```

## Troubleshooting

### "No tickers found in database"
Run `main.py fetch` first to populate EDGAR data

### All requests fail immediately
- Check internet connection
- Verify yfinance package is installed: `pip list | grep yfinance`
- Try the minimal test: Set `YFINANCE_MINIMAL=1` in `.env` and run stock gatherer

### Rate limit hit on first batch
- YFinance may have stricter limits currently
- Try again in a few hours
- Use very conservative settings (delay=1.0s or more)

### Inconsistent results between runs
- Normal! YFinance API can be unstable
- Run tests at different times of day
- Use the most conservative estimates for production

## Notes

- **Ticker Prioritization**: The test uses your prioritizer to test high-value tickers first
- **Data Preservation**: The test only fetches minimal data (5 days history) to minimize API calls
- **Logs**: Detailed results are saved to `logs/yfinance_capacity_test_YYYYMMDD_HHMMSS.log`
- **Safe Testing**: The test stops early if it detects rate limits or repeated failures

## Next Steps

After determining your capacity:

1. Update `.env` with appropriate settings
2. Consider running initial gathering overnight
3. Set up monitoring for rate limit warnings
4. Plan for incremental updates (append mode) rather than full refreshes
