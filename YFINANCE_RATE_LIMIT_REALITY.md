# YFinance Rate Limit Reality Check

## What We Discovered

YFinance has **extremely aggressive rate limits** that are not publicly documented:

- Rate limits trigger after just a **few requests** (sometimes even 1-5)
- Limits persist for **1-24 hours** (unpredictable)
- Error: "Too Many Requests. Rate limited. Try after a while."
- Even testing/diagnostics can trigger the limit

## Current Status

✅ **YFinance package is installed and working**
❌ **You are currently rate-limited** (as of Nov 17, 2025 ~8:44 PM)
⏱️ **Estimated wait time: 6-24 hours** before limits reset

## When Can You Use YFinance?

### Wait Period
You must wait for the rate limit to reset. Try:
- **Minimum: 6 hours** from your last request
- **Safe: 24 hours** to be sure
- **Test first**: Always run `python test_yfinance_simple.py` before gathering

### Testing Timeline

```bash
# Current time: Nov 17, 2025 ~8:44 PM
# Last request: Nov 17, 2025 ~8:44 PM

# Earliest retry: Nov 18, 2025 ~2:44 AM (6 hours)
# Safe retry: Nov 18, 2025 ~8:44 PM (24 hours)

# When ready:
python test_yfinance_simple.py

# If all 9 tickers pass, you can try gathering
```

## Realistic Gathering Strategy

### Option 1: Ultra-Conservative (Recommended)

**Settings:**
```bash
YFINANCE_DISABLED=0
YFINANCE_BASE_DELAY=60.0  # 1 minute between tickers
MAX_CPU_IO_WORKERS=1       # Single-threaded only
```

**Capacity:**
- ~60 tickers per hour
- ~500 tickers per 8-hour overnight run
- High risk of hitting limits even with delays

**Use for:**
- Your top 50-100 most important companies
- Quarterly updates only
- Supplementary data, not primary source

### Option 2: Spot Checks Only

**Don't try to gather all tickers**. Instead:
1. Use SEC EDGAR data as your primary source (you have this!)
2. Use YFinance only for:
   - Validating a few key metrics
   - Real-time price checks for analysis
   - One-off lookups when needed
3. Manually collect critical data for your top 10-20 companies

### Option 3: Alternative Data Sources

Consider switching to services with clearer limits:

1. **Alpha Vantage** (Free tier: 25 req/day, 5 req/min)
   - More generous than YFinance
   - Clear documentation
   - Stable limits

2. **Polygon.io** (Free tier: 5 req/min)
   - Good free tier
   - Historical data available
   - WebSocket for real-time

3. **IEX Cloud** (Free tier available)
   - Clean API
   - Good documentation
   - Predictable limits

4. **EODHD** (End of Day Historical Data)
   - Affordable paid plans
   - Bulk downloads available
   - Good for historical analysis

## What Works Well in Your Project

✅ **SEC EDGAR data** - Unlimited, reliable, authoritative
✅ **FRED macroeconomic data** - You have this working
✅ **Your prioritization system** - Ready to use when limits allow
✅ **Data preservation** - Parquet recovery system in place

## Recommended Workflow

### Phase 1: EDGAR First (Now)
```bash
# Focus on what you can control
python main.py fetch          # Get SEC filings (no limits!)
python main.py parse           # Extract data
python main.py load            # Load to database
python main.py gather-macro    # Get FRED data (you have API key)
```

### Phase 2: YFinance When Ready (Tomorrow+)
```bash
# After 24 hours, test connectivity
python test_yfinance_simple.py

# If successful, gather top 50 tickers only
# Edit .env: YFINANCE_DISABLED=0, BASE_DELAY=60.0
python main.py gather-stocks --mode append

# Monitor logs for rate limits
tail -f data_gathering/logs/stock_data_gatherer*.log
```

### Phase 3: Analysis (Ongoing)
```bash
# You have rich EDGAR data - use it!
# Financial statements, XBRL facts, filing metadata
# This is actually better than YFinance for fundamentals
```

## Key Insights

1. **YFinance limits are worse than expected** - Your `YFINANCE_DISABLED=1` default was correct
2. **SEC data is your strength** - You have comprehensive, unlimited access
3. **Quality over quantity** - 50 well-chosen tickers > 5000 rate-limited ones
4. **Alternative sources may be better** - Consider switching for price/market data

## Testing Checklist

Before attempting any YFinance gathering:

- [ ] Waited at least 6 hours (24 hours safer)
- [ ] Ran `python test_yfinance_simple.py` successfully
- [ ] Set `YFINANCE_BASE_DELAY=60.0` or higher
- [ ] Prioritized tickers (use top 50-100 only)
- [ ] Set up monitoring/logging
- [ ] Have plan B if rate limited again

## Bottom Line

**YFinance is too unreliable for production use at scale.**

Your project's real value is in the **SEC EDGAR data** you're gathering - that's comprehensive, authoritative, and unlimited. Use YFinance as a supplement, not a foundation.

Consider:
- Keep YFinance disabled by default ✅
- Focus on maximizing EDGAR data extraction ✅
- Add alternative price data sources if needed
- Use your prioritization for the few tickers you do fetch from YFinance
