# Production Workflow Guide

## Overview
This guide covers the recommended production workflows for gathering stock data and ticker information using the Polygon/Massive.com API with free tier constraints.

## Free Tier Constraints
- ⚠️ **5 API calls per minute** (strictly enforced)
- ⚠️ **2 years historical data** (not 5 years)
- ✅ **Unlimited daily calls** (no daily cap)
- ✅ **1-day delayed data** (previous day's close)

---

## Complete Production Workflow

### 1. Prioritize Tickers (Optional but Recommended)

Generate a prioritized backlog to focus on the most valuable tickers first:

**VS Code Debug:**
- `Backlog: generate (default weights)` - All tickers with balanced scoring
- `Backlog: generate top 1200` - Limit to top 1200 tickers
- `Backlog: generate custom weights` - Fine-tune prioritization

**Terminal:**
```bash
# Generate full prioritized backlog
python main.py generate_backlog

# Limit to top 1200 tickers
python main.py generate_backlog --limit 1200

# Custom weight distribution
python main.py generate_backlog --weights "xbrl_richness=0.3,key_metrics=0.2,stock_data_need=0.35,filing_activity=0.15"
```

This creates the `prioritized_tickers_stock_backlog` table which is automatically used by both stock and ticker info gatherers.

---

### 2. Gather Stock Price Data (Aggregates)

**Production Runs (VS Code Debug):**
- `Stocks (Polygon): PRODUCTION - append 2yr` - **Recommended for daily updates**
- `Stocks (Polygon): initial full 2yr` - First-time run (fetches all 2 years)
- `Stocks (Polygon): full refresh 2yr` - Rebuild everything (rarely needed)

**Testing Runs (VS Code Debug):**
- `Stocks (Polygon): TEST - 10 tickers` - Quick test with 10 tickers
- `Stocks (Polygon): TEST - 50 tickers` - Moderate test with 50 tickers

**Terminal Commands:**

```bash
# PRODUCTION: Daily append (most common)
python main.py gather-stocks-polygon --mode append --lookback-years 2

# PRODUCTION: Initial load (first time)
python main.py gather-stocks-polygon --mode initial_load --lookback-years 2

# PRODUCTION: Full refresh (rebuild everything)
python main.py gather-stocks-polygon --mode full_refresh --lookback-years 2

# TEST: Quick 10-ticker test
python main.py gather-stocks-polygon --mode append --lookback-years 2 --limit 10

# TEST: 50-ticker initial load test
python main.py gather-stocks-polygon --mode initial_load --lookback-years 2 --limit 50
```

**Overnight Run (9-hour max runtime):**
```bash
nohup python main.py gather-stocks-polygon \
  --mode append \
  --lookback-years 2 \
  > logs/overnight_polygon_$(date +%Y%m%d_%H%M).log 2>&1 &

# Save process ID
echo $! > /tmp/polygon_overnight.pid
```

---

### 3. Gather Ticker Info (Reference Data)

**Production Runs (VS Code Debug):**
- `Ticker Info (Polygon): PRODUCTION - prioritized` - **Recommended (uses backlog)**
- `Ticker Info (Polygon): PRODUCTION - all missing` - Fetch all missing tickers
- `Ticker Info (Polygon): force refresh all` - Re-fetch everything (expensive)

**Testing Runs (VS Code Debug):**
- `Ticker Info (Polygon): TEST - top 50` - Test with top 50 prioritized tickers
- `Ticker Info (Polygon): TEST - top 200` - Test with top 200 prioritized tickers

**Terminal Commands:**

```bash
# PRODUCTION: Prioritized refresh (recommended - uses backlog)
python main.py gather-ticker-info --prioritize

# PRODUCTION: Fetch all missing tickers (no prioritization)
python main.py gather-ticker-info

# PRODUCTION: Force refresh everything (expensive!)
python main.py gather-ticker-info --force-refresh

# TEST: Top 50 prioritized tickers
python main.py gather-ticker-info --prioritize --limit 50

# TEST: Top 200 prioritized tickers
python main.py gather-ticker-info --prioritize --limit 200

# Specific tickers only
python main.py gather-ticker-info --tickers AAPL MSFT GOOGL
```

---

### 4. Load Data into Database

**VS Code Debug:**
- `Load: ticker info` - Load Polygon ticker reference data
- `Load: stocks (stock_history)` - Load stock price history
- `Load: stock_history (direct)` - Direct loader script

**Terminal:**
```bash
# Load ticker info
python main.py load-ticker-info

# Load stock history
python main.py load_stocks

# Direct loader (alternative)
python data_processing/load_supplementary_data.py stock_history
```

---

### 5. Enrich Ticker Metadata (Optional)

Populate `massive_tickers` table with comprehensive ticker metadata:

**VS Code Debug:**
- `Enrich: massive_tickers (active only)` - Active tickers only (recommended)
- `Enrich: massive_tickers (include delisted)` - Include delisted tickers

**Terminal:**
```bash
# Active tickers only (recommended)
python main.py enrich-tickers --active-only

# Include delisted tickers
python main.py enrich-tickers --include-delisted
```

⚠️ **Note:** This can make 20+ API calls depending on pagination. Use sparingly.

---

## Recommended Daily Workflow

```bash
# 1. (Optional) Generate fresh prioritization if needed
python main.py generate_backlog --limit 1200

# 2. Gather stock price updates (append mode)
python main.py gather-stocks-polygon --mode append --lookback-years 2

# 3. Gather ticker info for new/missing tickers (prioritized)
python main.py gather-ticker-info --prioritize --limit 200

# 4. Load everything into database
python main.py load_stocks
python main.py load-ticker-info

# 5. Validate database integrity
python main.py validate
```

---

## Rate Limiting Strategy

The code automatically enforces rate limits via `PolygonRateLimiter`:
- **Default:** 5 calls/minute with 12-second minimum spacing
- **Adaptive:** Increases to 15s spacing after hitting 429
- **Conservative:** Set `aggressive_spacing=True` for 1.25x spacing buffer

---

## Monitoring & Progress

### Real-time Monitoring
```bash
# Watch live progress
tail -f logs/stock_data_gatherer_polygon.log

# Check for errors
grep ERROR logs/*.log

# Check progress updates
grep "📈 Progress" logs/*.log
```

### Database Checkpoints
Stock gatherer writes to database every 15 minutes automatically.

### Final Summary Example
```
⏱️  Total Runtime: 2.15 hours
📊 Jobs Processed: 2402/2402
✅ Success: 2350
⚠️  Empty: 45
❌ Errors: 7
```

---

## Troubleshooting

### Rate Limit Errors (429)
- Code automatically backs off to 15s+ spacing
- Reduces `calls_per_minute` by 1 on each 429
- Waits 60s before retry

### Missing Tickers in stock_history
- Run `python main.py enrich-tickers --active-only`
- Check validation with `python main.py validate`
- Review `massive_tickers` table for coverage

### Orphaned Facts
- Review `XBRL_ORPHAN_ANALYSIS.md` for remediation strategies
- Run `python main.py investigate_orphans` for diagnostics

---

## VS Code Launch Configuration Highlights

### Production Ready
All production configurations enforce the 2-year free tier limit and use appropriate modes:
- `--lookback-years 2` (free tier max)
- `--mode append` (incremental updates)
- `--prioritize` (uses backlog table)

### Testing Ready
Testing configurations limit ticker counts for quick validation:
- `--limit 10` or `--limit 50` for rapid testing
- All other production parameters preserved

### Easy Access
Organized by category in VS Code debug panel:
1. **Core EDGAR Pipeline** - fetch → parse → load → validate
2. **Ticker Prioritization** - backlog generation with weights
3. **Polygon Stock Data** - production and testing runs
4. **Polygon Ticker Info** - production and testing runs
5. **Data Loading** - all loaders
6. **Utilities** - validation, cleanup, inspection

---

## Key Files
- `main.py` - Orchestrator entry point
- `data_gathering/stock_data_gatherer_polygon.py` - Stock price gatherer
- `data_gathering/ticker_info_gatherer_polygon.py` - Ticker info gatherer
- `scripts/generate_prioritized_backlog.py` - Ticker prioritization
- `utils/polygon_client.py` - Rate-limited API client
- `.vscode/launch.json` - VS Code debug configurations
