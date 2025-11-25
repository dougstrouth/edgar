# Polygon Stock Data Gatherer - Overnight Run Guide

## Features

✅ **9-Hour Maximum Runtime** - Automatically stops after 9 hours  
✅ **Rate Limit Enforcement** - Respects 5 calls/minute free tier limit  
✅ **Periodic Database Writes** - Writes to database every 15 minutes  
✅ **Progress Tracking** - Shows elapsed time, remaining time, and success/error counts  
✅ **Automatic Recovery** - Saves parquet files continuously, can resume if interrupted  

## How to Run Overnight

### 1. Start the Pipeline (No Limit - Full Run)

```bash
cd /Users/dougstrouth/github_noicloud/edgar
.venv/bin/python main.py gather-stocks-polygon --mode append --lookback-years 2 > logs/overnight_polygon_$(date +%Y%m%d).log 2>&1 &
```

**Note**: Changed to `--lookback-years 2` to match your free tier (2 years historical data)

### 2. Monitor Progress

```bash
# Watch live progress
tail -f logs/overnight_polygon_*.log

# Check for errors
grep ERROR logs/overnight_polygon_*.log

# Check progress updates
grep "📈 Progress" logs/overnight_polygon_*.log
```

### 3. Safety Features

- **Automatic Timeout**: Stops after 9 hours
- **Periodic Saves**: Data written to database every 15 minutes
- **Rate Limiting**: Worker processes use 4 calls/min (under the 5/min limit)
- **Graceful Shutdown**: Can Ctrl+C and data will be saved

## Key Parameters

- `--mode append`: Only fetch missing/new data (recommended for overnight runs)
- `--mode initial_load`: Fetch all historical data (first time only)
- `--mode full_refresh`: Re-fetch everything (rarely needed)
- `--limit N`: Limit to N tickers (for testing)
- `--lookback-years N`: How many years of history to fetch (default: 5, free tier: 2)

## Rate Limits (Free Tier)

- ⚠️ **5 API calls per minute** (enforced in code)
- ⚠️ **2 years historical data** (not 5 years!)
- ✅ **Unlimited daily calls** (no daily cap)
- ✅ **1-day delayed data** (previous day's close)

## Progress Reporting

Every 50 jobs, you'll see:
```
📈 Progress: 150/2402 (6.2%) | Elapsed: 0.5h | Remaining: 8.5h | Success: 145, Empty: 3, Errors: 2
```

## Database Checkpoints

Every 15 minutes:
```
💾 Loading data to database (periodic checkpoint)...
✅ Database updated successfully
```

## Final Summary

```
⏱️  Total Runtime: 2.15 hours (7740 seconds)
📊 Jobs Processed: 2402/2402
✅ Success: 2350
⚠️  Empty: 45
❌ Errors: 7
📁 Parquet files written to: /path/to/parquet_data
```

## Recommended Overnight Command

```bash
# Generate the prioritized backlog first (if not already done)
.venv/bin/python main.py generate_backlog

# Run overnight with proper lookback
nohup .venv/bin/python main.py gather-stocks-polygon \
  --mode append \
  --lookback-years 2 \
  > logs/overnight_polygon_$(date +%Y%m%d_%H%M).log 2>&1 &

# Save the process ID
echo $! > /tmp/polygon_overnight.pid

# Check it's running
ps -p $(cat /tmp/polygon_overnight.pid)
```

## Stop the Process

```bash
# Graceful stop (saves current data)
kill $(cat /tmp/polygon_overnight.pid)

# Check the final log
tail -100 logs/overnight_polygon_*.log
```

## Troubleshooting

### Process Died?
Check the log for the last message:
```bash
tail -50 logs/overnight_polygon_*.log
```

### Out of API Calls?
Should not happen with rate limiting, but if you see 429 errors repeatedly, the script backs off automatically.

### Database Locked?
The periodic writes use proper connection management. If you see lock errors, ensure no other processes are writing to the database.

### Want to Resume?
Run the same command again - it will skip already-fetched data (if using `--mode append`)

## Expected Runtime

- **~2,400 jobs** (from prioritized backlog with 2-year lookback)
- **5 calls/minute** = 300 calls/hour
- **Estimated time**: ~8 hours for full backlog
- **Maximum time**: 9 hours (hard timeout)

## What Happens After 9 Hours?

The script will:
1. Stop accepting new jobs
2. Finish any in-flight API calls
3. Write all accumulated data to parquet
4. Load all data into the database
5. Print final summary
6. Exit cleanly

## Files Created

- **Parquet files**: `/Users/dougstrouth/datasets_noicloud/edgar/downloads/parquet_data/stock_history/polygon_batch_*.parquet`
- **Database**: Records written to `stock_history` table
- **Log file**: `logs/overnight_polygon_YYYYMMDD_HHMM.log`
