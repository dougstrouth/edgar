# YFinance Data Preservation Strategy

## Critical Context

YFinance API access is **severely rate-limited** and we have **no prioritization strategy** yet for which tickers to fetch. Every API call is precious and we **cannot afford to lose any data** that we manage to gather.

## Current Status

**DISABLED BY DEFAULT** - YFinance gathering scripts will not run unless explicitly enabled.

### Why Disabled?

1. **No Ticker Prioritization**: We have thousands of tickers but no strategy for which ones matter most
2. **Severe Rate Limits**: API calls are heavily throttled (429 errors are common)
3. **Limited API Budget**: Cannot afford to waste calls on low-value tickers
4. **Data Loss Risk**: Without proper preservation, crashes could lose hard-won data

## Data Preservation Architecture

### Multi-Layer Protection

When YFinance gathering is enabled, we use **three layers of data preservation**:

#### Layer 1: Immediate Recovery Files (NEW)
- **Location**: `.cache/recovery_parquet/`
- **Trigger**: IMMEDIATELY after successful API fetch, BEFORE any processing
- **Format**: Individual Parquet files per ticker per fetch
- **Naming**: 
  - Stock history: `stock_history_{ticker}_{timestamp_ms}.parquet`
  - Income stmt: `yf_income_{ticker}_{timestamp_ms}.parquet`
  - Balance sheet: `yf_balance_{ticker}_{timestamp_ms}.parquet`
  - Cash flow: `yf_cashflow_{ticker}_{timestamp_ms}.parquet`
- **Purpose**: Prevent data loss from process crashes, memory errors, or downstream failures
- **Recovery**: Manual inspection and re-ingestion if main pipeline failed

#### Layer 2: Batch Parquet Files
- **Location**: `$PARQUET_DIR/{table_name}/batch_*.parquet`
- **Trigger**: After successful processing by writer process
- **Format**: Batched records (1000 tickers per file for stock history, 250 for info)
- **Purpose**: Standard pipeline intermediate storage
- **Recovery**: Automatic via `update_from_parquet.py` or `edgar_data_loader.py`

#### Layer 3: DuckDB Database
- **Location**: `$DB_FILE`
- **Trigger**: After Parquet load completes
- **Format**: Final normalized tables
- **Tracking**: `parquet_file_log` prevents reprocessing
- **Purpose**: Production queryable data store

## How to Enable (When Ready)

### Prerequisites

Before enabling, you MUST have:

1. **Ticker Prioritization Strategy**
   - Which tickers are high-priority? (e.g., largest market cap, most filings)
   - Maximum number of tickers to process per day
   - Order of priority (process high-value tickers first)

2. **Rate Limit Configuration**
   - Set `YFINANCE_MAX_RETRIES` (default: 5)
   - Set `YFINANCE_BASE_DELAY` (default: 15.0 seconds)
   - Consider `YFINANCE_MAX_WORKERS` (default: 4, reduce if hitting limits)

3. **Recovery Plan**
   - Monitor `.cache/recovery_parquet/` for orphaned files
   - Script to re-ingest recovery files if main pipeline fails

### Enable YFinance Gathering

```bash
# In your .env file or environment:
export YFINANCE_DISABLED=0

# Optional: Configure retry behavior
export YFINANCE_MAX_RETRIES=5
export YFINANCE_BASE_DELAY=15.0
export YFINANCE_MAX_WORKERS=2  # Reduce if hitting rate limits

# Run targeted gather (recommended)
python data_gathering/stock_data_gatherer.py --mode append

# Or via orchestrator
python main.py gather_stocks
```

## Data Recovery Procedures

### Scenario 1: Process Crashed During Fetch

**Symptoms**: 
- Process terminated mid-run
- `.cache/recovery_parquet/` contains files
- Parquet batch files may be incomplete or missing

**Recovery**:
```bash
# 1. Inspect recovery directory
ls -lh .cache/recovery_parquet/

# 2. Check what was already loaded into DB
# (use validate script or query parquet_file_log)

# 3. Create a recovery script to re-ingest orphaned recovery files
# Example (manual):
python -c "
import pandas as pd
from pathlib import Path

recovery_dir = Path('.cache/recovery_parquet')
for f in recovery_dir.glob('stock_history_*.parquet'):
    df = pd.read_parquet(f)
    print(f'{f.name}: {len(df)} rows for ticker {df[\"ticker\"].iloc[0]}')
    # Append to batch parquet or directly to DB
"
```

### Scenario 2: Writer Process Failed

**Symptoms**:
- Recovery files exist
- Batch parquet files are incomplete
- Main DB not updated

**Recovery**:
```bash
# Recovery files are intact - re-run the writer manually
# or trigger incremental load from existing batch parquets

python update_from_parquet.py  # Loads any new batch files
```

### Scenario 3: Rate Limit Exceeded

**Symptoms**:
- Many 429 errors in logs
- Few successful fetches
- Recovery directory has minimal files

**Actions**:
1. **STOP immediately** - Don't waste remaining API budget
2. Review which tickers were attempted (check error logs)
3. Increase `YFINANCE_BASE_DELAY` significantly (e.g., 30-60 seconds)
4. Reduce `YFINANCE_MAX_WORKERS` to 1 or 2
5. Implement ticker prioritization before retrying

## Best Practices

### When Gathering Data

1. **Start Small**: Test with `--mode append` and a few high-priority tickers
2. **Monitor Actively**: Watch logs for 429 errors
3. **Preserve Recovery Files**: Never delete `.cache/recovery_parquet/` until verified in DB
4. **Check DB After Each Run**: Validate data made it to final destination
5. **Document Successes**: Note which tickers were successfully fetched

### Ticker Prioritization Ideas

Consider prioritizing by:
- Market capitalization (largest companies first)
- Filing frequency (most active filers)
- Data completeness (fill gaps first)
- Industry sectors (diversify coverage)
- Recent IPOs (newer companies may have less historical data available)

### Rate Limit Management

- **Conservative Defaults**: Start with high delays, reduce only if successful
- **Exponential Backoff**: Built-in retry logic doubles delay on each 429
- **Worker Limits**: Fewer workers = less parallel stress on API
- **Untrackable Tickers**: 404s are logged to `yf_untrackable_tickers` to skip in future runs

## Recovery File Cleanup

Recovery files should be cleaned up only after verification:

```bash
# After successful run, verify data in DB
python utils/validate_edgar_db.py

# If all data is confirmed in DB, clean recovery files
rm -rf .cache/recovery_parquet/*

# Otherwise, preserve them for manual recovery
```

## Emergency Stop

If a run is clearly failing or wasting API budget:

```bash
# Kill the process immediately
pkill -f stock_data_gatherer
pkill -f stock_info_gatherer

# Preserve recovery files - they contain all successful fetches
# Do NOT delete .cache/recovery_parquet/

# Review what succeeded
ls -lh .cache/recovery_parquet/
```

## Future Enhancements

1. **Automatic Recovery**: Script to auto-ingest orphaned recovery files
2. **Prioritization Engine**: Configurable ticker ranking system
3. **API Budget Tracking**: Monitor daily/hourly API call consumption
4. **Incremental Targets**: Only fetch new data for specific tickers
5. **Progress Checkpointing**: Resume interrupted runs from last successful ticker
