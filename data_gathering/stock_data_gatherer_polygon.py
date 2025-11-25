# -*- coding: utf-8 -*-
"""
Massive.com (formerly Polygon.io) Stock Data Gatherer

Fetches historical stock price data from Massive.com and stores it in Parquet format.
Uses the same architecture as stock_data_gatherer.py but with Massive.com's API.

Note: Polygon.io rebranded as Massive.com on Oct 30, 2025. Existing API keys
continue to work. The API now uses api.massive.com (api.polygon.io still supported).

Free tier limits:
- 5 API calls per minute (no daily limit)
- Previous day's data (1-day delay)
- Unlimited historical data access

Data Adjustment:
- REST API data is fetched with adjusted=true (split/dividend adjusted)
- Flat Files contain unadjusted data and require manual adjustment
- This script uses the REST API, so all data is pre-adjusted for corporate actions
- For splits data, use the /v3/reference/splits endpoint

Uses:
- config_utils.AppConfig for loading configuration from .env
- logging_utils.setup_logging for standardized logging
- database_conn.ManagedDatabaseConnection for DB connection management
- polygon_client.PolygonClient for API access
- prioritizer.prioritize_tickers_hybrid for intelligent ticker selection
"""

import sys
import logging
import argparse
import time
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Set
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd  # type: ignore
import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection
from utils.polygon_client import PolygonClient, PolygonRateLimiter
from utils.prioritizer import prioritize_tickers_for_stock_data
from utils.untrackable import mark_untrackable, get_untrackable_tickers

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# Constants
DEFAULT_MAX_WORKERS = 1  # Free tier safe: 5 req/min global
BATCH_SIZE = 100  # Default number of records before writing to parquet (override via .env POLYGON_BATCH_SIZE)
LOOKBACK_YEARS = 5  # Default historical data period
DEFAULT_MAX_RUNTIME_HOURS = 15  # Default max runtime (increased for rate limiting)
DEFAULT_CALLS_PER_MINUTE = 3  # Align with ticker info gatherer (can raise to 5 safely)


def get_polygon_untrackable_tickers(con: duckdb.DuckDBPyConnection, expiry_days: int = 365) -> Set[str]:
    """Backward-compatible wrapper using shared utility (deprecated name)."""
    return get_untrackable_tickers(con, expiry_days=expiry_days)


# Helper for clamping overly large historical intervals when using a plan table
def _clamp_date_range(start_d: date, end_d: date, clamp_days: int) -> date:
    """Return a potentially adjusted start date ensuring we only fetch at most clamp_days.

    Keeps the end date unchanged. If the original span is already <= clamp_days, returns original start.
    """
    span_days = (end_d - start_d).days
    if span_days <= clamp_days:
        return start_d
    # Shift start forward so span == clamp_days
    return end_d - timedelta(days=clamp_days)


def get_tickers_to_process(
    con: duckdb.DuckDBPyConnection,
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[str]:
    """Get list of tickers to process, optionally prioritized."""
    logger.info("Querying for tickers to process...")
    
    if target_tickers:
        logger.info(f"Using provided list of {len(target_tickers)} target tickers")
        return target_tickers[:limit] if limit else target_tickers
    
    # Get all tickers from database
    query = "SELECT DISTINCT ticker FROM tickers WHERE ticker IS NOT NULL ORDER BY ticker;"
    tickers_df = con.execute(query).df()
    all_tickers = tickers_df['ticker'].tolist()
    
    logger.info(f"Found {len(all_tickers)} unique tickers in database")
    
    if not all_tickers:
        return []
    
    # Return all tickers - prioritization will happen in run_polygon_pipeline
    # The limit will be applied AFTER prioritization
    return all_tickers


def get_latest_stock_dates(
    con: duckdb.DuckDBPyConnection,
    tickers: List[str]
) -> Dict[str, date]:
    """Get the latest date we have stock data for each ticker.
    
    Note: Polygon normalizes tickers (e.g., BH-A becomes BH.A). We need to query
    for both the original ticker and its normalized form.
    """
    logger.info("Querying latest existing stock data dates...")
    latest_dates: Dict[str, date] = {}
    
    try:
        # Check if table exists
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "stock_history" not in tables:
            logger.info("stock_history table doesn't exist yet, will fetch all data")
            return latest_dates
        
        # Polygon normalizes tickers: replaces hyphens with periods (e.g., BH-A -> BH.A)
        # Create a list of both original and normalized tickers to query
        tickers_to_query = set()
        ticker_map = {}  # Maps normalized ticker back to original
        for t in tickers:
            tickers_to_query.add(t)
            ticker_map[t] = t
            # Also add normalized form (hyphen -> period)
            if '-' in t:
                normalized = t.replace('-', '.')
                tickers_to_query.add(normalized)
                ticker_map[normalized] = t  # Map normalized back to original
        
        # Get latest dates for our tickers (both forms)
        tickers_list = list(tickers_to_query)
        placeholders = ','.join(['?'] * len(tickers_list))
        query = f"""
            SELECT ticker, MAX(date) as latest_date
            FROM stock_history
            WHERE ticker IN ({placeholders})
            GROUP BY ticker
        """
        result_df = con.execute(query, tickers_list).df().df()
        
        for _, row in result_df.iterrows():
            ticker_in_table = row['ticker']
            latest = row['latest_date']
            if pd.notna(latest):
                # Map back to original ticker name (in case it was normalized)
                original_ticker = ticker_map.get(ticker_in_table, ticker_in_table)
                
                # Normalize to python date object
                if isinstance(latest, str):
                    latest_dates[original_ticker] = datetime.fromisoformat(latest).date()
                elif isinstance(latest, pd.Timestamp):
                    latest_dates[original_ticker] = latest.date()
                elif isinstance(latest, datetime):
                    latest_dates[original_ticker] = latest.date()
                elif isinstance(latest, date):
                    latest_dates[original_ticker] = latest
                else:
                    # Fallback: try pandas to_datetime then .date()
                    latest_dates[original_ticker] = pd.to_datetime(latest).date()
        
        logger.info(f"Found existing data for {len(latest_dates)} tickers")
        
    except Exception as e:
        logger.warning(f"Could not query latest dates: {e}")
    
    return latest_dates


def get_missing_intervals(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    start_date: date,
    end_date: date
) -> List[Dict[str, date]]:
    """Return a list of (start, end) date ranges where the DB has no data for the ticker.

    This function queries existing dates for the ticker between start_date and end_date
    and returns the complementary intervals that need fetching. If no rows exist,
    returns a single interval covering the full requested range.
    
    Note: Polygon normalizes tickers (e.g., BH-A becomes BH.A). We check both forms.
    """
    try:
        # Query for both original ticker and normalized form (hyphen -> period)
        tickers_to_check = [ticker]
        if '-' in ticker:
            tickers_to_check.append(ticker.replace('-', '.'))
        
        query = "SELECT date FROM stock_history WHERE ticker IN (?, ?) AND date BETWEEN ? AND ? ORDER BY date"
        rows = con.execute(query, [tickers_to_check[0], tickers_to_check[1] if len(tickers_to_check) > 1 else tickers_to_check[0], start_date, end_date]).fetchall()
        existing = [r[0] for r in rows]
    except Exception as e:
        logger.warning(f"Could not query existing dates for {ticker}: {e}")
        return [{'start': start_date, 'end': end_date}]

    # Normalize existing dates to python date objects
    existing_dates = []
    for d in existing:
        if d is None:
            continue
        if isinstance(d, datetime):
            existing_dates.append(d.date())
        elif isinstance(d, date):
            existing_dates.append(d)
        else:
            try:
                existing_dates.append(pd.to_datetime(d).date())
            except Exception:
                continue

    if not existing_dates:
        return [{'start': start_date, 'end': end_date}]

    existing_dates = sorted(set(existing_dates))
    intervals: List[Dict[str, date]] = []

    # Before first existing date
    first = existing_dates[0]
    if start_date < first:
        intervals.append({'start': start_date, 'end': first - timedelta(days=1)})

    # Between existing dates
    for prev, curr in zip(existing_dates[:-1], existing_dates[1:]):
        gap_start = prev + timedelta(days=1)
        gap_end = curr - timedelta(days=1)
        if gap_start <= gap_end:
            intervals.append({'start': gap_start, 'end': gap_end})

    # After last existing date
    last = existing_dates[-1]
    if last < end_date:
        intervals.append({'start': last + timedelta(days=1), 'end': end_date})

    return intervals


def load_parquet_to_db(db_path: str, parquet_dir: Path, logger: logging.Logger) -> None:
    """Load parquet files from parquet_dir into the stock_history table."""
    parquet_files = list(parquet_dir.glob("polygon_batch_*.parquet"))
    
    if not parquet_files:
        logger.info("No parquet files to load")
        return
    
    logger.info(f"Loading {len(parquet_files)} parquet files into database...")
    
    with ManagedDatabaseConnection(db_path_override=db_path, read_only=False) as con:
        if not con:
            raise RuntimeError("Failed to connect to database")
        
        # Check if table exists
        table_exists = False
        try:
            con.execute("SELECT COUNT(*) FROM stock_history LIMIT 1")
            table_exists = True
        except:
            pass
        
        if not table_exists:
            # Create table if not exists
            con.execute("""
                CREATE TABLE stock_history (
                    ticker VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    adj_close DOUBLE,
                    volume BIGINT
                )
            """)
            logger.info("Created stock_history table")
        
        # Load each parquet file
        records_loaded = 0
        for pq_file in parquet_files:
            try:
                # Count records before insert
                before_count = con.execute("SELECT COUNT(*) FROM stock_history").fetchone()[0]
                
                # Load parquet file directly - only insert new records
                con.execute(f"""
                    INSERT INTO stock_history 
                    SELECT * FROM read_parquet('{pq_file}')
                    WHERE (ticker, date) NOT IN (
                        SELECT ticker, date FROM stock_history
                    )
                """)
                
                # Count records after insert
                after_count = con.execute("SELECT COUNT(*) FROM stock_history").fetchone()[0]
                count = after_count - before_count
                records_loaded += count
                logger.debug(f"Loaded {count} new records from {pq_file.name}")
            except Exception as e:
                logger.error(f"Failed to load {pq_file.name}: {e}")
        
        logger.info(f"Loaded {records_loaded} total new records into stock_history table")


def fetch_worker(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to fetch data for a single ticker.
    Runs in separate process.
    """
    ticker = job['ticker']
    start_date = job['start_date']
    end_date = job['end_date']
    api_key = job['api_key']
    db_path = job.get('db_path')  # For marking untrackable tickers
    
    # Create client in this process (can't share across processes)
    # Use configurable rate limit (defaults to conservative 3 calls/min; free tier allows up to 5)
    calls_per_minute = job.get('calls_per_minute', DEFAULT_CALLS_PER_MINUTE)
    rate_limiter = PolygonRateLimiter(calls_per_minute=calls_per_minute)
    client = PolygonClient(api_key, rate_limiter=rate_limiter)  # use default retry profile to match info gatherer
    
    try:
        # Fetch aggregates
        results = client.get_aggregates(ticker, start_date, end_date, timespan='day')
        
        if not results:
            # No data found (legitimate empty response, not an error)
            return {
                'status': 'empty',
                'ticker': ticker,
                'message': 'No data available for date range'
            }
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Rename columns to match our schema
        # Polygon columns: v (volume), vw (vwap), o (open), c (close), h (high), l (low), t (timestamp)
        df = df.rename(columns={
            'v': 'volume',
            'vw': 'vwap',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'timestamp_ms',
            'n': 'transactions'
        })
        
        # Convert timestamp from milliseconds to date
        df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
        
        # Add ticker column
        df['ticker'] = ticker
        
        # Conform to loader schema for 'stock_history'
        # Expected columns: ticker, date, open, high, low, close, adj_close, volume
        df['adj_close'] = df['close']
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
        
        logger.info(f"✅ {ticker}: Fetched {len(df)} days ({df['date'].min()} to {df['date'].max()})")
        
        return {
            'status': 'success',
            'ticker': ticker,
            'data': df
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.warning(f"❌ {ticker}: {error_msg}")
        
        # Check if this is a permanent error (4xx client errors) that should be tracked
        is_permanent_error = (
            '400' in error_msg or 'Bad Request' in error_msg or
            '404' in error_msg or 'Not Found' in error_msg or
            'Client Error' in error_msg
        )
        
        if is_permanent_error and db_path:
            try:
                with ManagedDatabaseConnection(db_path_override=db_path, read_only=False) as conn:
                    if conn:
                        mark_untrackable(conn, ticker, error_msg)
            except Exception as db_e:
                logger.error(f"Failed to mark {ticker} as untrackable: {db_e}")
        
        return {
            'status': 'error',
            'ticker': ticker,
            'error': error_msg,
            'is_permanent': is_permanent_error
        }


def run_polygon_pipeline(
    config: AppConfig,
    mode: str = 'append',
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None,
    lookback_years: int = LOOKBACK_YEARS,
    plan_table: Optional[str] = None
):
    """
    Main pipeline for fetching stock data from Polygon.io.
    
    Args:
        config: AppConfig instance
        mode: 'initial_load', 'append', or 'full_refresh'
        target_tickers: Optional list of specific tickers to fetch
        limit: Optional limit on number of tickers to process
        lookback_years: How many years of history to fetch
    """
    logger.info("=" * 80)
    logger.info("Starting Polygon.io Stock Data Pipeline")
    logger.info("=" * 80)
    
    # Get API key
    api_key = config.get_optional_var("POLYGON_API_KEY")
    if not api_key:
        logger.critical("POLYGON_API_KEY not found in .env file. Cannot continue.")
        logger.critical("Get a free API key from: https://polygon.io/")
        return
    
    # Setup directories
    parquet_dir = config.PARQUET_DIR / "stock_history"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    
    # Get max workers & calls/minute
    max_workers = config.get_optional_int("POLYGON_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    calls_per_minute_cfg = config.get_optional_int("POLYGON_CALLS_PER_MINUTE", DEFAULT_CALLS_PER_MINUTE)
    calls_per_minute = calls_per_minute_cfg if calls_per_minute_cfg is not None else DEFAULT_CALLS_PER_MINUTE
    logger.info(f"Using up to {max_workers} workers at {calls_per_minute} calls/min")
    logger.info(f"Mode: {mode}")
    logger.info(f"Lookback period: {lookback_years} years")
    
    # Connect to database
    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as con:
        if not con:
            logger.critical("Failed to connect to database")
            return
        
        # Auto-detect prioritized backlog table if not explicitly provided
        backlog_table = "prioritized_tickers_stock_backlog"
        if plan_table is None:
            # Check if the backlog table exists
            try:
                table_exists = con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{backlog_table}'"
                ).fetchone()[0] > 0
                
                if table_exists:
                    logger.info(f"✓ Found prioritized backlog table '{backlog_table}'")
                    plan_table = backlog_table
                else:
                    logger.warning(f"⚠️  Backlog table '{backlog_table}' not found.")
                    logger.warning(f"   Run 'python main.py generate_backlog' to create it.")
                    logger.warning(f"   Falling back to on-the-fly prioritization...")
            except Exception as e:
                logger.warning(f"Could not check for backlog table: {e}")
        
        # If a plan table is available, load tickers from it
        if plan_table:
            logger.info(f"Using plan table '{plan_table}' for ticker sourcing.")
            try:
                tickers_df = con.execute(f"SELECT DISTINCT ticker FROM {plan_table} ORDER BY rank").df()
                tickers = tickers_df['ticker'].dropna().tolist()
                if limit:
                    tickers = tickers[:limit]
                logger.info(f"Loaded {len(tickers)} tickers from plan table.")
            except Exception as e:
                logger.critical(f"Failed reading plan table {plan_table}: {e}")
                return
        else:
            # Get tickers to process normally
            tickers = get_tickers_to_process(con, target_tickers, limit)
        
        if not tickers:
            logger.warning("No tickers found to process")
            return
        
        logger.info(f"Processing up to {limit if limit else 'all'} tickers")
        
        # Prioritize only if not using plan table
        if not plan_table:
            try:
                logger.info("Prioritizing tickers using XBRL-aware strategy...")
                prioritized = prioritize_tickers_for_stock_data(
                    db_path=config.DB_FILE_STR,
                    tickers=tickers,
                    lookback_days=365
                )
                if limit:
                    prioritized = prioritized[:limit]
                tickers = [ticker for ticker, score in prioritized]
                logger.info(f"Tickers prioritized. Top {min(10, len(prioritized))}:")
                for i, (ticker, score) in enumerate(prioritized[:10], 1):
                    logger.info(f"  {i:2}. {ticker:8} (score: {score:.4f})")
            except Exception as e:
                logger.warning(f"Prioritization failed: {e}. Using unprioritized list.")
                if limit:
                    tickers = tickers[:limit]
        
        # Filter out untrackable tickers (404s, etc.)
        untrackable_tickers = get_polygon_untrackable_tickers(con, expiry_days=365)
        if untrackable_tickers:
            before_count = len(tickers)
            tickers = [t for t in tickers if t not in untrackable_tickers]
            skipped_count = before_count - len(tickers)
            if skipped_count > 0:
                logger.info(f"Skipping {skipped_count} previously failed (untrackable) tickers")
        
        # Determine date ranges
        if mode == 'initial_load' or mode == 'full_refresh':
            # Fetch full history
            end_date = date.today() - timedelta(days=1)  # Polygon free tier is 1-day delayed
            start_date = end_date - timedelta(days=365 * lookback_years)
            latest_dates = {}
        else:  # append mode
            # Get what we already have
            latest_dates = get_latest_stock_dates(con, tickers)
            end_date = date.today() - timedelta(days=1)
            start_date = end_date - timedelta(days=365 * lookback_years)
    
    # Create jobs
    jobs: List[Dict[str, Any]] = []
    
    if plan_table:
        # Use the plan table (with start_date/end_date columns)
        logger.info(f"Building jobs from plan table '{plan_table}' (includes date ranges).")
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as con_plan:
            if not con_plan:
                logger.critical("Database connection failed when reading plan table for job creation.")
                return
            try:
                # Filter plan table by the limited ticker list
                ticker_list = ','.join(f"'{t}'" for t in tickers)
                rows = con_plan.execute(f"SELECT ticker, start_date, end_date FROM {plan_table} WHERE ticker IN ({ticker_list}) ORDER BY rank").fetchall()
            except Exception as e:
                logger.critical(f"Failed to read plan rows: {e}")
                logger.critical(f"Expected columns: ticker, start_date, end_date, rank")
                logger.critical(f"Hint: Regenerate the backlog with 'python main.py generate_backlog'")
                return

            # Determine clamp window (may override lookback_years if env var provided)
            clamp_years_env = config.get_optional_int("POLYGON_CLAMP_LOOKBACK_YEARS")
            effective_years = clamp_years_env if clamp_years_env is not None else lookback_years
            clamp_days = effective_years * 365
            clamped_intervals = 0

            for ticker, sdt, edt in rows:
                # Normalize types to date
                def to_date(val):
                    if val is None: return None
                    if isinstance(val, date): return val
                    if isinstance(val, datetime): return val.date()
                    try:
                        return datetime.fromisoformat(str(val)).date()
                    except Exception:
                        return None
                
                start_d = to_date(sdt)
                end_d = to_date(edt)
                
                # Validate dates
                if not start_d or not end_d:
                    logger.debug(f"Skipping {ticker} (invalid dates: start={sdt}, end={edt})")
                    continue
                if start_d > end_d:
                    logger.debug(f"Skipping {ticker} (start > end)")
                    continue
                # Clamp overly large ranges to reduce API pressure
                adjusted_start = _clamp_date_range(start_d, end_d, clamp_days)
                if adjusted_start != start_d:
                    clamped_intervals += 1
                    logger.debug(f"Clamped {ticker} range {start_d}→{end_d} to {adjusted_start}→{end_d} ({clamp_days}d window)")

                jobs.append({
                    'ticker': ticker,
                    'start_date': adjusted_start,
                    'end_date': end_d,
                    'api_key': api_key,
                    'db_path': config.DB_FILE_STR,
                    'calls_per_minute': calls_per_minute
                })
        logger.info(f"Created {len(jobs)} jobs from plan table.")
        if clamped_intervals:
            pct = (clamped_intervals / len(jobs)) * 100 if jobs else 0
            logger.info(f"🔧 Clamped {clamped_intervals} intervals ({pct:.1f}% of jobs) to at most {clamp_days} days (≈{effective_years}y)")
    else:
        # No plan table - do intelligent gap analysis per ticker
        skipped_fully_up_to_date = 0
        total_intervals_created = 0
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as con_check:
            if not con_check:
                logger.warning("Could not open DB to check existing dates; creating full-range jobs for all tickers")
                for ticker in tickers:
                    jobs.append({
                        'ticker': ticker,
                        'start_date': start_date,
                        'end_date': end_date,
                        'api_key': api_key,
                        'db_path': config.DB_FILE_STR,
                        'calls_per_minute': calls_per_minute
                    })
                total_intervals_created = len(jobs)
            else:
                for ticker in tickers:
                    if mode == 'append' and ticker in latest_dates:
                        candidate_start = latest_dates[ticker] + timedelta(days=1)
                        if candidate_start > end_date:
                            logger.debug(f"{ticker}: Already up to date")
                            skipped_fully_up_to_date += 1
                            continue
                    else:
                        candidate_start = start_date
                    intervals = get_missing_intervals(con_check, ticker, candidate_start, end_date)
                    if not intervals:
                        logger.debug(f"{ticker}: No missing intervals found; skipping")
                        skipped_fully_up_to_date += 1
                        continue
                    for interval in intervals:
                        jobs.append({
                            'ticker': ticker,
                            'start_date': interval['start'],
                            'end_date': interval['end'],
                            'api_key': api_key,
                            'db_path': config.DB_FILE_STR,
                            'calls_per_minute': calls_per_minute
                        })
                        total_intervals_created += 1
        logger.info(f"Created {total_intervals_created} fetch intervals across {len(tickers)} tickers (skipped {skipped_fully_up_to_date} fully up-to-date tickers)")
    
    logger.info(f"Created {len(jobs)} fetch jobs")
    
    if not jobs:
        logger.info("No jobs to process (all tickers up to date)")
        return
    
    # Start timer
    pipeline_start_time = time.time()
    max_runtime_hours_config = config.get_optional_float("POLYGON_MAX_RUNTIME_HOURS", DEFAULT_MAX_RUNTIME_HOURS)
    max_runtime_hours = max_runtime_hours_config if max_runtime_hours_config is not None else DEFAULT_MAX_RUNTIME_HOURS
    max_runtime_seconds = int(max_runtime_hours * 60 * 60)
    
    logger.info(f"⏰ Pipeline will run for maximum {max_runtime_hours:.1f} hours")
    # Unified batch size override
    batch_size_env = config.get_optional_int("POLYGON_BATCH_SIZE", BATCH_SIZE)
    effective_batch_size = batch_size_env if batch_size_env is not None else BATCH_SIZE
    logger.info(f"📊 Total jobs to process: {len(jobs)}")
    # Calculate realistic throughput estimate
    calls_per_hour = calls_per_minute * 60
    estimated_hours = len(jobs) / calls_per_hour if calls_per_hour else 0
    logger.info(f"⏱️  Estimated time: {estimated_hours:.1f} hours at configured rate ({calls_per_minute} calls/min)")
    
    # Process jobs
    success_count = 0
    error_count = 0
    empty_count = 0
    jobs_processed = 0
    failed_tickers: List[Dict[str, str]] = []  # Track failed tickers for retry
    
    # Collect data for batch writing
    data_batch = []
    
    # Track last write to database
    logger.info(f"Batch size: {effective_batch_size}")
    # Removing periodic DB write interval logic per simplification request (option A)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_worker, job): job for job in jobs}
        
        for future in as_completed(futures):
            # Check timeout
            elapsed_time = time.time() - pipeline_start_time
            if elapsed_time > max_runtime_seconds:
                logger.warning(f"⏰ Reached maximum runtime of {max_runtime_seconds / 3600:.1f} hours")
                logger.warning(f"   Processed {jobs_processed}/{len(jobs)} jobs before timeout")
                logger.warning(f"   Stopping gracefully and writing accumulated data...")
                break
            
            result = future.result()
            jobs_processed += 1
            
            # Progress reporting
            if jobs_processed % 50 == 0 or jobs_processed == len(jobs):
                progress_pct = (jobs_processed / len(jobs)) * 100
                elapsed_hours = elapsed_time / 3600
                remaining_hours = (max_runtime_seconds - elapsed_time) / 3600
                logger.info(f"📈 Progress: {jobs_processed}/{len(jobs)} ({progress_pct:.1f}%) | "
                           f"Elapsed: {elapsed_hours:.1f}h | Remaining: {remaining_hours:.1f}h | "
                           f"Success: {success_count}, Empty: {empty_count}, Errors: {error_count}")
            
            if result['status'] == 'success':
                success_count += 1
                data_batch.append(result['data'])
                
                # Write batch if we've accumulated enough OR if it's time for periodic DB write
                should_write_batch = len(data_batch) >= effective_batch_size

                if should_write_batch:
                    combined_df = pd.concat(data_batch, ignore_index=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = parquet_dir / f"polygon_batch_{timestamp}.parquet"
                    combined_df.to_parquet(filename, index=False, engine='pyarrow')
                    logger.info(f"📦 Wrote batch of {len(combined_df)} records to {filename.name} (batch_size={effective_batch_size})")
                    data_batch = []
                    
            elif result['status'] == 'empty':
                empty_count += 1
            else:
                error_count += 1
                # Track failed ticker for potential retry
                ticker_failed = result.get('ticker', 'unknown')
                error_msg = result.get('error', 'Unknown error')
                if isinstance(ticker_failed, str):
                    failed_tickers.append({
                        'ticker': ticker_failed,
                        'error': str(error_msg)
                    })
    
    # Write remaining data
    if data_batch:
        combined_df = pd.concat(data_batch, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = parquet_dir / f"polygon_batch_{timestamp}.parquet"
        combined_df.to_parquet(filename, index=False, engine='pyarrow')
        logger.info(f"📦 Wrote final batch of {len(combined_df)} records to {filename.name} (batch_size={effective_batch_size})")
    
    # Removed automatic final DB load; user runs explicit loader script separately.
    
    # Calculate runtime
    total_runtime = time.time() - pipeline_start_time
    runtime_hours = total_runtime / 3600
    
    # Save failed tickers to a file for potential retry
    if failed_tickers:
        failed_log = parquet_dir.parent / f"failed_tickers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(failed_log, 'w') as f:
            f.write(f"# Failed tickers from run at {datetime.now()}\n")
            f.write(f"# Total failures: {len(failed_tickers)}\n")
            f.write("#\n")
            for failure in failed_tickers:
                f.write(f"{failure['ticker']}\t{failure['error']}\n")
        logger.info(f"📝 Failed tickers written to: {failed_log}")
    
    logger.info("=" * 80)
    logger.info("Pipeline Complete")
    logger.info(f"⏱️  Total Runtime: {runtime_hours:.2f} hours ({total_runtime:.0f} seconds)")
    logger.info(f"📊 Jobs Processed: {jobs_processed}/{len(jobs)}")
    logger.info(f"✅ Success: {success_count}")
    logger.info(f"⚠️  Empty: {empty_count}")
    logger.info(f"❌ Errors: {error_count}")
    if failed_tickers:
        logger.warning(f"⚠️  {len(failed_tickers)} tickers failed and may need retry")
    logger.info(f"📁 Parquet files written to: {parquet_dir}")
    logger.info("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gather stock data from Polygon.io")
    parser.add_argument(
        "--mode",
        default="append",
        choices=['initial_load', 'append', 'full_refresh'],
        help="The run mode: initial_load (first time), append (update), full_refresh (redo all)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tickers to process (for testing)"
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=LOOKBACK_YEARS,
        help=f"Years of historical data to fetch (default: {LOOKBACK_YEARS})"
    )
    parser.add_argument(
        "--target-tickers-table",
        type=str,
        default=None,
        help="Optional plan table containing ticker,start_date,end_date columns (e.g. stock_fetch_plan)."
    )
    
    args = parser.parse_args()
    
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    
    run_polygon_pipeline(
        config=config,
        mode=args.mode,
        limit=args.limit,
        lookback_years=args.lookback_years,
        plan_table=args.target_tickers_table
    )
