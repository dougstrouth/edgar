# -*- coding: utf-8 -*-
"""
Polygon.io Stock Data Gatherer

Fetches historical stock price data from Polygon.io and stores it in Parquet format.
Uses the same architecture as stock_data_gatherer.py but with Polygon.io's API.

Polygon.io free tier:
- 5 API calls per minute (no daily limit)
- Previous day's data (1-day delay)
- Unlimited historical data access

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
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
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

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# Constants
DEFAULT_MAX_WORKERS = 1  # Free tier safe: 5 req/min global
BATCH_SIZE = 100  # Number of records before writing to parquet
LOOKBACK_YEARS = 5  # Default historical data period


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
    """Get the latest date we have stock data for each ticker."""
    logger.info("Querying latest existing stock data dates...")
    latest_dates: Dict[str, date] = {}
    
    try:
        # Check if table exists
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "stock_history" not in tables:
            logger.info("stock_history table doesn't exist yet, will fetch all data")
            return latest_dates
        
        # Get latest dates for our tickers
        placeholders = ','.join(['?'] * len(tickers))
        query = f"""
            SELECT ticker, MAX(date) as latest_date
            FROM stock_history
            WHERE ticker IN ({placeholders})
            GROUP BY ticker
        """
        result_df = con.execute(query, tickers).df()
        
        for _, row in result_df.iterrows():
            ticker = row['ticker']
            latest = row['latest_date']
            if pd.notna(latest):
                # Normalize to python date object
                if isinstance(latest, str):
                    latest_dates[ticker] = datetime.fromisoformat(latest).date()
                elif isinstance(latest, pd.Timestamp):
                    latest_dates[ticker] = latest.date()
                elif isinstance(latest, datetime):
                    latest_dates[ticker] = latest.date()
                elif isinstance(latest, date):
                    latest_dates[ticker] = latest
                else:
                    # Fallback: try pandas to_datetime then .date()
                    latest_dates[ticker] = pd.to_datetime(latest).date()
        
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
    """
    try:
        query = "SELECT date FROM stock_history WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date"
        rows = con.execute(query, [ticker, start_date, end_date]).fetchall()
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


def fetch_worker(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to fetch data for a single ticker.
    Runs in separate process.
    """
    ticker = job['ticker']
    start_date = job['start_date']
    end_date = job['end_date']
    api_key = job['api_key']
    
    # Create client in this process (can't share across processes)
    # Use slower rate limit in workers to be safe
    rate_limiter = PolygonRateLimiter(calls_per_minute=4)  # Slightly under 5 for safety
    client = PolygonClient(api_key, rate_limiter=rate_limiter)
    
    try:
        # Fetch aggregates
        results = client.get_aggregates(ticker, start_date, end_date, timespan='day')
        
        if not results:
            return {
                'status': 'empty',
                'ticker': ticker,
                'error': 'No data returned from Polygon'
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
        return {
            'status': 'error',
            'ticker': ticker,
            'error': error_msg
        }


def run_polygon_pipeline(
    config: AppConfig,
    mode: str = 'append',
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None,
    lookback_years: int = LOOKBACK_YEARS
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
    
    # Get max workers
    max_workers = config.get_optional_int("POLYGON_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    logger.info(f"Using up to {max_workers} workers")
    logger.info(f"Mode: {mode}")
    logger.info(f"Lookback period: {lookback_years} years")
    
    # Connect to database
    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as con:
        if not con:
            logger.critical("Failed to connect to database")
            return
        
        # Get tickers to process
        tickers = get_tickers_to_process(con, target_tickers, limit)
        
        if not tickers:
            logger.warning("No tickers found to process")
            return
        
        logger.info(f"Processing up to {limit if limit else 'all'} tickers")
        
        # Prioritize tickers based on XBRL richness and stock data needs
        try:
            logger.info("Prioritizing tickers using XBRL-aware strategy...")
            prioritized = prioritize_tickers_for_stock_data(
                db_path=config.DB_FILE_STR,
                tickers=tickers,
                lookback_days=365
            )
            # Apply limit AFTER prioritization to get the most important tickers
            if limit:
                prioritized = prioritized[:limit]
            tickers = [ticker for ticker, score in prioritized]
            logger.info(f"Tickers prioritized. Top {min(10, len(prioritized))}:")
            for i, (ticker, score) in enumerate(prioritized[:10], 1):
                logger.info(f"  {i:2}. {ticker:8} (score: {score:.4f})")
        except Exception as e:
            logger.warning(f"Prioritization failed: {e}. Using unprioritized list.")
            # Apply limit even if prioritization failed
            if limit:
                tickers = tickers[:limit]
        
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
    
    # Create jobs for workers, split into missing intervals where appropriate
    jobs = []
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
                    'api_key': api_key
                })
            total_intervals_created = len(jobs)
        else:
            for ticker in tickers:
                # Determine candidate start for the ticker
                if mode == 'append' and ticker in latest_dates:
                    candidate_start = latest_dates[ticker] + timedelta(days=1)
                    if candidate_start > end_date:
                        logger.debug(f"{ticker}: Already up to date")
                        skipped_fully_up_to_date += 1
                        continue
                else:
                    candidate_start = start_date

                # Compute missing intervals within [candidate_start, end_date]
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
                        'api_key': api_key
                    })
                    total_intervals_created += 1

    logger.info(f"Created {total_intervals_created} fetch intervals across {len(tickers)} tickers (skipped {skipped_fully_up_to_date} fully up-to-date tickers)")
    
    logger.info(f"Created {len(jobs)} fetch jobs")
    
    if not jobs:
        logger.info("No jobs to process (all tickers up to date)")
        return
    
    # Process jobs
    success_count = 0
    error_count = 0
    empty_count = 0
    
    # Collect data for batch writing
    data_batch = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_worker, job): job for job in jobs}
        
        for future in as_completed(futures):
            result = future.result()
            
            if result['status'] == 'success':
                success_count += 1
                data_batch.append(result['data'])
                
                # Write batch if we've accumulated enough
                if len(data_batch) >= BATCH_SIZE:
                    combined_df = pd.concat(data_batch, ignore_index=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = parquet_dir / f"polygon_batch_{timestamp}.parquet"
                    combined_df.to_parquet(filename, index=False, engine='pyarrow')
                    logger.info(f"📦 Wrote batch of {len(combined_df)} records to {filename.name}")
                    data_batch = []
                    
            elif result['status'] == 'empty':
                empty_count += 1
            else:
                error_count += 1
    
    # Write remaining data
    if data_batch:
        combined_df = pd.concat(data_batch, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = parquet_dir / f"polygon_batch_{timestamp}.parquet"
        combined_df.to_parquet(filename, index=False, engine='pyarrow')
        logger.info(f"📦 Wrote final batch of {len(combined_df)} records to {filename.name}")
    
    logger.info("=" * 80)
    logger.info("Pipeline Complete")
    logger.info(f"Success: {success_count}")
    logger.info(f"Empty: {empty_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Parquet files written to: {parquet_dir}")
    logger.info("=" * 80)
    logger.info("\nNext steps:")
    logger.info("  1. Load parquet data: python data_processing/load_supplementary_data.py stock_history")
    logger.info("  2. Or use existing loader: python data_processing/load_supplementary_data.py stock_history")


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
        lookback_years=args.lookback_years
    )
