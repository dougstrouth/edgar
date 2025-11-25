# -*- coding: utf-8 -*-
"""
Massive.com (formerly Polygon.io) Ticker Info Gatherer

Fetches comprehensive ticker details from Massive.com v3 Reference API and stores 
in Parquet format. This includes company information, sector, industry, market cap,
currency, active status, and more.

Note: Polygon.io rebranded as Massive.com on Oct 30, 2025. Existing API keys
continue to work.

Free tier limits:
- 5 API calls per minute (no daily limit)
- Access to v3 reference data for all tickers

Important: All stock Flat Files contain unadjusted data. To get adjusted data
(splits, dividends), use the REST API with adjusted=true flag.

Uses:
- config_utils.AppConfig for loading configuration from .env
- logging_utils.setup_logging for standardized logging
- database_conn.ManagedDatabaseConnection for DB connection management
- polygon_client.PolygonClient for API access
"""

import sys
import logging
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Set
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd  # type: ignore
import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_utils import AppConfig  # noqa: E402
from utils.logging_utils import setup_logging  # noqa: E402
from utils.database_conn import ManagedDatabaseConnection  # noqa: E402
from utils.polygon_client import PolygonClient, PolygonRateLimiter  # noqa: E402
from utils.prioritizer import prioritize_tickers_for_stock_data  # noqa: E402

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# Constants
DEFAULT_MAX_WORKERS = 1  # Free tier safe: 5 req/min global
DEFAULT_CALLS_PER_MINUTE = 3  # Conservative default (max 5 for free tier)
BATCH_SIZE = 100  # Number of records before writing to parquet
DEFAULT_INFO_MAX_RUNTIME_HOURS = 4  # Safeguard to avoid runaway
DEFAULT_REFRESH_DAYS = 30  # Re-fetch info if older than this (unless force-refresh)

def filter_tickers_for_info(
    all_tickers: List[str],
    existing_info: Dict[str, datetime],
    refresh_days: int,
    force_refresh: bool,
    current_time: Optional[datetime] = None,
) -> List[str]:
    """Determine which tickers to (re)fetch based on last info age.

    Returns all_tickers if force_refresh, otherwise those missing OR older than refresh_days.
    """
    if force_refresh:
        return all_tickers
    now = current_time or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=refresh_days)
    to_fetch: List[str] = []
    for t in all_tickers:
        ts = existing_info.get(t)
        if ts is None or ts < cutoff:
            to_fetch.append(t)
    return to_fetch


def get_tickers_to_fetch(
    con: duckdb.DuckDBPyConnection,
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[str]:
    """Get list of tickers to fetch info for.
    
    Note: Does NOT apply ORDER BY - ordering should be handled by prioritization.
    If no prioritization requested, tickers will be in natural DB order.
    """
    logger.info("Querying for tickers to fetch info...")
    
    if target_tickers:
        logger.info(f"Using provided list of {len(target_tickers)} target tickers")
        return target_tickers[:limit] if limit else target_tickers
    
    # Get all tickers from database (no ORDER BY - let prioritization handle ordering)
    query = "SELECT DISTINCT ticker FROM tickers WHERE ticker IS NOT NULL;"
    tickers_df = con.execute(query).df()
    all_tickers = tickers_df['ticker'].tolist()
    
    logger.info(f"Found {len(all_tickers)} unique tickers in database")
    
    # Do NOT apply limit here - let it happen after prioritization
    return all_tickers


def get_existing_ticker_info(
    con: duckdb.DuckDBPyConnection,
    tickers: List[str]
) -> Dict[str, datetime]:
    """Get the last fetch timestamp for tickers we already have info for."""
    logger.info("Querying existing ticker info fetch timestamps...")
    existing_info: Dict[str, datetime] = {}
    
    try:
        # Check if table exists
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "updated_ticker_info" not in tables:
            logger.info("updated_ticker_info table doesn't exist yet")
            return existing_info
        
        # Get latest fetch timestamps for our tickers
        placeholders = ','.join(['?'] * len(tickers))
        query = f"""
            SELECT ticker, fetch_timestamp
            FROM updated_ticker_info
            WHERE ticker IN ({placeholders})
        """
        result_df = con.execute(query, tickers).df()
        
        for _, row in result_df.iterrows():
            ticker = row['ticker']
            fetch_ts = row['fetch_timestamp']
            if pd.notna(fetch_ts):
                # Normalize to python datetime object
                if isinstance(fetch_ts, str):
                    existing_info[ticker] = datetime.fromisoformat(fetch_ts)
                elif isinstance(fetch_ts, pd.Timestamp):
                    existing_info[ticker] = fetch_ts.to_pydatetime()
                elif isinstance(fetch_ts, datetime):
                    existing_info[ticker] = fetch_ts
                else:
                    existing_info[ticker] = pd.to_datetime(fetch_ts).to_pydatetime()
        
        logger.info(f"Found existing info for {len(existing_info)} tickers")
        
    except Exception as e:
        logger.warning(f"Could not query existing ticker info: {e}")
    
    return existing_info


def get_polygon_untrackable_tickers(con: duckdb.DuckDBPyConnection, expiry_days: int = 365) -> Set[str]:
    """
    Gets a set of tickers that have been marked as untrackable from Polygon API within the expiry period.
    Tickers marked untrackable longer ago than `expiry_days` will be retried.
    
    Common reasons for untrackable:
    - 404 Not Found (ticker doesn't exist in Polygon's database)
    - Delisted or invalid tickers
    """
    logger.info(f"Querying for Polygon untrackable tickers (expiry: {expiry_days} days) to exclude...")
    untrackable_set: Set[str] = set()
    try:
        # Check if table exists first
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "polygon_untrackable_tickers" in tables:
            query = f"SELECT ticker FROM polygon_untrackable_tickers WHERE last_failed_timestamp >= (now() - INTERVAL '{expiry_days} days');"
            results = con.execute(query).fetchall()
            untrackable_set = {row[0] for row in results}
            logger.info(f"Found {len(untrackable_set)} Polygon untrackable tickers to skip")
        else:
            logger.info("polygon_untrackable_tickers table doesn't exist yet")
    except Exception as e:
        logger.warning(f"Could not query polygon_untrackable_tickers: {e}")
    return untrackable_set


def fetch_worker(job: Dict[str, Any], client: Optional[PolygonClient] = None) -> Dict[str, Any]:
    """
    Worker function to fetch ticker details for a single ticker.
    
    Args:
        job: Job dict with ticker, api_key, db_path, calls_per_minute
        client: Optional pre-created client (for single-worker mode to maintain rate limit state)
    """
    ticker = job['ticker']
    api_key = job['api_key']
    calls_per_minute = job.get('calls_per_minute', DEFAULT_CALLS_PER_MINUTE)
    db_path = job.get('db_path')  # Get DB path to log failures
    
    # Create client only if not provided (multi-worker mode creates per-process)
    # For single-worker mode, reuse the provided client to maintain rate limit state
    if client is None:
        rate_limiter = PolygonRateLimiter(calls_per_minute=calls_per_minute)
        client = PolygonClient(api_key, rate_limiter=rate_limiter)
    
    try:
        # Fetch ticker details from v3 API
        details = client.get_ticker_details(ticker)
        
        if not details:
            error_msg = 'No data returned from Polygon'
            # Mark as untrackable if we got a definitive 404
            # (check if the error was a 404 from the client)
            return {
                'status': 'empty',
                'ticker': ticker,
                'error': error_msg
            }
        
        # Add fetch timestamp
        fetch_timestamp = datetime.now(timezone.utc)
        
        # Extract relevant fields
        ticker_info = {
            'ticker': details.get('ticker'),
            'cik': details.get('cik'),
            'name': details.get('name'),
            'market': details.get('market'),
            'locale': details.get('locale'),
            'primary_exchange': details.get('primary_exchange'),
            'type': details.get('type'),
            'active': details.get('active'),
            'currency_name': details.get('currency_name'),
            'currency_symbol': details.get('currency_symbol'),
            'base_currency_name': details.get('base_currency_name'),
            'base_currency_symbol': details.get('base_currency_symbol'),
            'composite_figi': details.get('composite_figi'),
            'share_class_figi': details.get('share_class_figi'),
            'description': details.get('description'),  # Company description
            'homepage_url': details.get('homepage_url'),
            'total_employees': details.get('total_employees'),
            'list_date': details.get('list_date'),
            'sic_code': details.get('sic_code'),
            'sic_description': details.get('sic_description'),
            'ticker_root': details.get('ticker_root'),
            'source_feed': details.get('source_feed'),
            'market_cap': details.get('market_cap'),
            'weighted_shares_outstanding': details.get('weighted_shares_outstanding'),
            'round_lot': details.get('round_lot'),
            'fetch_timestamp': fetch_timestamp
        }
        
        # Parse timestamps if present
        for ts_field in ['last_updated_utc', 'delisted_utc']:
            ts_val = details.get(ts_field)
            if ts_val:
                try:
                    ticker_info[ts_field] = datetime.fromisoformat(ts_val.replace('Z', '+00:00'))
                except Exception:
                    ticker_info[ts_field] = None
            else:
                ticker_info[ts_field] = None
        
        # Handle address if present
        address = details.get('address', {})
        if address:
            ticker_info['address_1'] = address.get('address1')
            ticker_info['city'] = address.get('city')
            ticker_info['state'] = address.get('state')
            ticker_info['postal_code'] = address.get('postal_code')
        else:
            ticker_info['address_1'] = None
            ticker_info['city'] = None
            ticker_info['state'] = None
            ticker_info['postal_code'] = None
        
        # Handle branding if present
        branding = details.get('branding', {})
        if branding:
            ticker_info['logo_url'] = branding.get('logo_url')
            ticker_info['icon_url'] = branding.get('icon_url')
        else:
            ticker_info['logo_url'] = None
            ticker_info['icon_url'] = None
        
        logger.info(f"✅ {ticker}: Fetched ticker info")
        
        return {
            'status': 'success',
            'ticker': ticker,
            'data': ticker_info
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
            # Mark ticker as untrackable in database to avoid wasting future API calls
            try:
                with ManagedDatabaseConnection(db_path_override=db_path, read_only=False) as conn:
                    if conn:
                        conn.execute("""
                            CREATE TABLE IF NOT EXISTS polygon_untrackable_tickers (
                                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                                reason VARCHAR,
                                last_failed_timestamp TIMESTAMPTZ
                            );
                        """)
                        conn.execute(
                            "INSERT OR REPLACE INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
                            [ticker, error_msg, datetime.now(timezone.utc)]
                        )
                        logger.info(f"🚫 Marked {ticker} as Polygon-untrackable (client error) in database")
            except Exception as db_e:
                logger.error(f"Failed to mark {ticker} as untrackable: {db_e}")
        
        return {
            'status': 'error',
            'ticker': ticker,
            'error': error_msg,
            'is_permanent': is_permanent_error
        }


def run_polygon_ticker_info_pipeline(
    config: AppConfig,
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None,
    force_refresh: bool = False,
    prioritize: bool = False,
    prioritizer_lookback_days: int = 365,
):
    """
    Main pipeline for fetching ticker info from Polygon.io.
    
    Args:
        config: AppConfig instance
        target_tickers: Optional list of specific tickers to fetch
        limit: Optional limit on number of tickers to process
        force_refresh: If True, fetch info even if we already have it
    """
    logger.info("=" * 80)
    logger.info("Starting Polygon.io Ticker Info Pipeline")
    logger.info("=" * 80)
    
    # Get API key
    api_key = config.get_optional_var("POLYGON_API_KEY")
    if not api_key:
        logger.critical("POLYGON_API_KEY not found in .env file. Cannot continue.")
        logger.critical("Get a free API key from: https://polygon.io/")
        return
    
    # Setup directories
    parquet_dir = config.PARQUET_DIR / "updated_ticker_info"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    
    # Get max workers
    max_workers = config.get_optional_int("POLYGON_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    calls_per_minute = config.get_optional_int("POLYGON_CALLS_PER_MINUTE", DEFAULT_CALLS_PER_MINUTE)
    # Ensure we have a valid value (shouldn't be None with default, but be safe)
    calls_per_minute = calls_per_minute if calls_per_minute is not None else DEFAULT_CALLS_PER_MINUTE
    logger.info(f"Using up to {max_workers} workers at {calls_per_minute} calls/min")

    # Runtime & refresh windows from env
    max_runtime_hours_env = config.get_optional_float("POLYGON_INFO_MAX_RUNTIME_HOURS", DEFAULT_INFO_MAX_RUNTIME_HOURS)
    max_runtime_hours = max_runtime_hours_env if max_runtime_hours_env is not None else DEFAULT_INFO_MAX_RUNTIME_HOURS
    refresh_days_env = config.get_optional_int("POLYGON_INFO_REFRESH_DAYS", DEFAULT_REFRESH_DAYS)
    refresh_days = refresh_days_env if refresh_days_env is not None else DEFAULT_REFRESH_DAYS
    logger.info(f"Max runtime: {max_runtime_hours:.1f} hours | Refresh window: {refresh_days} days")
    
    # Get database connection
    with ManagedDatabaseConnection(str(config.DB_FILE)) as con:
        if con is None:
            logger.critical("Failed to connect to database")
            return
        
        # Get tickers to process
        all_tickers = get_tickers_to_fetch(con, target_tickers, limit)
        
        if not all_tickers:
            logger.warning("No tickers to process")
            return
        
        # Optional prioritization BEFORE refresh filtering (so stale tickers maintain ordering)
        if prioritize:
            try:
                logger.info(f"Prioritizing {len(all_tickers)} tickers using stock data heuristic (lookback {prioritizer_lookback_days}d)...")
                prioritized_pairs = prioritize_tickers_for_stock_data(
                    db_path=config.DB_FILE_STR,
                    tickers=all_tickers,
                    lookback_days=prioritizer_lookback_days
                )
                all_tickers = [t for t, _score in prioritized_pairs]
                logger.info("Top 10 prioritized tickers:")
                for i, (t, score) in enumerate(prioritized_pairs[:10], 1):
                    logger.info(f"  {i:2}. {t:10} score={score:.4f}")
            except Exception as e:
                logger.warning(f"Prioritization failed: {e}; proceeding unprioritized")

        # Existing info and refresh logic (after potential prioritization)
        existing_info = get_existing_ticker_info(con, all_tickers)
        tickers_to_fetch = filter_tickers_for_info(all_tickers, existing_info, refresh_days, force_refresh)
        skipped = len(all_tickers) - len(tickers_to_fetch)
        if force_refresh:
            logger.info("Force refresh enabled - fetching all tickers")
        else:
            logger.info(f"Skipping {skipped} tickers with recent info (< {refresh_days}d)")
        
        if not tickers_to_fetch:
            logger.info("All tickers already have info. Use --force-refresh to re-fetch.")
            return
        
        # Apply limit AFTER prioritization & refresh filtering
        if limit:
            tickers_to_fetch = tickers_to_fetch[:limit]
            logger.info(f"Applied limit: processing first {len(tickers_to_fetch)} prioritized tickers")

        # Get untrackable tickers (404s, etc.) to skip
        untrackable_tickers = get_polygon_untrackable_tickers(con, expiry_days=365)
        tickers_to_fetch = [t for t in tickers_to_fetch if t not in untrackable_tickers]
        skipped_untrackable = len(all_tickers) - len(tickers_to_fetch) - skipped
        
        if skipped_untrackable > 0:
            logger.info(f"Skipping {skipped_untrackable} previously failed (untrackable) tickers")
        
        logger.info(f"Will fetch info for {len(tickers_to_fetch)} tickers (prioritize={prioritize})")
    
    # Create jobs with db_path for failure tracking
    jobs = [
        {
            'ticker': ticker,
            'api_key': api_key,
            'db_path': str(config.DB_FILE),
            'calls_per_minute': calls_per_minute
        }
        for ticker in tickers_to_fetch
    ]
    
    # Process jobs
    results = []
    success_count = 0
    error_count = 0
    empty_count = 0
    failed_tickers: List[Dict[str, str]] = []
    data_batch: List[Dict[str, Any]] = []  # accumulate ticker_info dicts

    logger.info(f"Processing {len(jobs)} tickers with {max_workers} workers...")
    pipeline_start = datetime.now(timezone.utc)
    max_runtime_seconds = int(max_runtime_hours * 3600)
    
    def handle_result(result: Dict[str, Any]):
        nonlocal success_count, error_count, empty_count
        results.append(result)
        if result['status'] == 'success':
            success_count += 1
            data_batch.append(result['data'])
        elif result['status'] == 'error':
            error_count += 1
            failed_tickers.append({'ticker': result.get('ticker', 'unknown'), 'error': result.get('error', 'Unknown')})
        else:
            empty_count += 1

        # Batch write condition
        if len(data_batch) >= BATCH_SIZE:
            write_batch(parquet_dir, data_batch)
            data_batch.clear()

        # Runtime check
        elapsed = (datetime.now(timezone.utc) - pipeline_start).total_seconds()
        if elapsed > max_runtime_seconds:
            logger.warning(f"⏰ Reached max runtime {max_runtime_hours:.1f}h, stopping early.")
            return False
        return True

    def write_batch(parquet_dir: Path, batch: List[Dict[str, Any]]):
        if not batch:
            return
        df = pd.DataFrame(batch)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        file = parquet_dir / f"ticker_info_batch_{ts}.parquet"
        df.to_parquet(file, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"📦 Wrote batch of {len(df)} ticker info records to {file.name}")

    if max_workers == 1:
        # Single-worker mode: create ONE shared client to maintain rate limit state
        rate_limiter = PolygonRateLimiter(calls_per_minute=calls_per_minute)
        shared_client = PolygonClient(api_key, rate_limiter=rate_limiter)
        for job in jobs:
            if not handle_result(fetch_worker(job, client=shared_client)):
                break
    else:
        # Multi-worker mode: each process creates its own client (can't share across processes)
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_worker, job): job for job in jobs}
            for future in as_completed(futures):
                if not handle_result(future.result()):
                    break
    
    # Write successful results to parquet
    # Final write of remaining batch
    if data_batch:
        write_batch(parquet_dir, data_batch)
        data_batch.clear()

    # Failed tickers log
    if failed_tickers:
        log_file = parquet_dir / f"failed_ticker_info_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w') as f:
            f.write(f"# Failed ticker info fetches at {datetime.now(timezone.utc).isoformat()}\n")
            for rec in failed_tickers:
                f.write(f"{rec['ticker']}\t{rec['error']}\n")
        logger.info(f"📝 Failed tickers written to {log_file}")
    
    # Summary
    logger.info("=" * 80)
    logger.info("Ticker Info Pipeline Complete")
    logger.info("=" * 80)
    logger.info(f"✅ Success: {success_count}")
    logger.info(f"⚠️  Empty:   {empty_count}")
    logger.info(f"❌ Errors:  {error_count}")
    logger.info(f"📊 Total:   {len(results)}")
    if failed_tickers:
        logger.warning(f"⚠️  {len(failed_tickers)} failures logged for retry")


def main():
    """Entry point for standalone execution."""
    parser = argparse.ArgumentParser(
        description="Fetch ticker info from Massive.com (Polygon.io) v3 API"
    )
    parser.add_argument(
        '--tickers',
        type=str,
        nargs='+',
        help='Specific tickers to fetch (e.g., AAPL MSFT)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of tickers to process'
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Fetch info even for tickers we already have'
    )
    parser.add_argument(
        '--prioritize',
        action='store_true',
        help='Apply stock data prioritization ordering before refresh filtering'
    )
    parser.add_argument(
        '--prioritizer-lookback-days',
        type=int,
        default=365,
        help='Lookback window (days) used by prioritizer heuristic (default: 365)'
    )
    
    args = parser.parse_args()
    
    # Load config
    config = AppConfig()
    
    # Run pipeline
    run_polygon_ticker_info_pipeline(
        config=config,
        target_tickers=args.tickers,
        limit=args.limit,
        force_refresh=args.force_refresh,
        prioritize=args.prioritize,
        prioritizer_lookback_days=args.prioritizer_lookback_days,
    )


if __name__ == "__main__":
    main()
