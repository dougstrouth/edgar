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
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
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

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# Constants
DEFAULT_MAX_WORKERS = 1  # Free tier safe: 5 req/min global
BATCH_SIZE = 100  # Number of records before writing to parquet


def get_tickers_to_fetch(
    con: duckdb.DuckDBPyConnection,
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[str]:
    """Get list of tickers to fetch info for."""
    logger.info("Querying for tickers to fetch info...")
    
    if target_tickers:
        logger.info(f"Using provided list of {len(target_tickers)} target tickers")
        return target_tickers[:limit] if limit else target_tickers
    
    # Get all tickers from database
    query = "SELECT DISTINCT ticker FROM tickers WHERE ticker IS NOT NULL ORDER BY ticker;"
    tickers_df = con.execute(query).df()
    all_tickers = tickers_df['ticker'].tolist()
    
    logger.info(f"Found {len(all_tickers)} unique tickers in database")
    
    if limit:
        all_tickers = all_tickers[:limit]
        logger.info(f"Limited to {len(all_tickers)} tickers")
    
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


def fetch_worker(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to fetch ticker details for a single ticker.
    Runs in separate process.
    """
    ticker = job['ticker']
    api_key = job['api_key']
    
    # Create client in this process (can't share across processes)
    # Use slower rate limit in workers to be safe
    rate_limiter = PolygonRateLimiter(calls_per_minute=4)  # Slightly under 5 for safety
    client = PolygonClient(api_key, rate_limiter=rate_limiter)
    
    try:
        # Fetch ticker details from v3 API
        details = client.get_ticker_details(ticker)
        
        if not details:
            return {
                'status': 'empty',
                'ticker': ticker,
                'error': 'No data returned from Polygon'
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
        return {
            'status': 'error',
            'ticker': ticker,
            'error': error_msg
        }


def run_polygon_ticker_info_pipeline(
    config: AppConfig,
    target_tickers: Optional[List[str]] = None,
    limit: Optional[int] = None,
    force_refresh: bool = False
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
    logger.info(f"Using up to {max_workers} workers")
    
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
        
        # Filter out tickers we already have (unless force_refresh)
        if not force_refresh:
            existing_info = get_existing_ticker_info(con, all_tickers)
            tickers_to_fetch = [t for t in all_tickers if t not in existing_info]
            logger.info(f"Skipping {len(existing_info)} tickers with existing info")
        else:
            tickers_to_fetch = all_tickers
            logger.info("Force refresh enabled - fetching all tickers")
        
        if not tickers_to_fetch:
            logger.info("All tickers already have info. Use --force-refresh to re-fetch.")
            return
        
        logger.info(f"Will fetch info for {len(tickers_to_fetch)} tickers")
    
    # Create jobs
    jobs = [{'ticker': ticker, 'api_key': api_key} for ticker in tickers_to_fetch]
    
    # Process jobs
    results = []
    success_count = 0
    error_count = 0
    empty_count = 0
    
    logger.info(f"Processing {len(jobs)} tickers with {max_workers} workers...")
    
    if max_workers == 1:
        # Serial processing
        for job in jobs:
            result = fetch_worker(job)
            results.append(result)
            
            if result['status'] == 'success':
                success_count += 1
            elif result['status'] == 'error':
                error_count += 1
            else:
                empty_count += 1
    else:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_worker, job): job for job in jobs}
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    success_count += 1
                elif result['status'] == 'error':
                    error_count += 1
                else:
                    empty_count += 1
    
    # Write successful results to parquet
    successful_results = [r for r in results if r['status'] == 'success']
    
    if successful_results:
        logger.info(f"Writing {len(successful_results)} successful results to Parquet...")
        
        # Collect all ticker info data
        ticker_data = [r['data'] for r in successful_results]
        df = pd.DataFrame(ticker_data)
        
        # Write to parquet with timestamp in filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = parquet_dir / f"ticker_info_{timestamp}.parquet"
        df.to_parquet(output_file, index=False, engine='pyarrow', compression='snappy')
        
        logger.info(f"Wrote {len(df)} records to {output_file}")
    
    # Summary
    logger.info("=" * 80)
    logger.info("Ticker Info Pipeline Complete")
    logger.info("=" * 80)
    logger.info(f"✅ Success: {success_count}")
    logger.info(f"⚠️  Empty:   {empty_count}")
    logger.info(f"❌ Errors:  {error_count}")
    logger.info(f"📊 Total:   {len(results)}")


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
    
    args = parser.parse_args()
    
    # Load config
    config = AppConfig()
    
    # Run pipeline
    run_polygon_ticker_info_pipeline(
        config=config,
        target_tickers=args.tickers,
        limit=args.limit,
        force_refresh=args.force_refresh
    )


if __name__ == "__main__":
    main()
