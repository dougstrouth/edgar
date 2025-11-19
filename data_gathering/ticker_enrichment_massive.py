#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ticker Enrichment from Massive.com v3 API

Enriches the tickers table with additional metadata from Massive.com's v3 reference API.
This adds fields like active status, FIGI identifiers, market classification, etc.

Usage:
    python data_gathering/ticker_enrichment_massive.py [--limit N] [--active-only]
    python main.py enrich-tickers
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from tqdm import tqdm

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection
from utils.polygon_client import PolygonClient

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)


def enrich_tickers_from_massive(
    config: AppConfig,
    limit: int = None,
    active_only: bool = True
):
    """
    Fetch all tickers from Massive.com v3 API and update the tickers table.
    
    Args:
        config: AppConfig instance
        limit: Optional limit on number of tickers to fetch (for testing)
        active_only: Only fetch actively traded tickers (default: True)
    """
    logger.info("=" * 80)
    logger.info("Ticker Enrichment from Massive.com v3 API")
    logger.info("=" * 80)
    
    # Get API key
    api_key = config.get_optional_var("POLYGON_API_KEY")
    if not api_key:
        logger.critical("POLYGON_API_KEY not found in .env file. Cannot continue.")
        logger.critical("Get a free API key from: https://massive.com/")
        return
    
    # Create client
    client = PolygonClient(api_key)
    
    # Fetch all tickers from Massive.com
    logger.info(f"Fetching tickers from Massive.com (active_only={active_only})...")
    all_tickers = client.get_all_tickers(active=active_only, market='stocks', limit=1000)
    
    if not all_tickers:
        logger.error("Failed to fetch tickers from Massive.com")
        return
    
    logger.info(f"Retrieved {len(all_tickers)} tickers from Massive.com")
    
    # Apply limit if specified
    if limit:
        all_tickers = all_tickers[:limit]
        logger.info(f"Limited to {len(all_tickers)} tickers for processing")
    
    # Format for database
    logger.info("Formatting ticker data for database...")
    formatted_tickers = []
    for ticker_data in all_tickers:
        formatted = PolygonClient.format_ticker_for_db(ticker_data)
        formatted_tickers.append(formatted)
    
    # Convert to DataFrame
    df = pd.DataFrame(formatted_tickers)
    logger.info(f"Prepared {len(df)} tickers for database update")
    
    # Update database
    logger.info("Updating tickers table in database...")
    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as con:
        if not con:
            logger.critical("Failed to connect to database")
            return
        
        # Register DataFrame as temporary view
        con.register('new_tickers', df)
        
        # Merge/upsert into massive_tickers table (separate from SEC tickers table)
        # Use INSERT OR REPLACE to update existing tickers or insert new ones
        merge_query = """
            INSERT OR REPLACE INTO massive_tickers
            SELECT * FROM new_tickers
        """
        
        try:
            con.execute(merge_query)
            rows_affected = con.execute("SELECT COUNT(*) FROM massive_tickers").fetchone()[0]
            logger.info(f"✅ Successfully updated massive_tickers table ({rows_affected} total tickers)")
            
            # Show sample of enriched data
            sample = con.execute("""
                SELECT ticker, name, active, market, primary_exchange, type, last_updated_utc
                FROM massive_tickers
                WHERE name IS NOT NULL
                LIMIT 10
            """).fetchall()
            
            logger.info("Sample of enriched ticker data:")
            for row in sample:
                logger.info(f"  {row[0]:8} | {row[1][:30]:30} | Active: {row[2]} | {row[3]} | {row[4]} | {row[5]}")
                
        except Exception as e:
            logger.error(f"Failed to update tickers table: {e}", exc_info=True)
            return
    
    logger.info("=" * 80)
    logger.info("Ticker enrichment complete!")
    logger.info("=" * 80)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich tickers table with Massive.com v3 API data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tickers to process (for testing)"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        default=True,
        help="Only fetch actively traded tickers (default: True)"
    )
    parser.add_argument(
        "--include-delisted",
        action="store_true",
        help="Include delisted tickers"
    )
    
    args = parser.parse_args()
    
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    
    active_only = not args.include_delisted
    
    enrich_tickers_from_massive(
        config=config,
        limit=args.limit,
        active_only=active_only
    )


if __name__ == "__main__":
    main()
