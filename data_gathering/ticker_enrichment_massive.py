#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Massive.com v3 Ticker Enrichment

Fetches and enriches ticker metadata from Massive.com's reference API and upserts
into `massive_tickers` using `INSERT OR REPLACE` on a PRIMARY KEY (`ticker`).

Key behaviors:
 - Writes a timestamped parquet backup for every batch to `PARQUET_DIR/massive_tickers/`
     (e.g. `massive_tickers_batch_<UTC_TIMESTAMP>.parquet`) for audit & replay.
 - Uses a temporary staging table `massive_tickers_batch` to ensure deterministic
     batch application before upsert.
 - Creates the base table if absent with a minimal schema (compatible with enrichment output).

Usage:
        python data_gathering/ticker_enrichment_massive.py [--limit N] [--active-only]
        python main.py enrich-tickers

See `SUPPLEMENTARY_LOADING_METHOD.md` for general loading strategy context.
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime, timezone

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
    limit: Optional[int] = None,
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

    # Parquet backup directory
    parquet_backup_dir = config.PARQUET_DIR / "massive_tickers"
    parquet_backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = parquet_backup_dir / f"massive_tickers_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    try:
        df.to_parquet(backup_file, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"📦 Wrote parquet backup: {backup_file.name}")
    except Exception as e:
        logger.warning(f"Failed to write parquet backup ({backup_file}): {e}")
    
    # Update database
    logger.info("Updating tickers table in database...")
    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as con:
        if not con:
            logger.critical("Failed to connect to database")
            return
        
        # Ensure base table exists (schema is defined in edgar_data_loader; create minimal if missing)
        tables = {r[0].lower() for r in con.execute("SHOW TABLES;").fetchall()}
        if "massive_tickers" not in tables:
            logger.info("Creating massive_tickers table (not found)...")
            con.execute("""
                CREATE TABLE massive_tickers (
                    ticker VARCHAR PRIMARY KEY COLLATE NOCASE,
                    cik VARCHAR(10),
                    name VARCHAR,
                    market VARCHAR,
                    locale VARCHAR,
                    primary_exchange VARCHAR,
                    type VARCHAR,
                    active BOOLEAN,
                    currency_name VARCHAR,
                    currency_symbol VARCHAR,
                    base_currency_name VARCHAR,
                    base_currency_symbol VARCHAR,
                    composite_figi VARCHAR,
                    share_class_figi VARCHAR,
                    last_updated_utc TIMESTAMP,
                    delisted_utc TIMESTAMP,
                    source VARCHAR DEFAULT 'massive.com_v3_api',
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

        # Register DataFrame as temporary view
        con.register('new_tickers', df)
        # Use staging batch table for atomic upsert
        con.execute("DROP TABLE IF EXISTS massive_tickers_batch;")
        con.execute("CREATE TEMP TABLE massive_tickers_batch AS SELECT * FROM new_tickers;")
        con.unregister('new_tickers')

        try:
            con.execute("""
                INSERT OR REPLACE INTO massive_tickers
                SELECT * FROM massive_tickers_batch;
            """)
            rows_affected_row = con.execute("SELECT COUNT(*) FROM massive_tickers").fetchone()
            rows_affected = rows_affected_row[0] if rows_affected_row else 0
            logger.info(f"✅ Successfully upserted batch; table now has {rows_affected} tickers total")
            
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
            logger.error(f"Failed to upsert massive_tickers batch: {e}", exc_info=True)
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
