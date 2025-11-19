# -*- coding: utf-8 -*-
"""
Ticker Info Loader

Loads ticker information from Polygon.io (stored in Parquet format) into the DuckDB
database. Uses upsert logic to update existing records or insert new ones.

The updated_ticker_info table stores comprehensive ticker details including:
- Company information (name, description, homepage)
- Market data (type, exchange, active status)
- Financial identifiers (CIK, FIGI codes)
- Sector/industry classification (SIC codes)
- Company metrics (market cap, employees, shares outstanding)
- Address and branding information

Uses:
- config_utils.AppConfig for loading configuration
- logging_utils.setup_logging for standardized logging
- database_conn.ManagedDatabaseConnection for DB connection management
"""

import sys
import logging
from pathlib import Path
from typing import Optional

import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_utils import AppConfig  # noqa: E402
from utils.logging_utils import setup_logging  # noqa: E402
from utils.database_conn import ManagedDatabaseConnection  # noqa: E402

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# Table schema - column order matches parquet file output
TICKER_INFO_SCHEMA = """
    CREATE TABLE IF NOT EXISTS updated_ticker_info (
        ticker VARCHAR NOT NULL COLLATE NOCASE,
        cik VARCHAR,
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
        description VARCHAR,
        homepage_url VARCHAR,
        total_employees BIGINT,
        list_date VARCHAR,
        sic_code VARCHAR,
        sic_description VARCHAR,
        ticker_root VARCHAR,
        source_feed VARCHAR,
        market_cap DOUBLE,
        weighted_shares_outstanding BIGINT,
        round_lot INTEGER,
        fetch_timestamp TIMESTAMPTZ NOT NULL,
        last_updated_utc TIMESTAMPTZ,
        delisted_utc TIMESTAMPTZ,
        address_1 VARCHAR,
        city VARCHAR,
        state VARCHAR,
        postal_code VARCHAR,
        logo_url VARCHAR,
        icon_url VARCHAR,
        PRIMARY KEY (ticker)
    );
"""


def create_table_if_not_exists(con: duckdb.DuckDBPyConnection):
    """Create the updated_ticker_info table if it doesn't exist."""
    logger.info("Creating updated_ticker_info table if not exists...")
    con.execute(TICKER_INFO_SCHEMA)
    logger.info("Table created/verified")


def load_ticker_info_from_parquet(
    con: duckdb.DuckDBPyConnection,
    parquet_dir: Path
) -> int:
    """
    Load ticker info from all Parquet files in the directory.
    Uses upsert logic to update existing records.
    
    Returns:
        Number of records loaded
    """
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {parquet_dir}")
        return 0
    
    logger.info(f"Found {len(parquet_files)} parquet file(s) to load")
    
    total_loaded = 0
    
    for parquet_file in parquet_files:
        logger.info(f"Loading {parquet_file.name}...")
        
        try:
            # Read parquet file into pandas first
            import pandas as pd
            df = pd.read_parquet(parquet_file)
            
            if df.empty:
                logger.warning(f"  {parquet_file.name}: No records found")
                continue
            
            # Explicitly set column types to avoid DuckDB conversion issues
            # Convert timestamp columns to proper datetime if they aren't already
            for ts_col in ['last_updated_utc', 'delisted_utc']:
                if ts_col in df.columns:
                    # These might be None/NaT, handle gracefully
                    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
            
            # fetch_timestamp should already be datetime, but ensure it's timezone-aware
            if 'fetch_timestamp' in df.columns and df['fetch_timestamp'].dtype == 'object':
                df['fetch_timestamp'] = pd.to_datetime(df['fetch_timestamp'], errors='coerce', utc=True)
            
            # Ensure URL and string columns are proper strings (not mixed with timestamps)
            for str_col in ['homepage_url', 'logo_url', 'icon_url', 'address_1', 'city', 'state', 'postal_code']:
                if str_col in df.columns:
                    df[str_col] = df[str_col].astype(str).replace({'nan': None, 'None': None})
            
            # Create temp table from pandas dataframe
            con.execute("DROP TABLE IF EXISTS temp_ticker_info;")
            con.register('temp_df', df)
            con.execute("CREATE TEMP TABLE temp_ticker_info AS SELECT * FROM temp_df;")
            con.unregister('temp_df')
            
            # Upsert: Delete existing records for these tickers, then insert
            logger.info(f"  Upserting {len(df)} records...")
            
            con.execute("""
                DELETE FROM updated_ticker_info
                WHERE ticker IN (SELECT ticker FROM temp_ticker_info);
            """)
            
            # Explicitly specify columns to ensure proper mapping
            con.execute("""
                INSERT INTO updated_ticker_info (
                    ticker, cik, name, market, locale, primary_exchange, type, active,
                    currency_name, currency_symbol, base_currency_name, base_currency_symbol,
                    composite_figi, share_class_figi, description, homepage_url,
                    total_employees, list_date, sic_code, sic_description,
                    ticker_root, source_feed, market_cap, weighted_shares_outstanding, round_lot,
                    fetch_timestamp, last_updated_utc, delisted_utc,
                    address_1, city, state, postal_code, logo_url, icon_url
                )
                SELECT
                    ticker, cik, name, market, locale, primary_exchange, type, active,
                    currency_name, currency_symbol, base_currency_name, base_currency_symbol,
                    composite_figi, share_class_figi, description, homepage_url,
                    total_employees, list_date, sic_code, sic_description,
                    ticker_root, source_feed, market_cap, weighted_shares_outstanding, round_lot,
                    fetch_timestamp, last_updated_utc, delisted_utc,
                    address_1, city, state, postal_code, logo_url, icon_url
                FROM temp_ticker_info;
            """)
            
            # Clean up temp table
            con.execute("DROP TABLE temp_ticker_info;")
            
            total_loaded += len(df)
            logger.info(f"  ✅ Loaded {len(df)} records from {parquet_file.name}")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to load {parquet_file.name}: {e}")
            continue
    
    return total_loaded


def run_ticker_info_loader(config: AppConfig, parquet_subdir: Optional[str] = None):
    """
    Main function to load ticker info from Parquet files into DuckDB.
    
    Args:
        config: AppConfig instance
        parquet_subdir: Optional subdirectory name (default: 'updated_ticker_info')
    """
    logger.info("=" * 80)
    logger.info("Starting Ticker Info Loader")
    logger.info("=" * 80)
    
    # Determine parquet directory
    subdir = parquet_subdir or "updated_ticker_info"
    parquet_dir = config.PARQUET_DIR / subdir
    
    if not parquet_dir.exists():
        logger.warning(f"Parquet directory does not exist: {parquet_dir}")
        logger.info("Run ticker_info_gatherer_polygon.py first to fetch data")
        return
    
    logger.info(f"Parquet directory: {parquet_dir}")
    logger.info(f"Database: {config.DB_FILE}")
    
    # Connect to database
    with ManagedDatabaseConnection(str(config.DB_FILE)) as con:
        if con is None:
            logger.critical("Failed to connect to database")
            return
        
        # Create table if needed
        create_table_if_not_exists(con)
        
        # Load data
        total_loaded = load_ticker_info_from_parquet(con, parquet_dir)
        
        # Get final count
        result = con.execute("SELECT COUNT(*) as cnt FROM updated_ticker_info;").fetchone()
        total_records = result[0] if result else 0
        
        logger.info("=" * 80)
        logger.info("Ticker Info Loader Complete")
        logger.info("=" * 80)
        logger.info(f"Records loaded this run: {total_loaded}")
        logger.info(f"Total records in table: {total_records}")


def main():
    """Entry point for standalone execution."""
    # Load config
    config = AppConfig()
    
    # Run loader
    run_ticker_info_loader(config)


if __name__ == "__main__":
    main()
