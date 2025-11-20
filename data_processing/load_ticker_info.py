# -*- coding: utf-8 -*-
"""
Ticker Info Loader (Polygon / Massive)

Supports two loading modes for the `updated_ticker_info` table:

1. Incremental Upsert (default):
     - Reads all parquet batches in the target directory and merges them into
         the existing table with `INSERT OR REPLACE` (PRIMARY KEY on `ticker`).
     - Efficient for frequent small refreshes or partial updates.

2. Full Refresh Blue-Green Swap (`--full-refresh`):
     - Stages all parquet rows in `updated_ticker_info_new` then atomically
         swaps it into place. Use after major schema/source changes or to
         guarantee a clean historical reset.

Data stored includes company metadata, identifiers (CIK, FIGIs), market
classification, capitalization metrics, employee counts, SIC descriptors,
branding and address info, plus timing columns (`fetch_timestamp`, `last_updated_utc`).

See `SUPPLEMENTARY_LOADING_METHOD.md` for broader methodology (snapshot vs
incremental classification and operational safeguards).
"""

import sys
import logging
from pathlib import Path
from typing import Optional, List
import argparse

import duckdb
import pandas as pd  # type: ignore

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

def _read_and_prepare_parquet(parquet_files: List[Path]) -> Optional[pd.DataFrame]:
    """Read multiple parquet files and normalize types. Returns combined DataFrame or None."""
    if not parquet_files:
        return None
    frames = []
    for pf in parquet_files:
        try:
            df = pd.read_parquet(pf)
            if df.empty:
                logger.warning(f"{pf.name}: empty batch skipped")
                continue
            for ts_col in ['last_updated_utc', 'delisted_utc']:
                if ts_col in df.columns:
                    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
            if 'fetch_timestamp' in df.columns and df['fetch_timestamp'].dtype == 'object':
                df['fetch_timestamp'] = pd.to_datetime(df['fetch_timestamp'], errors='coerce', utc=True)
            for str_col in ['homepage_url', 'logo_url', 'icon_url', 'address_1', 'city', 'state', 'postal_code']:
                if str_col in df.columns:
                    df[str_col] = df[str_col].astype(str).replace({'nan': None, 'None': None})
            frames.append(df)
        except Exception as e:
            logger.error(f"Failed reading {pf.name}: {e}")
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return combined

def load_ticker_info_from_parquet(con: duckdb.DuckDBPyConnection, parquet_dir: Path, full_refresh: bool) -> int:
    """Unified loader supporting incremental upsert or full-refresh blue-green swap."""
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {parquet_dir}")
        return 0
    logger.info(f"Found {len(parquet_files)} parquet file(s)")
    df = _read_and_prepare_parquet(parquet_files)
    if df is None or df.empty:
        logger.warning("No usable records after reading parquet batches.")
        return 0
    total_rows = len(df)
    logger.info(f"Prepared {total_rows} rows for loading")
    if full_refresh:
        staging = "updated_ticker_info_new"
        logger.info("FULL REFRESH: Creating staging table and loading data...")
        # Create staging with schema then populate
        con.execute(TICKER_INFO_SCHEMA.replace("updated_ticker_info", staging))
        con.register("temp_df_full", df)
        con.execute(f"INSERT INTO {staging} SELECT * FROM temp_df_full;")
        con.unregister("temp_df_full")
        _row = con.execute(f"SELECT COUNT(*) FROM {staging};").fetchone()
        count_new = _row[0] if _row else 0
        # Existing count for guard
        existing_row = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='updated_ticker_info';").fetchone()
        existing_exists = existing_row is not None and existing_row[0] == 1
        existing_count = 0
        if existing_exists:
            _ex_row = con.execute("SELECT COUNT(*) FROM updated_ticker_info;").fetchone()
            existing_count = _ex_row[0] if _ex_row else 0
        if existing_count > 0 and count_new < existing_count:
            logger.error(
                f"SWAP GUARD: Aborting full refresh; staging row count {count_new} < existing {existing_count}."
            )
            con.execute(f"DROP TABLE IF EXISTS {staging};")
            return 0
        # Additional guard: prevent replacing distinct ticker universe with fewer distinct tickers
        if existing_count > 0:
            existing_distinct_row = con.execute("SELECT COUNT(DISTINCT ticker) FROM updated_ticker_info;").fetchone()
            existing_distinct = existing_distinct_row[0] if existing_distinct_row else 0
            staging_distinct_row = con.execute(f"SELECT COUNT(DISTINCT ticker) FROM {staging};").fetchone()
            staging_distinct = staging_distinct_row[0] if staging_distinct_row else 0
            if staging_distinct < existing_distinct:
                logger.error(
                    f"SWAP GUARD: Aborting full refresh; staging distinct tickers {staging_distinct} < existing {existing_distinct}."
                )
                con.execute(f"DROP TABLE IF EXISTS {staging};")
                return 0
        if count_new == 0 and existing_count > 0:
            logger.error("SWAP GUARD: Aborting full refresh; staging empty while existing has rows.")
            con.execute(f"DROP TABLE IF EXISTS {staging};")
            return 0
        if count_new == 0 and existing_count == 0:
            logger.warning("Staging empty and no existing table; creating empty updated_ticker_info (initial load).")
        logger.info(f"Staging row count: {count_new}. Performing atomic swap...")
        con.begin()
        try:
            con.execute("DROP TABLE IF EXISTS updated_ticker_info;")
            con.execute(f"ALTER TABLE {staging} RENAME TO updated_ticker_info;")
            con.commit()
            logger.info("FULL REFRESH: Swap complete.")
        except Exception as e:
            con.rollback()
            logger.error("Swap failed; rolled back.", exc_info=True)
            raise e
        return count_new
    else:
        # Incremental upsert path
        logger.info("INCREMENTAL: Upserting batch into existing table.")
        con.execute(TICKER_INFO_SCHEMA)
        con.execute("DROP TABLE IF EXISTS temp_ticker_info;")
        con.register("temp_df_inc", df)
        con.execute("CREATE TEMP TABLE temp_ticker_info AS SELECT * FROM temp_df_inc;")
        con.unregister("temp_df_inc")
        # INSERT OR REPLACE handles PK collision updates
        con.execute("""
            INSERT OR REPLACE INTO updated_ticker_info (
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
        con.execute("DROP TABLE IF EXISTS temp_ticker_info;")
        logger.info(f"INCREMENTAL: Upserted {total_rows} rows.")
        return total_rows


def run_ticker_info_loader(config: AppConfig, parquet_subdir: Optional[str] = None, full_refresh: bool = False):
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
        # Load data accordingly
        total_loaded = load_ticker_info_from_parquet(con, parquet_dir, full_refresh=full_refresh)
        
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
    parser = argparse.ArgumentParser(description="Load Polygon/Massive ticker info into DuckDB")
    parser.add_argument("--parquet-subdir", default="updated_ticker_info", help="Override parquet subdirectory name")
    parser.add_argument("--full-refresh", action="store_true", help="Perform blue-green full replacement of table")
    args = parser.parse_args()
    config = AppConfig()
    run_ticker_info_loader(config, parquet_subdir=args.parquet_subdir, full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
