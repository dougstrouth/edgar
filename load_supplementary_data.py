# -*- coding: utf-8 -*-
"""
Supplementary Data Loader from Parquet to DuckDB.

This script handles loading supplementary data (like stock history or yfinance info)
from Parquet files into the main DuckDB database. It's designed to be run
after the corresponding 'gather' script has created the Parquet files.
"""

import sys
import logging
import argparse
from pathlib import Path

# --- Import Utilities ---
from config_utils import AppConfig
from logging_utils import setup_logging
from database_conn import ManagedDatabaseConnection

# Define the schemas for the supplementary tables.
SUPPLEMENTARY_SCHEMAS = {
    "stock_history": """
        CREATE TABLE IF NOT EXISTS stock_history (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            date DATE NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            adj_close DOUBLE, volume BIGINT,
            PRIMARY KEY (ticker, date)
        );""",
    "stock_fetch_errors": """
        CREATE TABLE IF NOT EXISTS stock_fetch_errors (
            cik VARCHAR, ticker VARCHAR COLLATE NOCASE,
            error_timestamp TIMESTAMPTZ NOT NULL,
            error_type VARCHAR NOT NULL, error_message VARCHAR,
            start_date_req DATE, end_date_req DATE
        );""",
    "yf_profile_metrics": """
        CREATE TABLE IF NOT EXISTS yf_profile_metrics (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            fetch_timestamp TIMESTAMPTZ NOT NULL,
            cik VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            country VARCHAR,
            market_cap BIGINT,
            beta DOUBLE,
            trailing_pe DOUBLE,
            forward_pe DOUBLE,
            enterprise_value BIGINT,
            book_value DOUBLE,
            price_to_book DOUBLE,
            trailing_eps DOUBLE,
            forward_eps DOUBLE,
            peg_ratio DOUBLE,
            PRIMARY KEY (ticker)
        );""",
    "yf_recommendations": """
        CREATE TABLE IF NOT EXISTS yf_recommendations (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            recommendation_timestamp TIMESTAMPTZ NOT NULL,
            firm VARCHAR NOT NULL,
            grade_from VARCHAR,
            grade_to VARCHAR,
            action VARCHAR,
            PRIMARY KEY (ticker, recommendation_timestamp, firm)
        );""",
    "yf_major_holders": """
        CREATE TABLE IF NOT EXISTS yf_major_holders (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            fetch_timestamp TIMESTAMPTZ NOT NULL,
            pct_insiders DOUBLE,
            pct_institutions DOUBLE,
            PRIMARY KEY (ticker)
        );""",
    "yf_stock_actions": """
        CREATE TABLE IF NOT EXISTS yf_stock_actions (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            action_date DATE NOT NULL,
            action_type VARCHAR NOT NULL,
            value DOUBLE,
            PRIMARY KEY (ticker, action_date, action_type)
        );""",
    "yf_info_fetch_errors": """
        CREATE TABLE IF NOT EXISTS yf_info_fetch_errors (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            error_timestamp TIMESTAMPTZ NOT NULL,
            error_message VARCHAR
        );""",
    "yf_income_statement": """
        CREATE TABLE IF NOT EXISTS yf_income_statement (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
    "yf_balance_sheet": "CREATE TABLE IF NOT EXISTS yf_balance_sheet (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, item_name VARCHAR NOT NULL, item_value BIGINT, PRIMARY KEY (ticker, report_date, item_name));",
    "yf_cash_flow": "CREATE TABLE IF NOT EXISTS yf_cash_flow (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, item_name VARCHAR NOT NULL, item_value BIGINT, PRIMARY KEY (ticker, report_date, item_name));",
    "yf_untrackable_tickers": """
        CREATE TABLE IF NOT EXISTS yf_untrackable_tickers (
            ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
            reason VARCHAR,
            last_failed_timestamp TIMESTAMPTZ
        );
    """,
    "macro_economic_data": """
        CREATE TABLE IF NOT EXISTS macro_economic_data (
            series_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            value DOUBLE,
            PRIMARY KEY (series_id, date)
        );
    """,
    # Add schemas for yf_info tables here as they are created
}

def load_data(config: AppConfig, logger: logging.Logger, source_name: str, full_refresh: bool = False):
    """
    Loads data from a specific Parquet source into the DuckDB database.

    Args:
        config: The application configuration.
        logger: The logger instance.
        source_name: The name of the data source (e.g., 'stock_history'),
                     which corresponds to the Parquet subdirectory.
        full_refresh: If True, drops the table before loading.
    """
    parquet_path = config.PARQUET_DIR / source_name
    create_sql = SUPPLEMENTARY_SCHEMAS.get(source_name)

    if not create_sql:
        logger.error(f"No schema defined for source '{source_name}'. Aborting.")
        return

    if not parquet_path.exists() or not any(parquet_path.glob('*.parquet')):
        logger.warning(f"Parquet directory for source '{source_name}' not found or is empty. Nothing to load.")
        return

    logger.info(f"--- Starting data load for '{source_name}' ---")
    logger.info(f"Source Parquet Path: {parquet_path}")

    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as con:
            if not con:
                logger.critical("Database connection failed. Aborting load.")
                return

            if full_refresh:
                # --- Blue-Green Deployment for Full Refresh ---
                table_new = f"{source_name}_new"
                logger.info(f"FULL REFRESH: Creating new table '{table_new}' from Parquet...")
                
                # Create the new table directly from Parquet data
                con.execute(f"CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM read_parquet('{str(parquet_path / '*.parquet')}');")
                
                logger.info(f"FULL REFRESH: Atomically swapping '{source_name}' with '{table_new}'...")
                con.begin()
                try:
                    con.execute(f"DROP TABLE IF EXISTS {source_name};")
                    con.execute(f"ALTER TABLE {table_new} RENAME TO {source_name};")
                    con.commit()
                    logger.info(f"FULL REFRESH: Atomic swap for '{source_name}' complete.")
                except Exception as swap_e:
                    logger.error(f"ATOMIC SWAP FAILED for '{source_name}'. Rolling back.", exc_info=True)
                    con.rollback()
                    raise swap_e
            else:
                # --- Append/Update Logic (Original Behavior) ---
                # 1. Create the table if it doesn't exist
                logger.info(f"Ensuring table '{source_name}' exists...")
                con.execute(create_sql)

                # 2. Bulk load data from Parquet files using INSERT OR REPLACE
                logger.info(f"Bulk loading data from Parquet into '{source_name}' (append/update mode)...")
                sql = f"""
                    INSERT OR REPLACE INTO {source_name}
                    SELECT * FROM read_parquet('{str(parquet_path / '*.parquet')}');
                """
                con.execute(sql)
                logger.info(f"Successfully loaded data for '{source_name}'.")

    except Exception as e:
        logger.critical(f"An error occurred during the load process for '{source_name}': {e}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load supplementary data from Parquet to DuckDB.")
    # Allow multiple sources to be specified
    parser.add_argument(
        "source",
        nargs='+', # Accept one or more source names
        choices=SUPPLEMENTARY_SCHEMAS.keys(),
        help="The name of the data source to load (must match a key in SUPPLEMENTARY_SCHEMAS)."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop the target table before loading data, ensuring a clean slate. "
             "Use this after running a gatherer in full_refresh mode."
    )
    args = parser.parse_args()

    # Handle the case where 'all_yf' is passed
    sources_to_load = args.source
    if 'all_yf' in sources_to_load:
        # Replace 'all_yf' with the actual list of yf tables, excluding the stock history ones
        sources_to_load = [s for s in SUPPLEMENTARY_SCHEMAS.keys() if s.startswith('yf_')]

    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    main_logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    for source_name in sources_to_load:
        load_data(app_config, main_logger, source_name, full_refresh=args.full_refresh)

    main_logger.info(f"--- Supplementary Data Loader Finished for source(s): {', '.join(sources_to_load)} ---")