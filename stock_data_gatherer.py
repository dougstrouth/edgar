# -*- coding: utf-8 -*-
"""
Stock Data Gatherer Script (Refactored for Utilities)

Connects to EDGAR DuckDB using centralized database_conn utility.
Identifies CIKs and date ranges, maps CIKs to potentially MULTIPLE tickers
using sec-cik-mapper, fetches historical stock data for ALL found tickers
using yfinance, loads into DuckDB, and logs errors.
Supports modes, batch loading, and error logging.

Uses:
- config_utils.AppConfig for loading configuration from .env.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.
"""

import logging # Keep for level constants
import os
import sys
import time
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, List, Set, Tuple

import duckdb
import pandas as pd
import yfinance as yf
from sec_cik_mapper import StockMapper
# from dotenv import load_dotenv # No longer needed here
from tqdm import tqdm

# --- Import Utilities ---
from config_utils import AppConfig                 # Import configuration loader
from logging_utils import setup_logging            # Import logging setup function
from database_conn import ManagedDatabaseConnection # Use context manager

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
STOCK_TABLE_NAME = "stock_history"
ERROR_TABLE_NAME = "stock_fetch_errors"
YFINANCE_DELAY = 0.5
DEFAULT_BATCH_SIZE = 75

# --- Database Functions (Updated to use logger instance) ---
def get_companies_and_xbrl_dates(
    con: duckdb.DuckDBPyConnection,
    logger: logging.Logger, # Pass logger
    target_ciks: Optional[List[str]] = None
) -> pd.DataFrame:
    """Queries the DB for CIKs and their min/max XBRL fact dates."""
    logger.info("Querying database for CIKs and XBRL fact date ranges...")
    base_query = """
    WITH XbrlDates AS (
        SELECT f.cik, MIN(f.period_start_date) AS min_start_date, MAX(f.period_end_date) AS max_end_date
        FROM xbrl_facts f WHERE f.period_start_date IS NOT NULL OR f.period_end_date IS NOT NULL GROUP BY f.cik
    )
    SELECT DISTINCT c.cik, COALESCE(xd.min_start_date, xd.max_end_date) as min_date, COALESCE(xd.max_end_date, xd.min_start_date) as max_date
    FROM companies c JOIN XbrlDates xd ON c.cik = xd.cik
    WHERE (xd.min_start_date IS NOT NULL OR xd.max_end_date IS NOT NULL)
    """
    params = []
    if target_ciks: base_query += f" AND c.cik IN ({','.join(['?'] * len(target_ciks))})"; params.extend(target_ciks)
    base_query += " ORDER BY c.cik;"
    try:
        df = con.execute(base_query, params).fetchdf()
        if not df.empty:
            df['min_date'] = pd.to_datetime(df['min_date'], errors='coerce').dt.date
            df['max_date'] = pd.to_datetime(df['max_date'], errors='coerce').dt.date
            # Drop rows where date conversion failed
            df.dropna(subset=['min_date', 'max_date'], inplace=True)
            logger.info(f"Found {len(df)} CIKs with valid XBRL dates matching criteria.")
            logger.debug(f"Sample CIK Dates:\n{df.head().to_string()}")
        else: logger.warning("No CIKs found with XBRL dates matching criteria.")
        return df
    except Exception as e:
        logger.error(f"Failed query company/XBRL dates: {e}", exc_info=True)
        return pd.DataFrame()

def get_ticker_date_range(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    logger: logging.Logger # Pass logger
) -> Optional[Tuple[str, date, date]]:
     """Gets the CIK and overall min/max XBRL date range for a specific ticker."""
     logger.debug(f"Getting date range for specific ticker: {ticker}")
     query = """
        WITH XbrlDates AS (
            SELECT f.cik, MIN(f.period_start_date) AS min_start_date, MAX(f.period_end_date) AS max_end_date
            FROM xbrl_facts f WHERE f.period_start_date IS NOT NULL OR f.period_end_date IS NOT NULL GROUP BY f.cik
        )
        SELECT t.cik, COALESCE(xd.min_start_date, xd.max_end_date) as min_date, COALESCE(xd.max_end_date, xd.min_start_date) as max_date
        FROM tickers t JOIN XbrlDates xd ON t.cik = xd.cik WHERE t.ticker = ? LIMIT 1;"""
     try:
         result = con.execute(query, [ticker]).fetchone()
         if result:
             cik, min_d, max_d = result
             min_date = pd.to_datetime(min_d, errors='coerce').date() if pd.notna(min_d) else None
             max_date = pd.to_datetime(max_d, errors='coerce').date() if pd.notna(max_d) else None
             if cik and min_date and max_date: return str(cik), min_date, max_date
             else: logger.warning(f"Found ticker {ticker} missing CIK/date (CIK: {cik}, Min: {min_date}, Max: {max_date})"); return None
         else: logger.warning(f"Ticker {ticker} not found or no fact dates."); return None
     except Exception as e:
         logger.error(f"Failed get date range ticker {ticker}: {e}", exc_info=True)
         return None

def get_latest_stock_dates(
    con: duckdb.DuckDBPyConnection,
    logger: logging.Logger, # Pass logger
    target_tickers: Optional[List[str]] = None
) -> Dict[str, date]:
    """Queries the DB for the latest stock date recorded for each ticker."""
    logger.info("Querying latest existing stock data dates...")
    latest_dates = {}; table_exists = False
    try: # Check if table exists before querying
        tables = con.execute("SHOW TABLES;").fetchall()
        if any(STOCK_TABLE_NAME.lower() == t[0].lower() for t in tables): table_exists = True
        else: logger.warning(f"Table '{STOCK_TABLE_NAME}' does not exist. Cannot get latest dates.")
    except Exception as e: logger.error(f"Failed checking existence of {STOCK_TABLE_NAME}: {e}")

    if table_exists:
        base_query = f"SELECT ticker, MAX(date) as max_date FROM {STOCK_TABLE_NAME}"
        params = []
        if target_tickers: base_query += f" WHERE ticker IN ({','.join(['?'] * len(target_tickers))})"; params.extend(target_tickers)
        base_query += " GROUP BY ticker;"
        try:
             results = con.execute(base_query, params).fetchall()
             for ticker, max_date_val in results:
                  if max_date_val:
                    max_date = None
                    if isinstance(max_date_val, datetime): max_date = max_date_val.date()
                    elif isinstance(max_date_val, date): max_date = max_date_val
                    else:
                        try: max_date = pd.to_datetime(max_date_val).date()
                        except: logger.warning(f"Could not convert max_date {max_date_val} for {ticker}")
                    if max_date: latest_dates[ticker] = max_date
             logger.info(f"Found latest dates for {len(latest_dates)} tickers already in DB.")
        except Exception as e: logger.error(f"Failed query latest stock dates: {e}", exc_info=True)
    return latest_dates

def setup_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger): # Pass logger
    """Creates the stock_history and stock_fetch_errors tables if they don't exist."""
    logger.info(f"Setting up tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'")
    create_stock_sql = f"""CREATE TABLE IF NOT EXISTS {STOCK_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, date DATE NOT NULL, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE, volume BIGINT, PRIMARY KEY (ticker, date));"""
    create_error_sql = f"""CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);"""
    try:
        con.execute(create_stock_sql); logger.info(f"Table '{STOCK_TABLE_NAME}' OK.")
        con.execute(create_error_sql); logger.info(f"Table '{ERROR_TABLE_NAME}' OK.")
    except Exception as e: logger.error(f"Failed create tables: {e}", exc_info=True); raise

def drop_stock_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger): # Pass logger
    """Drops the stock_history and stock_fetch_errors tables."""
    logger.warning(f"Dropping tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'...")
    try:
        con.execute(f"DROP TABLE IF EXISTS {STOCK_TABLE_NAME};"); logger.info(f"Table '{STOCK_TABLE_NAME}' dropped.")
        con.execute(f"DROP TABLE IF EXISTS {ERROR_TABLE_NAME};"); logger.info(f"Table '{ERROR_TABLE_NAME}' dropped.")
    except Exception as e: logger.error(f"Failed drop tables: {e}", exc_info=True); raise

def clear_stock_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger): # Pass logger
    """Deletes all data from the stock_history and stock_fetch_errors tables."""
    logger.warning(f"Clearing data from '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'...")
    try:
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if STOCK_TABLE_NAME.lower() in tables: con.execute(f"DELETE FROM {STOCK_TABLE_NAME};"); logger.info(f"Cleared {STOCK_TABLE_NAME}.")
        else: logger.warning(f"Table {STOCK_TABLE_NAME} not found.")
        if ERROR_TABLE_NAME.lower() in tables: con.execute(f"DELETE FROM {ERROR_TABLE_NAME};"); logger.info(f"Cleared {ERROR_TABLE_NAME}.")
        else: logger.warning(f"Table {ERROR_TABLE_NAME} not found.")
    except Exception as e: logger.error(f"Failed clear tables: {e}", exc_info=True); raise

def log_fetch_error(
    con: duckdb.DuckDBPyConnection,
    logger: logging.Logger, # Pass logger
    cik: Optional[str],
    ticker: Optional[str],
    error_type: str,
    message: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Logs a fetch error to the database and the logger."""
    timestamp = datetime.now(timezone.utc)
    # Log to standard logger first
    logger.warning(f"Log err: CIK={cik}, Ticker={ticker}, Type={error_type}, Msg={message}")
    # Log to database
    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None
        # Prepare data tuple, handle potential None values gracefully
        params = [
            cik, ticker, timestamp, error_type, message, start_date_obj, end_date_obj
        ]
        con.execute(f"INSERT INTO {ERROR_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?)", params)
    except duckdb.Error as db_err:
         # Check if error is due to transaction context (already rolled back maybe)
        if "TransactionContext Error" in str(db_err) or "Connection has been closed" in str(db_err):
            logger.error(f"Could not log DB error for Ticker {ticker} (transaction likely aborted): {db_err}")
        else:
            logger.error(f"CRITICAL DB error inserting error log CIK {cik}/Ticker {ticker}: {db_err}", exc_info=True)
    except Exception as e:
        logger.error(f"CRITICAL unexpected error inserting error log CIK {cik}/Ticker {ticker}: {e}", exc_info=True)


def load_stock_data_batch(
    con: duckdb.DuckDBPyConnection,
    logger: logging.Logger, # Pass logger
    stock_dfs: List[pd.DataFrame],
    insert_mode: str = "REPLACE"
) -> bool:
    """Loads a batch of stock data DataFrames into the database."""
    if not stock_dfs: logger.info("No DFs in batch."); return True
    try:
        full_batch_df = pd.concat(stock_dfs, ignore_index=True)
    except Exception as concat_e:
        logger.error(f"Error concat DFs: {concat_e}", exc_info=True)
        # Log error for each ticker represented in the failed batch
        for df in stock_dfs:
            if not df.empty and 'ticker' in df.columns:
                ticker = df['ticker'].iloc[0]
                log_fetch_error(con, logger, None, ticker, 'Load Error', f'Concat fail: {concat_e}')
        return False

    if full_batch_df.empty: logger.warning("Batch DF empty after concat."); return True

    # Ensure date column is in correct format (object suitable for DuckDB date)
    if 'date' in full_batch_df.columns:
        full_batch_df['date'] = pd.to_datetime(full_batch_df['date']).dt.date

    # Convert Pandas NA/NaT to None for DuckDB compatibility
    load_df_final = full_batch_df.astype(object).where(pd.notnull(full_batch_df), None)

    logger.info(f"Loading batch {len(load_df_final)} records ({len(stock_dfs)} tickers) Mode '{insert_mode}'...")
    reg_name = f'stock_batch_df_reg_{int(time.time()*1000)}' # Unique registration name
    try:
        con.register(reg_name, load_df_final)
        # Use INSERT OR REPLACE (for append) or simple INSERT (for full_refresh initial load)
        if insert_mode.upper() == "REPLACE":
            sql = f"INSERT OR REPLACE INTO {STOCK_TABLE_NAME} SELECT * FROM {reg_name}"
        elif insert_mode.upper() == "IGNORE": # Added ignore option
            sql = f"INSERT OR IGNORE INTO {STOCK_TABLE_NAME} SELECT * FROM {reg_name}"
        else: # Default to plain INSERT (used for full_refresh)
             sql = f"INSERT INTO {STOCK_TABLE_NAME} SELECT * FROM {reg_name}"

        con.execute(sql)
        con.unregister(reg_name)
        logger.info(f"Batch load successful.")
        return True
    except Exception as e:
        logger.error(f"DB err batch insert: {e}", exc_info=True)
        try: con.unregister(reg_name) # Attempt to unregister even on failure
        except: pass
        # Log error for each ticker in the failed batch
        for ticker in load_df_final['ticker'].unique():
            log_fetch_error(con, logger, None, ticker, 'Load Error', f'Batch insert failed: {e}')
        return False

# --- Stock Data Fetching (Updated to use logger) ---
def fetch_stock_history(
    ticker: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger # Pass logger
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Fetches stock history using yfinance for a given ticker and date range."""
    logger.debug(f"Fetching {ticker} from {start_date} to {end_date}")
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

        if start_dt > end_dt:
            logger.warning(f"Start date > End date: {start_date} > {end_date} for {ticker}. Skipping fetch.")
            return None, "Start date after end date"

        # Fetch data including the end_date by adding one day to the end param
        stock = yf.Ticker(ticker)
        history = stock.history(start=start_date, end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'), interval="1d")

        if history.empty:
            logger.warning(f"No data returned from yfinance for {ticker} in range {start_date} to {end_date}.")
            return None, "No data from yfinance"

        # Prepare DataFrame
        history.reset_index(inplace=True)
        history['ticker'] = ticker

        # Rename columns
        history.rename(columns={
            'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
        }, inplace=True)

        # Convert Date column to date objects, coercing errors
        history['date'] = pd.to_datetime(history['date'], errors='coerce').dt.date
        history = history[history['date'].notna()] # Remove rows where date conversion failed

        if history.empty:
             logger.warning(f"Date parsing removed all rows for {ticker} ({start_date} to {end_date}).")
             return None, "Date parsing removed all rows"

        # Ensure all required DB columns exist
        db_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        for col in db_columns:
            if col not in history.columns: history[col] = pd.NA # Use Pandas NA

        # Convert volume to Int64 (nullable integer)
        history['volume'] = pd.to_numeric(history['volume'], errors='coerce').astype('Int64')

        # Filter final data to the exact requested date range
        history = history[(history['date'] >= start_dt) & (history['date'] <= end_dt)]

        if history.empty:
            logger.warning(f"Filtering removed all rows for {ticker}. Data might be outside requested range {start_date}-{end_date}.")
            return None, "Data outside requested range after filtering"

        return history[db_columns], None # Return only necessary columns

    except Exception as e:
        error_msg = f"yfinance fetch failed {ticker}: {type(e).__name__}-{e}"
        logger.error(error_msg, exc_info=False) # Log full traceback usually not needed for yfinance errors
        return None, error_msg


# --- Main Pipeline Function (Refactored to use logger) ---
def run_stock_data_pipeline(
    logger: logging.Logger, # Pass logger
    config: AppConfig,      # Pass config
    mode: str = 'append',   # Changed default to 'append'
    batch_size: int = DEFAULT_BATCH_SIZE,
    append_start_date: Optional[str] = None,
    target_tickers: Optional[List[str]] = None,
    db_path_override: Optional[str] = None
):
    """Main function using ManagedDatabaseConnection and utilities."""
    start_run_time = time.time()
    db_log_path = db_path_override if db_path_override else config.DB_FILE_STR # Use config default
    logger.info(f"--- Starting Stock Data Pipeline ---")
    logger.info(f"Database Target: {db_log_path}")
    logger.info(f"Run Mode: {mode}")
    if target_tickers: logger.info(f"Target Tickers: {target_tickers}")
    if mode == 'append' and append_start_date: logger.info(f"Append Start Date Override: {append_start_date}")
    logger.info(f"Batch Size (Tickers): {batch_size}")

    valid_modes = ['initial_load', 'append', 'full_refresh', 'drop_tables']
    if mode not in valid_modes: logger.error(f"Invalid mode '{mode}'. Exiting."); return

    # Stats counters
    processed_tickers_count = 0; skipped_tickers_count = 0
    fetch_errors_count = 0; load_errors_count = 0; total_tickers_to_process = 0

    try:
        # Use context manager with override or config default path
        with ManagedDatabaseConnection(db_path_override=db_log_path, read_only=False) as write_conn:
            if not write_conn:
                logger.critical("Exiting due to database connection failure.")
                return # Cannot proceed without connection

            # --- Mode Handling & Setup ---
            try:
                write_conn.begin() # Start transaction for setup/clear
                if mode == 'drop_tables': drop_stock_tables(write_conn, logger); write_conn.commit(); logger.info("Drop finished."); return
                setup_tables(write_conn, logger) # Pass logger
                if mode == 'full_refresh': clear_stock_tables(write_conn, logger); logger.info("Full refresh clear done.") # Pass logger
                write_conn.commit() # Commit setup/clear transaction
            except Exception as setup_e:
                logger.error(f"Failed setup/clear: {setup_e}. Exiting.", exc_info=True)
                try: write_conn.rollback()
                except: pass
                return # Exit if setup fails

            # --- Initialize CIK Mapper (if needed) ---
            mapper = None
            target_tickers_upper = [t.upper() for t in target_tickers] if target_tickers else None
            if not target_tickers_upper:
                 try: mapper = StockMapper(); logger.info("StockMapper initialized.")
                 except Exception as e: logger.error(f"Could not init StockMapper: {e}. Cannot map CIKs.", exc_info=True); return

            # --- Identify Tickers and CIK/Date Ranges ---
            tickers_to_process_map: Dict[str, Dict] = {}
            try:
                if target_tickers_upper:
                    logger.info(f"Getting date ranges for {len(target_tickers_upper)} target tickers...")
                    for ticker in tqdm(target_tickers_upper, desc="Getting Ticker Dates"):
                        # Pass logger
                        range_info = get_ticker_date_range(write_conn, ticker, logger)
                        if range_info:
                            cik, min_d, max_d = range_info
                            tickers_to_process_map[ticker] = {'cik': cik, 'min_date': min_d, 'max_date': max_d}
                        else:
                            # Log error inside try-except block
                            try:
                                write_conn.begin()
                                log_fetch_error(write_conn, logger, None, ticker, 'Missing Info', 'Could not find CIK/date range')
                                write_conn.commit()
                            except Exception as log_e:
                                logger.error(f"Failed to log Missing Info error for {ticker}: {log_e}")
                                try: write_conn.rollback()
                                except: pass
                elif mapper: # Process all CIKs
                    # Pass logger
                    companies_data_df = get_companies_and_xbrl_dates(write_conn, logger)
                    if not companies_data_df.empty:
                        logger.info(f"Mapping CIKs to tickers for {len(companies_data_df)} companies...")
                        for _, row in tqdm(companies_data_df.iterrows(), total=len(companies_data_df), desc="Mapping CIKs"):
                            cik, min_d, max_d = row['cik'], row['min_date'], row['max_date']
                            if pd.isna(min_d) or pd.isna(max_d): continue
                            try:
                                tickers_set: Set[str] = mapper.cik_to_tickers.get(cik, set())
                                if not tickers_set:
                                     try:
                                         write_conn.begin(); log_fetch_error(write_conn, logger, cik, None, 'No Ticker Found', 'Mapper no tickers.'); write_conn.commit()
                                     except Exception as log_e: logger.error(f"Failed log No Ticker err {cik}: {log_e}"); write_conn.rollback()
                                     continue
                                for ticker in tickers_set:
                                     if ticker not in tickers_to_process_map: tickers_to_process_map[ticker] = {'cik': cik, 'min_date': min_d, 'max_date': max_d}
                                     else: logger.debug(f"Ticker {ticker} already mapped from CIK {tickers_to_process_map[ticker]['cik']}. Ignore CIK {cik}.")
                            except Exception as map_e:
                                 try:
                                     write_conn.begin(); log_fetch_error(write_conn, logger, cik, None, 'Mapper Error', f"Lookup failed: {map_e}"); write_conn.commit()
                                 except Exception as log_e: logger.error(f"Failed log Mapper err {cik}: {log_e}"); write_conn.rollback()
                    else: logger.warning("Company/XBRL date query returned no results.")
            except Exception as e: logger.error(f"Error during ticker identification: {e}", exc_info=True); return

            if not tickers_to_process_map: logger.warning("No tickers identified for processing."); return

            # --- Get Latest Dates for Append Mode ---
            latest_dates = {}
            if mode == 'append':
                # Pass logger
                latest_dates = get_latest_stock_dates(write_conn, logger, target_tickers=list(tickers_to_process_map.keys()))

            # --- Iterate, Fetch, Batch Load ---
            total_tickers_to_process = len(tickers_to_process_map)
            batch_data_dfs: List[pd.DataFrame] = []
            processed_tickers_in_batch = 0
            logger.info(f"Starting fetch/load loop for {total_tickers_to_process} unique tickers...")
            ticker_items = list(tickers_to_process_map.items())

            for i, (ticker, info) in enumerate(tqdm(ticker_items, desc="Fetching Stock Data", unit="ticker")):
                cik, min_date_overall, max_date_overall = info['cik'], info['min_date'], info['max_date']
                fetch_start_date = None; fetch_end_date = datetime.now(timezone.utc).date() # Default end date is today
                logger.debug(f"Processing {i+1}/{total_tickers_to_process}: {ticker} (CIK {cik})")

                # Determine fetch range
                if mode == 'append':
                    last_date = latest_dates.get(ticker); start_date_override = None
                    if append_start_date:
                        try: start_date_override = datetime.strptime(append_start_date, '%Y-%m-%d').date()
                        except ValueError: logger.error(f"Invalid append_start_date: {append_start_date}. Ignored.")
                    if start_date_override: fetch_start_date = start_date_override
                    elif last_date: fetch_start_date = last_date + timedelta(days=1)
                    else: fetch_start_date = min_date_overall # Fallback for new ticker in append mode
                    # Keep fetch_end_date as today for append mode
                else: # initial_load or full_refresh
                    fetch_start_date = min_date_overall
                    fetch_end_date = max_date_overall # Use max date from XBRL facts

                # Validate dates before fetching
                if not fetch_start_date or not fetch_end_date:
                     try: write_conn.begin(); log_fetch_error(write_conn, logger, cik, ticker, 'Date Error', 'Invalid fetch dates generated.'); write_conn.commit()
                     except Exception as log_e: logger.error(f"Failed log Date err {ticker}: {log_e}"); write_conn.rollback()
                     skipped_tickers_count += 1; continue
                if fetch_start_date > fetch_end_date:
                    try: write_conn.begin(); log_fetch_error(write_conn, logger, cik, ticker, 'Date Error', f'Start>End: {fetch_start_date} > {fetch_end_date}.', fetch_start_date.strftime('%Y-%m-%d'), fetch_end_date.strftime('%Y-%m-%d')); write_conn.commit()
                    except Exception as log_e: logger.error(f"Failed log Start>End err {ticker}: {log_e}"); write_conn.rollback()
                    skipped_tickers_count += 1; continue

                fetch_start_date_str = fetch_start_date.strftime('%Y-%m-%d')
                fetch_end_date_str = fetch_end_date.strftime('%Y-%m-%d')

                # Fetch Data (pass logger)
                stock_history_df, fetch_error_msg = fetch_stock_history(ticker, fetch_start_date_str, fetch_end_date_str, logger)

                if stock_history_df is None:
                    fetch_errors_count += 1; skipped_tickers_count += 1
                    try: write_conn.begin(); log_fetch_error(write_conn, logger, cik, ticker, 'Fetch Error', fetch_error_msg or "Unknown fetch error", fetch_start_date_str, fetch_end_date_str); write_conn.commit()
                    except Exception as log_e: logger.error(f"Failed log Fetch err {ticker}: {log_e}"); write_conn.rollback()
                elif not stock_history_df.empty:
                    batch_data_dfs.append(stock_history_df); processed_tickers_in_batch += 1
                else: # Empty DataFrame returned, but no error message -> means no *new* data in range
                    logger.info(f"No new data found for {ticker} from {fetch_start_date_str} to {fetch_end_date_str}.")
                    # Don't count as skipped, just nothing to add

                # Load Batch Periodically
                is_last_item = (i + 1 == total_tickers_to_process)
                if batch_data_dfs and (processed_tickers_in_batch >= batch_size or is_last_item):
                    logger.info(f"Loading batch ({processed_tickers_in_batch} tickers)...")
                    # Use REPLACE for append/initial, INSERT for full_refresh
                    insert_mode = "REPLACE" if mode != 'full_refresh' else "INSERT"
                    load_successful = False
                    try:
                        write_conn.begin() # Transaction for batch load
                        # Pass logger to batch load function
                        load_successful = load_stock_data_batch(write_conn, logger, batch_data_dfs, insert_mode=insert_mode)
                        if load_successful:
                            write_conn.commit()
                            processed_tickers_count += processed_tickers_in_batch # Increment successful count
                            logger.info(f"Batch commit successful for {processed_tickers_in_batch} tickers.")
                        else:
                            # Errors logged within load_stock_data_batch
                            write_conn.rollback()
                            load_errors_count += processed_tickers_in_batch # Count tickers in failed batch
                            skipped_tickers_count += processed_tickers_in_batch # Also count as skipped overall
                            logger.warning(f"Batch rolled back for {processed_tickers_in_batch} tickers.")
                    except Exception as batch_load_e:
                        logger.error(f"Unexpected batch load transaction error: {batch_load_e}", exc_info=True)
                        load_errors_count += processed_tickers_in_batch # Count tickers
                        skipped_tickers_count += processed_tickers_in_batch # Count as skipped
                        try: write_conn.rollback()
                        except Exception as rb_e: logger.error(f"Rollback attempt failed after batch load error: {rb_e}")
                        # Log a generic batch error if possible
                        first_ticker = batch_data_dfs[0]['ticker'].iloc[0] if batch_data_dfs and not batch_data_dfs[0].empty else "UNKNOWN"
                        try: log_fetch_error(write_conn, logger, None, f"Batch ~{first_ticker}", 'Load Error', f"Unexpected TX err: {batch_load_e}")
                        except: pass # Avoid error loops if logging fails
                    finally:
                        batch_data_dfs = [] # Reset batch regardless of outcome
                        processed_tickers_in_batch = 0

                if YFINANCE_DELAY > 0 and not is_last_item: time.sleep(YFINANCE_DELAY)

            # --- End of Loop ---
            logger.info("Finished processing all identified tickers.")

        # Context manager handles final commit/rollback/close for the main connection

    except Exception as pipeline_e:
         logger.error(f"Pipeline error outside main loop: {pipeline_e}", exc_info=True)

    end_run_time = time.time()
    logger.info(f"--- Stock Data Pipeline Finished ---")
    logger.info(f"Total Time: {end_run_time - start_run_time:.2f} sec")
    logger.info(f"Tickers Targeted: {total_tickers_to_process}")
    logger.info(f"Tickers Loaded OK: {processed_tickers_count}")
    logger.info(f"Tickers Skipped/Errored: {skipped_tickers_count}")
    logger.info(f"Fetch Errors Logged: {fetch_errors_count}")
    logger.info(f"Load Errors (Batches Failed): {load_errors_count}") # Corrected description
    logger.info(f"Check '{ERROR_TABLE_NAME}' in DB for details.")


# --- Script Execution Control ---
if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error loading config: {e}", exc_info=True)
        sys.exit(1)

    # Example: Append using DB_FILE from config
    run_stock_data_pipeline(
        logger=logger, # Pass the initialized logger
        config=config, # Pass the initialized config
        mode='append', # Default mode
        batch_size=config.get_optional_int("STOCK_GATHERER_BATCH_SIZE", DEFAULT_BATCH_SIZE), # Example optional config
        # append_start_date=config.get_optional_var("STOCK_GATHERER_APPEND_START"), # Example optional config
        # target_tickers=None, # Or load from config/args
        db_path_override=None # Use DB from config by default
    )

    # Example: Initial load to memory db for specific tickers
    # run_stock_data_pipeline(
    #     logger=logger,
    #     config=config, # Still pass config even if DB is overridden
    #     mode='initial_load',
    #     target_tickers=['AAPL', 'MSFT'],
    #     db_path_override=':memory:'
    # )

    logger.info("Script execution finished.")