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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, List, Set, Tuple

import duckdb
import pandas as pd
import yfinance as yf
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
DEFAULT_BATCH_SIZE = 75
DEFAULT_REQUEST_DELAY = 0.05 # Default 50ms delay
DEFAULT_MAX_WORKERS = 10 # Default number of concurrent workers

# --- Database Functions (Updated to use logger instance) ---
def get_tickers_to_process(
    con: duckdb.DuckDBPyConnection,
    target_tickers: Optional[List[str]] = None
) -> List[str]:
    """Gets a list of unique tickers to process from the 'tickers' table."""
    logger.info("Querying for unique tickers to process...")
    query = "SELECT DISTINCT ticker FROM tickers"
    params = []
    if target_tickers:
        query += f" WHERE ticker IN ({','.join(['?'] * len(target_tickers))})"
        params.extend(target_tickers)
    query += " ORDER BY ticker;"
    try:
        tickers_df = con.execute(query, params).df()
        tickers = tickers_df['ticker'].tolist()
        logger.info(f"Found {len(tickers)} unique tickers to process.")
        return tickers
    except Exception as e:
        logger.error(f"Failed to query tickers: {e}", exc_info=True)
        return []

def get_latest_stock_dates(
    con: duckdb.DuckDBPyConnection,
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

def setup_tables(con: duckdb.DuckDBPyConnection):
    """Creates the stock_history and stock_fetch_errors tables if they don't exist."""
    logger.info(f"Setting up tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'")
    create_stock_sql = f"""CREATE TABLE IF NOT EXISTS {STOCK_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, date DATE NOT NULL, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE, volume BIGINT, PRIMARY KEY (ticker, date));"""
    create_error_sql = f"""CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);"""
    try:
        con.execute(create_stock_sql); logger.info(f"Table '{STOCK_TABLE_NAME}' OK.")
        con.execute(create_error_sql); logger.info(f"Table '{ERROR_TABLE_NAME}' OK.")
    except Exception as e: logger.error(f"Failed create tables: {e}", exc_info=True); raise

def drop_stock_tables(con: duckdb.DuckDBPyConnection):
    """Drops the stock_history and stock_fetch_errors tables."""
    logger.warning(f"Dropping tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'...")
    try:
        con.execute(f"DROP TABLE IF EXISTS {STOCK_TABLE_NAME};"); logger.info(f"Table '{STOCK_TABLE_NAME}' dropped.")
        con.execute(f"DROP TABLE IF EXISTS {ERROR_TABLE_NAME};"); logger.info(f"Table '{ERROR_TABLE_NAME}' dropped.")
    except Exception as e: logger.error(f"Failed drop tables: {e}", exc_info=True); raise

def clear_stock_tables(con: duckdb.DuckDBPyConnection):
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
    stock_dfs: List[pd.DataFrame],
    insert_mode: str = "REPLACE"
) -> bool:
    """Loads a batch of stock data DataFrames into the database."""
    if not stock_dfs: logger.info("No dataframes in batch to load."); return True
    try:
        full_batch_df = pd.concat(stock_dfs, ignore_index=True)
    except Exception as concat_e:
        logger.error(f"Error concat DFs: {concat_e}", exc_info=True)
        # Log error for each ticker represented in the failed batch
        # This is less precise in the new model, but we can log a general error.
        log_fetch_error(con, None, "BATCH_CONCAT_FAIL", 'Load Error', f'Concat fail: {concat_e}')
        return False

    if full_batch_df.empty: logger.warning("Batch DF empty after concat."); return True

    # Ensure date column is in correct format (object suitable for DuckDB date)
    if 'date' in full_batch_df.columns:
        full_batch_df['date'] = pd.to_datetime(full_batch_df['date']).dt.date

    # Convert Pandas NA/NaT to None for DuckDB compatibility
    load_df_final = full_batch_df.astype(object).where(pd.notnull(full_batch_df), None)

    logger.info(f"Loading batch of {len(load_df_final)} records ({len(stock_dfs)} tickers) Mode '{insert_mode}'...")
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
        log_fetch_error(con, None, "BATCH_INSERT_FAIL", 'Load Error', f'Batch insert failed: {e}')
        return False

# --- Stock Data Fetching (Updated to use logger) ---
def fetch_stock_history(
    ticker: str,
    start_date: str,
    end_date: str
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
        e_str = str(e).lower()
        if "404" in e_str or "no data found" in e_str:
            error_msg = f"Ticker {ticker} not found on yfinance (404)."
            logger.debug(error_msg) # This is common, no need to spam logs
        elif "429" in e_str or "too many requests" in e_str:
            error_msg = f"Rate limit hit for {ticker} (429). Consider increasing YFINANCE_REQUEST_DELAY."
            logger.warning(error_msg)
        else:
            error_msg = f"yfinance fetch failed for {ticker}: {type(e).__name__} - {e}"
            logger.warning(error_msg) # Use warning for data issues, not script errors

        return None, error_msg

def fetch_worker(job: Dict[str, Any], delay: float) -> Dict[str, Any]:
    """
    Worker function to be run in a thread. Fetches stock history for a single ticker.
    """
    if delay > 0:
        time.sleep(delay)
    ticker, start_date, end_date = job['ticker'], job['start_date'], job['end_date']

    df, error_msg = fetch_stock_history(ticker, start_date, end_date)

    if error_msg:
        return {'status': 'error', 'job': job, 'message': error_msg}
    else:
        # Return df even if it's empty (no new data), but not on error
        return {'status': 'success', 'job': job, 'data': df}

# --- Main Pipeline Function (Refactored to use logger) ---
def run_stock_data_pipeline(
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
    max_workers = config.get_optional_int("YFINANCE_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    request_delay = config.get_optional_float("YFINANCE_REQUEST_DELAY", DEFAULT_REQUEST_DELAY)

    logger.info(f"--- Starting Stock Data Pipeline ---")
    logger.info(f"Database Target: {db_log_path}")
    logger.info(f"Run Mode: {mode}")
    if target_tickers: logger.info(f"Target Tickers: {target_tickers}")
    if mode == 'append' and append_start_date: logger.info(f"Append Start Date Override: {append_start_date}")
    logger.info(f"Batch Size (Tickers): {batch_size}")
    logger.info(f"Request Delay: {request_delay}s")


    valid_modes = ['initial_load', 'append', 'full_refresh', 'drop_tables']
    if mode not in valid_modes: logger.error(f"Invalid mode '{mode}'. Exiting."); return

    # Stats counters
    fetch_errors_count = 0; load_errors_count = 0; total_tickers_to_process = 0

    try:
        # Define PRAGMA settings for write-heavy operations
        write_pragmas = {
            'threads': os.cpu_count(),
            'memory_limit': '4GB'
        }
        if config.DUCKDB_TEMP_DIR:
            write_pragmas['temp_directory'] = f"'{config.DUCKDB_TEMP_DIR}'"

        # Use context manager with override or config default path
        with ManagedDatabaseConnection(db_path_override=db_log_path, read_only=False, pragma_settings=write_pragmas) as write_conn:
            if not write_conn:
                logger.critical("Exiting due to database connection failure.")
                return # Cannot proceed without connection

            # --- Mode Handling & Setup ---
            try:
                write_conn.begin() # Start transaction for setup/clear
                if mode == 'drop_tables': drop_stock_tables(write_conn); write_conn.commit(); logger.info("Drop finished."); return
                setup_tables(write_conn)
                if mode == 'full_refresh': clear_stock_tables(write_conn); logger.info("Full refresh clear done.")
                write_conn.commit() # Commit setup/clear transaction
            except Exception as setup_e:
                logger.error(f"Failed setup/clear: {setup_e}. Exiting.", exc_info=True)
                try: write_conn.rollback()
                except: pass
                return # Exit if setup fails
            
            # --- Identify Tickers to Process ---
            tickers_to_process = get_tickers_to_process(write_conn, target_tickers)
            if not tickers_to_process:
                logger.warning("No tickers found in the database to process.")
                return

            # --- Get Latest Dates for Append Mode ---
            latest_dates = {}
            if mode == 'append':
                latest_dates = get_latest_stock_dates(write_conn, target_tickers=tickers_to_process)

            # --- Prepare Jobs for Concurrent Fetching ---
            total_tickers_to_process = len(tickers_to_process)
            jobs_to_run: List[Dict[str, Any]] = []
            skipped_due_to_dates = 0
            logger.info(f"Preparing fetch jobs for {total_tickers_to_process} unique tickers...")

            for ticker in tqdm(tickers_to_process, desc="Preparing Jobs"):
                fetch_start_date = None; fetch_end_date = datetime.now(timezone.utc).date() # Default end date is today
                logger.debug(f"Preparing job for: {ticker}")

                # Determine fetch range
                if mode == 'append':
                    last_date = latest_dates.get(ticker); start_date_override = None
                    if append_start_date:
                        try: start_date_override = datetime.strptime(append_start_date, '%Y-%m-%d').date()
                        except ValueError: logger.error(f"Invalid append_start_date: {append_start_date}. Ignored.")
                    
                    if start_date_override: fetch_start_date = start_date_override
                    elif last_date: fetch_start_date = last_date + timedelta(days=1)
                    else: fetch_start_date = date(1990, 1, 1) # Fallback for new ticker: fetch from a long time ago

                else: # initial_load or full_refresh
                    fetch_start_date = date(1990, 1, 1) # Fetch all available history

                # Validate dates before fetching
                if not fetch_start_date or not fetch_end_date:
                     try: write_conn.begin(); log_fetch_error(write_conn, None, ticker, 'Date Error', 'Invalid fetch dates generated.'); write_conn.commit()
                     except Exception as log_e: logger.error(f"Failed log Date err {ticker}: {log_e}"); write_conn.rollback()
                     skipped_due_to_dates += 1; continue
                if fetch_start_date > fetch_end_date:
                    # This is a valid state (no new data to fetch), not an error.
                    logger.debug(f"Skipping {ticker}, start date {fetch_start_date} is after end date {fetch_end_date}.")
                    skipped_due_to_dates += 1
                    continue

                jobs_to_run.append({
                    'ticker': ticker, 'cik': None, # CIK not needed for this simplified logic
                    'start_date': fetch_start_date.strftime('%Y-%m-%d'),
                    'end_date': fetch_end_date.strftime('%Y-%m-%d')
                })

            logger.info(f"Prepared {len(jobs_to_run)} jobs. Skipped {skipped_due_to_dates} tickers due to date ranges.")

            # --- Concurrent Fetching ---
            all_fetched_dfs: List[pd.DataFrame] = []
            success_count = 0
            if jobs_to_run:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_job = {executor.submit(fetch_worker, job, request_delay): job for job in jobs_to_run}
                    progress = tqdm(as_completed(future_to_job), total=len(jobs_to_run), desc="Fetching Stock Data")

                    for future in progress:
                        job = future_to_job[future]
                        try:
                            result = future.result()
                            if result['status'] == 'success':
                                if result['data'] is not None and not result['data'].empty:
                                    all_fetched_dfs.append(result['data'])
                                success_count += 1
                            else:
                                fetch_errors_count += 1
                                log_fetch_error(write_conn, job['cik'], job['ticker'], 'Fetch Error', result['message'], job['start_date'], job['end_date'])
                        except Exception as exc:
                            fetch_errors_count += 1
                            log_fetch_error(write_conn, job['cik'], job['ticker'], 'Worker Error', f"Unhandled exception: {exc}", job['start_date'], job['end_date'])

            logger.info(f"Concurrent fetching complete. Success: {success_count}, Errors: {fetch_errors_count}")

            # --- Bulk Load ---
            if all_fetched_dfs:
                logger.info(f"Starting bulk load of {len(all_fetched_dfs)} fetched dataframes.")
                insert_mode = "REPLACE" if mode != 'full_refresh' else "INSERT"
                try:
                    write_conn.begin()
                    if not load_stock_data_batch(write_conn, all_fetched_dfs, insert_mode):
                        load_errors_count = len(all_fetched_dfs) # Mark all as failed if batch fails
                    write_conn.commit()
                except Exception as e:
                    logger.error(f"Critical error during bulk load transaction: {e}", exc_info=True)
                    load_errors_count = len(all_fetched_dfs)
                    try: write_conn.rollback()
                    except Exception as log_e: logger.error(f"Failed log Start>End err {ticker}: {log_e}"); write_conn.rollback()

            # --- End of Loop ---
            logger.info("Finished processing all identified tickers.")

        # Context manager handles final commit/rollback/close for the main connection

    except Exception as pipeline_e:
         logger.error(f"Pipeline error outside main loop: {pipeline_e}", exc_info=True)

    end_run_time = time.time()
    logger.info(f"--- Stock Data Pipeline Finished ---")
    logger.info(f"Total Time: {end_run_time - start_run_time:.2f} sec")
    logger.info(f"Tickers Targeted for Processing: {total_tickers_to_process}")
    logger.info(f"Tickers Successfully Fetched: {success_count}")
    logger.info(f"Fetch Errors Logged: {fetch_errors_count}")
    if load_errors_count > 0:
        logger.error(f"Load Errors: The final batch load failed for {load_errors_count} tickers.")
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
        config=config,
        mode='append', # Default mode
        batch_size=config.get_optional_int("STOCK_GATHERER_BATCH_SIZE", DEFAULT_BATCH_SIZE), # Example optional config
        # append_start_date=config.get_optional_var("STOCK_GATHERER_APPEND_START"), # Example optional config
        # target_tickers=None, # Or load from config/args
        db_path_override=None # Use DB from config by default
    )

    # Example: Initial load to memory db for specific tickers
    # run_stock_data_pipeline(
    #     config=config, # Still pass config even if DB is overridden
    #     mode='initial_load',
    #     target_tickers=['AAPL', 'MSFT'],
    #     db_path_override=':memory:'
    # )

    logger.info("Script execution finished.")