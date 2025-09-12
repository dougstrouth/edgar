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
import multiprocessing
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, List, Set, Tuple, Any, Union

import requests
import duckdb
import pandas as pd
import yfinance as yf
from tqdm import tqdm

# --- Import Utilities ---
from config_utils import AppConfig                 # Import configuration loader
from logging_utils import setup_logging            # Import logging setup function
from database_conn import ManagedDatabaseConnection
import parquet_converter # Reuse the parquet saving utility

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
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

def get_untrackable_tickers(con: duckdb.DuckDBPyConnection, expiry_days: int = 365) -> Set[str]:
    """
    Gets a set of tickers that have been marked as untrackable within the expiry period.
    Tickers marked untrackable longer ago than `expiry_days` will be retried.
    """
    logger.info(f"Querying for untrackable tickers (expiry: {expiry_days} days) to exclude...")
    untrackable_set: Set[str] = set()
    try: # Check if table exists first
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "yf_untrackable_tickers" in tables:
            query = f"SELECT ticker FROM yf_untrackable_tickers WHERE last_failed_timestamp >= (now() - INTERVAL '{expiry_days} days');"
            results = con.execute(query).fetchall()
            untrackable_set = {row[0] for row in results}
            logger.info(f"Found {len(untrackable_set)} recently untrackable tickers to exclude from this run.")
    except Exception as e:
        logger.error(f"Could not query untrackable tickers: {e}", exc_info=True)
    return untrackable_set

def get_latest_stock_dates(
    con: duckdb.DuckDBPyConnection,
    target_tickers: Optional[List[str]] = None
) -> Dict[str, date]:
    """Queries the DB for the latest stock date recorded for each ticker."""
    logger.info("Querying latest existing stock data dates...")
    latest_dates: Dict[str, date] = {}; table_exists = False
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

# --- Stock Data Fetching (Updated to use logger) ---
def fetch_stock_history(
    ticker: str,
    start_date: str,
    end_date: str,
    session: requests.Session
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Fetches stock history using yfinance for a given ticker and date range.
    This function is designed to be thread-safe.
    """
    logger.debug(f"Fetching {ticker} from {start_date} to {end_date}")
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

        if start_dt > end_dt:
            logger.warning(f"Start date > End date: {start_date} > {end_date} for {ticker}. Skipping fetch.")
            return None, "Start date after end date"

        # Fetch data including the end_date by adding one day to the end param
        # Use a standard requests.Session object to bypass yfinance's internal,
        stock = yf.Ticker(ticker, session=session)
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

def fetch_worker(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to be run in a process. Fetches stock history for a single ticker.
    Includes retry logic with exponential backoff for rate-limiting errors.
    """
    # Create a temporary, process-specific cache directory to prevent file locks
    temp_cache_dir = Path(f"./.yfinance_cache_{os.getpid()}")
    temp_cache_dir.mkdir(exist_ok=True)
    os.environ['YFINANCE_CACHE_DIR'] = str(temp_cache_dir.resolve())

    try:
        # Create a single session for this worker process to pass to yfinance
        session = requests.Session()

        ticker, start_date, end_date = job['ticker'], job['start_date'], job['end_date']
        # Make retry parameters configurable via environment variables, with sane defaults
        max_retries = int(os.environ.get("YFINANCE_MAX_RETRIES", "5"))
        base_delay = float(os.environ.get("YFINANCE_BASE_DELAY", "15.0")) # Increased base delay for persistent rate-limiting

        for attempt in range(max_retries):
            df, error_msg = fetch_stock_history(ticker, start_date, end_date, session)

            if not error_msg:
                return {'status': 'success', 'job': job, 'data': df}

            is_retryable = (
                "429" in error_msg or
                "too many requests" in error_msg.lower() or
                "unable to open database file" in error_msg.lower()
            )

            if is_retryable:
                if attempt < max_retries - 1:
                    # Exponential backoff with a random jitter
                    wait_time = base_delay * (2 ** attempt) + (os.urandom(1)[0] / 255.0)
                    logger.warning(f"Retryable error for {ticker} ('{error_msg[:50]}...'). Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    final_error_msg = f"Failed after {max_retries} retries: {error_msg}"
                    logger.error(final_error_msg)
                    return {'status': 'error', 'job': job, 'message': final_error_msg}
            else:
                # For "no data" errors, immediately write to the DB to make future runs smarter
                if error_msg == "No data from yfinance":
                    try:
                        db_path = job.get('db_path')
                        if db_path:
                            with ManagedDatabaseConnection(db_path_override=db_path, read_only=False) as conn:
                                if conn:
                                    conn.execute("""
                                        CREATE TABLE IF NOT EXISTS yf_untrackable_tickers (
                                            ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                                            reason VARCHAR,
                                            last_failed_timestamp TIMESTAMPTZ
                                        );
                                    """)
                                    conn.execute("INSERT OR REPLACE INTO yf_untrackable_tickers VALUES (?, ?, ?);", [job['ticker'], error_msg, datetime.now(timezone.utc)])
                                    logger.info(f"Marked {job['ticker']} as untrackable in the database.")
                        else:
                            logger.warning(f"No db_path in job, cannot mark {job['ticker']} as untrackable.")
                    except Exception as db_e:
                        logger.error(f"Failed to write untrackable status for {job['ticker']} to DB: {db_e}", exc_info=True)

                return {'status': 'error', 'job': job, 'message': error_msg}

        # This part should not be reached, but as a fallback:
        return {'status': 'error', 'job': job, 'message': "Exited retry loop unexpectedly."}
    finally:
        # Ensure the temporary cache directory is always removed
        try:
            shutil.rmtree(temp_cache_dir)
        except OSError as e:
            logger.error(f"Error removing temp cache dir {temp_cache_dir}: {e}")

def writer_process(queue: Any, parquet_dir: Path):
    """
    A separate process that listens on a queue for data and writes it to Parquet files.
    This version batches data before writing to improve performance.
    """
    parquet_converter.logger = logger # Share logger
    history_batch = []
    errors_batch = []
    BATCH_SIZE = 1000 # Number of items to accumulate before writing
    FLUSH_TIMEOUT = 5.0 # seconds

    def _flush_batch(batch: list, data_type: str):
        if not batch: return
        # Be more explicit in logging that we are batching tickers/errors
        item_name = "tickers" if data_type == 'stock_history' else "errors"
        logger.info(f"Flushing batch of {len(batch)} {item_name} to Parquet for '{data_type}'...")
        if data_type == 'stock_history':
            df = pd.concat(batch, ignore_index=True)
            df = parquet_converter._prepare_df_for_storage(df, str_cols=['ticker'], date_cols=['date'], numeric_cols=['open', 'high', 'low', 'close', 'adj_close'], int_cols=['volume'])
            parquet_converter.save_dataframe_to_parquet(df, parquet_dir / "stock_history")
        elif data_type == 'stock_fetch_errors':
            df = pd.DataFrame(batch)
            df = parquet_converter._prepare_df_for_storage(df, str_cols=['cik', 'ticker', 'error_type', 'error_message'], date_cols=['error_timestamp', 'start_date_req', 'end_date_req'])
            parquet_converter.save_dataframe_to_parquet(df, parquet_dir / "stock_fetch_errors")
        batch.clear()

    while True:
        try:
            # Wait for an item, but with a timeout to allow for periodic flushing
            item = queue.get(timeout=FLUSH_TIMEOUT)

            if item is None: # Sentinel value to signal termination
                _flush_batch(history_batch, 'stock_history')
                _flush_batch(errors_batch, 'stock_fetch_errors')
                logger.info("Writer process received sentinel. Shutting down.")
                break

            data_type, data = item
            try:
                if data_type == 'stock_history' and not data.empty:
                    history_batch.append(data)
                    if len(history_batch) >= BATCH_SIZE:
                        _flush_batch(history_batch, 'stock_history')
                elif data_type == 'stock_fetch_errors' and data:
                    errors_batch.append(data)
                    if len(errors_batch) >= BATCH_SIZE:
                        _flush_batch(errors_batch, 'stock_fetch_errors')
            except Exception as e:
                logger.error(f"Writer process failed to handle data of type {data_type}: {e}", exc_info=True)
        except multiprocessing.queues.Empty:
            # The queue was empty for the timeout period, a good time to flush any pending data
            logger.debug("Writer queue timeout reached, flushing any pending batches...")
            _flush_batch(history_batch, 'stock_history')
            _flush_batch(errors_batch, 'stock_fetch_errors')
        except Exception as e:
            logger.error(f"Writer process encountered an unexpected error: {e}", exc_info=True)

# Define constants for table names, now used for directory names
STOCK_TABLE_NAME = "stock_history"
ERROR_TABLE_NAME = "stock_fetch_errors"

# --- Main Pipeline Function (Refactored to use logger) ---
def run_stock_data_pipeline(
    config: AppConfig,      # Pass config
    mode: str = 'append',   # Changed default to 'append'
    append_start_date: Optional[str] = None,
    target_tickers: Optional[List[str]] = None,
    db_path_override: Optional[str] = None
):
    """Fetches stock data and saves it to Parquet files."""
    start_run_time = time.time()
    db_log_path = db_path_override if db_path_override else config.DB_FILE_STR # Use config default
    max_workers = config.get_optional_int("YFINANCE_MAX_WORKERS", DEFAULT_MAX_WORKERS)

    logger.info(f"--- Starting Stock Data to Parquet Pipeline ---")
    logger.info(f"yfinance version: {yf.__version__}")
    logger.info(f"Parquet Target: {config.PARQUET_DIR}")
    logger.info(f"Run Mode: {mode}")
    logger.info(f"Using up to {max_workers} workers. Retry policy: {os.environ.get('YFINANCE_MAX_RETRIES', '5')} retries, {os.environ.get('YFINANCE_BASE_DELAY', '15.0')}s base delay.")
    if target_tickers: logger.info(f"Target Tickers: {target_tickers}")
    if mode == 'append' and append_start_date: logger.info(f"Append Start Date Override: {append_start_date}")


    valid_modes = ['initial_load', 'append', 'full_refresh', 'drop_tables']
    if mode not in valid_modes: logger.error(f"Invalid mode '{mode}'. Exiting."); return

    # Stats counters
    fetch_errors_count = 0; success_count = 0; total_tickers_to_process = 0; skipped_due_to_dates = 0; skipped_untrackable = 0

    # --- STAGE 1: SETUP & JOB PREPARATION (DB Connection is opened and closed here) ---
    jobs_to_run: List[Dict[str, Any]] = []
    try:
        write_pragmas = {
            'threads': os.cpu_count(),
            'memory_limit': '4GB'
        }
        if config.DUCKDB_TEMP_DIR:
            write_pragmas['temp_directory'] = f"'{config.DUCKDB_TEMP_DIR}'"

        # --- Cleanliness Step for Parquet directories ---
        if mode == 'full_refresh':
            logger.info("Full refresh mode: Cleaning previous stock Parquet data...")
            stock_history_dir = config.PARQUET_DIR / STOCK_TABLE_NAME
            stock_errors_dir = config.PARQUET_DIR / ERROR_TABLE_NAME
            if stock_history_dir.exists(): shutil.rmtree(stock_history_dir)
            if stock_errors_dir.exists(): shutil.rmtree(stock_errors_dir)
            logger.info("Previous stock Parquet data cleaned.")

        with ManagedDatabaseConnection(db_path_override=db_log_path, read_only=False, pragma_settings=write_pragmas) as conn:
            if not conn:
                logger.critical("Exiting due to database connection failure.")
                return

            if mode == 'drop_tables':
                logger.warning("Drop tables mode is not applicable for Parquet generation. Exiting.")
                return

            tickers_to_process = get_tickers_to_process(conn, target_tickers)
            if not tickers_to_process:
                logger.warning("No tickers found in the database to process.")
                return

            # Get the set of tickers to exclude
            untrackable_expiry_days = config.get_optional_int("YFINANCE_UNTRACKABLE_EXPIRY_DAYS", 365)
            untrackable_tickers = get_untrackable_tickers(conn, expiry_days=untrackable_expiry_days)

            latest_dates = {}
            if mode == 'append':
                # This still reads from the DB to see what's already been loaded
                latest_dates = get_latest_stock_dates(conn, target_tickers=tickers_to_process)

            # Filter out untrackable tickers
            initial_count = len(tickers_to_process)
            tickers_to_process = [t for t in tickers_to_process if t not in untrackable_tickers]
            skipped_untrackable = initial_count - len(tickers_to_process)
            total_tickers_to_process = len(tickers_to_process) # Update count

            logger.info(f"Preparing {total_tickers_to_process} fetch jobs (skipped {skipped_untrackable} untrackable tickers)...")
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

                if not fetch_start_date or not fetch_end_date:
                     logger.warning(f"Invalid fetch dates generated for {ticker}. Skipping.")
                     skipped_due_to_dates += 1; continue
                if fetch_start_date > fetch_end_date:
                    logger.debug(f"Skipping {ticker}, start date {fetch_start_date} is after end date {fetch_end_date}.")
                    skipped_due_to_dates += 1
                    continue

                jobs_to_run.append({
                    'ticker': ticker, 'cik': None, # CIK not needed for this simplified logic
                    'start_date': fetch_start_date.strftime('%Y-%m-%d'),
                    'end_date': fetch_end_date.strftime('%Y-%m-%d'),
                    'db_path': db_log_path
                })
        # The 'with' block for the DB connection ends here, ensuring it is closed.
    except Exception as e:
        logger.critical(f"Failed during STAGE 1 (Job Preparation): {e}", exc_info=True)
        return # Exit if we can't even prepare the jobs

    logger.info(f"Prepared {len(jobs_to_run)} jobs. Skipped {skipped_due_to_dates} (date logic) + {skipped_untrackable} (untrackable) tickers.")

    # --- STAGE 2: CONCURRENT FETCHING & WRITING TO PARQUET ---
    if jobs_to_run:
        logger.info("--- Starting STAGE 2: Concurrent Fetching and Writing to Parquet ---")
        # Use a manager queue for inter-process communication
        manager = multiprocessing.Manager()
        write_queue = manager.Queue()

        # Use ProcessPoolExecutor for both writer and fetchers for complete isolation
        with ProcessPoolExecutor(max_workers=1) as writer_executor, \
             ProcessPoolExecutor(max_workers=max_workers) as fetch_executor:
            # Start the single writer process
            writer_future = writer_executor.submit(writer_process, write_queue, config.PARQUET_DIR)

            future_to_job = {fetch_executor.submit(fetch_worker, job): job for job in jobs_to_run}
            progress = tqdm(as_completed(future_to_job), total=len(jobs_to_run), desc="Fetching Stock Data")

            for future in progress:
                try:
                    result = future.result()
                    if result['status'] == 'success':
                        success_count += 1
                        if result.get('data') is not None and not result['data'].empty:
                            write_queue.put(('stock_history', result['data']))
                    elif result['status'] == 'error':
                        fetch_errors_count += 1
                        job = result['job']
                        # The worker now handles untrackable tickers directly.
                        # We just log all other errors to the Parquet error file.
                        error_record = {
                            'cik': job['cik'], 'ticker': job['ticker'], 'error_timestamp': datetime.now(timezone.utc),
                            'error_type': 'Fetch Error', 'error_message': result['message'],
                            'start_date_req': datetime.strptime(job['start_date'], '%Y-%m-%d').date(),
                            'end_date_req': datetime.strptime(job['end_date'], '%Y-%m-%d').date()
                        }
                        write_queue.put(('stock_fetch_errors', error_record))
                except Exception as exc:
                    job = future_to_job[future]
                    logger.error(f"Unhandled exception for ticker {job['ticker']}: {exc}", exc_info=True)
                    fetch_errors_count += 1

            # Signal the writer process to terminate
            write_queue.put(None)
            # Wait for the writer to finish
            writer_future.result()

    end_run_time = time.time()
    logger.info(f"--- Stock Data to Parquet Pipeline Finished ---")
    logger.info(f"Total Time: {end_run_time - start_run_time:.2f} sec")
    logger.info(f"Tickers Targeted for Processing: {total_tickers_to_process}")
    logger.info(f"Tickers Successfully Fetched: {success_count}")
    logger.info(f"Fetch Errors Logged: {fetch_errors_count}")
    logger.info(f"Data written to Parquet files in: {config.PARQUET_DIR}")


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
        mode='full_refresh', # Default mode
        # append_start_date=config.get_optional_var("STOCK_GATHERER_APPEND_START"), # Example optional config
        # target_tickers=None, # Or load from config/args
        db_path_override=None # Use DB from config by default
    )

    logger.info("Script execution finished.")