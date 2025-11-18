# -*- coding: utf-8 -*-
"""
Yahoo Finance Company Information Gatherer.

Fetches and stores supplementary company financial statement data from yfinance
(Income Statement, Balance Sheet, Cash Flow). This data complements the
historical stock prices and SEC filings.

Note: This script was previously configured to fetch profile metrics,
recommendations, and major holders, but this functionality has been
temporarily disabled to focus on core financial statements.
Uses:
- config_utils.AppConfig for loading configuration.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.
"""

import logging
import sys
import time
import os
import shutil
import multiprocessing
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from typing import Dict, Any, List

import pandas as pd
import yfinance as yf
import requests
from tqdm import tqdm



# --- Import Utilities ---
from utils.config_utils import AppConfig
import duckdb
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection
from data_processing import parquet_converter

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
DEFAULT_MAX_WORKERS = 4 # Reduced default to be more respectful of API rate limits

# Define constants for table names, now used for directory names
YF_INFO_FETCH_ERRORS = "yf_info_fetch_errors"
YF_INCOME_STATEMENT = "yf_income_statement"
YF_BALANCE_SHEET = "yf_balance_sheet"
YF_CASH_FLOW = "yf_cash_flow"

ALL_YF_TABLES = [YF_INFO_FETCH_ERRORS, YF_INCOME_STATEMENT, YF_BALANCE_SHEET, YF_CASH_FLOW]

# --- Database Schema ---
YF_TABLES_SCHEMA = { # This is now only used by the loader script
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
    "yf_balance_sheet": """
        CREATE TABLE IF NOT EXISTS yf_balance_sheet (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
    "yf_cash_flow": """
        CREATE TABLE IF NOT EXISTS yf_cash_flow (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
}

def get_untrackable_tickers(con: duckdb.DuckDBPyConnection, expiry_days: int = 365) -> set[str]:
    """
    Gets a set of tickers that have been marked as untrackable within the expiry period.
    Tickers marked untrackable longer ago than `expiry_days` will be retried.
    """
    logger.info(f"Querying for untrackable tickers (expiry: {expiry_days} days) to exclude...")
    untrackable_set: set[str] = set()
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


def _process_financial_statement(statement_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Helper function to transpose and melt a yfinance financial statement DataFrame."""
    if statement_df is None or statement_df.empty:
        return pd.DataFrame()
    try:
        statement_df = statement_df.transpose() # Dates become index, items become columns
        statement_df.reset_index(inplace=True)
        statement_df.rename(columns={'index': 'report_date'}, inplace=True) # The index is the date

        # Coerce to datetime and drop rows that couldn't be parsed.
        statement_df['report_date'] = pd.to_datetime(statement_df['report_date'], errors='coerce')
        statement_df.dropna(subset=['report_date'], inplace=True)

        if statement_df.empty:
            logger.warning(f"No valid dates found in financial statement for {ticker} after cleaning.")
            return pd.DataFrame()

        melted_df = statement_df.melt(id_vars=['report_date'], var_name='item_name', value_name='item_value')
        melted_df['ticker'] = ticker
        
        # report_date is already a datetime object, just need to get the date part
        melted_df['report_date'] = melted_df['report_date'].dt.date
        
        melted_df.dropna(subset=['item_value'], inplace=True)
        return melted_df[['ticker', 'report_date', 'item_name', 'item_value']]
    except Exception as e:
        logger.warning(f"Could not process financial statement for {ticker}: {e}")
        return pd.DataFrame()

def fetch_worker(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to be run in a process. Fetches all info for a single ticker.
    
    CRITICAL: Data preservation is the top priority. Each successful fetch is
    immediately written to individual recovery Parquet files before being returned,
    ensuring we never lose hard-won API data even if the process crashes.
    """
    ticker_str = job['ticker']
    
    # Create a temporary, process-specific cache directory inside a central .cache
    # folder to prevent file locks and keep the project root tidy.
    cache_parent_dir = Path("./.cache")
    cache_parent_dir.mkdir(exist_ok=True)
    temp_cache_dir = cache_parent_dir / f"yfinance_{os.getpid()}"
    temp_cache_dir.mkdir(exist_ok=True) # Create the specific process cache dir
    os.environ['YFINANCE_CACHE_DIR'] = str(temp_cache_dir.resolve())
    
    # Create a recovery directory for immediate data preservation
    recovery_dir = cache_parent_dir / "recovery_parquet"
    recovery_dir.mkdir(exist_ok=True)

    try:
        # Make retry parameters configurable via environment variables, with sane defaults
        max_retries = int(os.environ.get("YFINANCE_MAX_RETRIES", "5")) # type: ignore
        base_delay = float(os.environ.get("YFINANCE_BASE_DELAY", "15.0")) # type: ignore # Increased base delay for persistent rate-limiting

        for attempt in range(max_retries):
            error_msg = None
            try:
                session = requests.Session()
                ticker = yf.Ticker(ticker_str, session=session)

                # --- Fetch Financial Statements ---
                df_income = _process_financial_statement(ticker.income_stmt, ticker_str)
                df_balance = _process_financial_statement(ticker.balance_sheet, ticker_str)
                df_cashflow = _process_financial_statement(ticker.cashflow, ticker_str)

                # CRITICAL: Write to recovery Parquet IMMEDIATELY to preserve precious API data
                timestamp = int(time.time() * 1000)
                try:
                    if not df_income.empty:
                        recovery_file = recovery_dir / f"yf_income_{ticker_str}_{timestamp}.parquet"
                        df_income.to_parquet(recovery_file, index=False)
                        logger.info(f"PRESERVED: {ticker_str} income statement -> {recovery_file.name}")
                    if not df_balance.empty:
                        recovery_file = recovery_dir / f"yf_balance_{ticker_str}_{timestamp}.parquet"
                        df_balance.to_parquet(recovery_file, index=False)
                        logger.info(f"PRESERVED: {ticker_str} balance sheet -> {recovery_file.name}")
                    if not df_cashflow.empty:
                        recovery_file = recovery_dir / f"yf_cashflow_{ticker_str}_{timestamp}.parquet"
                        df_cashflow.to_parquet(recovery_file, index=False)
                        logger.info(f"PRESERVED: {ticker_str} cash flow -> {recovery_file.name}")
                except Exception as save_error:
                    logger.error(f"CRITICAL: Failed to save recovery data for {ticker_str}: {save_error}")
                    # Continue anyway - the main writer will still get it

                # If we get here, it's a success
                return {
                    'status': 'success', 'ticker': ticker_str,
                    YF_INCOME_STATEMENT: df_income, YF_BALANCE_SHEET: df_balance, YF_CASH_FLOW: df_cashflow
                }
            except Exception as e:
                e_str = str(e).lower()
                if "404" in e_str or "no data found" in e_str:
                    error_msg = f"Ticker {ticker_str} not found on yfinance (404)."
                    logger.debug(error_msg)
                elif "429" in e_str or "too many requests" in e_str:
                    error_msg = f"Rate limit hit for {ticker_str} (429)."
                    logger.warning(error_msg)
                else:
                    error_msg = f"yfinance info fetch failed for {ticker_str}: {type(e).__name__} - {e}"
                    logger.warning(error_msg)

            # --- Retry Logic ---
            is_retryable = error_msg and ("429" in error_msg or "too many requests" in error_msg.lower())
            if is_retryable:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + (os.urandom(1)[0] / 255.0)
                    logger.warning(f"Retryable error for {ticker_str} ('{error_msg[:50]}...'). Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"Failed after {max_retries} retries: {error_msg}"
                    logger.error(error_msg)
            
            # If we are here, it's a non-retryable error or the final failed retry
            if error_msg and "not found on yfinance" in error_msg:
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
                                conn.execute("INSERT OR REPLACE INTO yf_untrackable_tickers VALUES (?, ?, ?);", [ticker_str, "No data from yfinance", datetime.now(timezone.utc)])
                                logger.info(f"Marked {ticker_str} as untrackable in the database.")
                except Exception as db_e:
                    logger.error(f"Failed to write untrackable status for {ticker_str} to DB: {db_e}", exc_info=True)

            return {'status': 'error', 'ticker': ticker_str, 'message': error_msg or "Exited retry loop unexpectedly."}
        # This path should not be reachable due to the logic inside the loop, but is required for type checkers
        return {'status': 'error', 'ticker': ticker_str, 'message': "Exited fetch worker unexpectedly."}
    finally:
        # Ensure the temporary cache directory is always removed
        try:
            shutil.rmtree(temp_cache_dir)
        except OSError as e:
            logger.error(f"Error removing temp cache dir {temp_cache_dir}: {e}")

def writer_process(queue: multiprocessing.Queue, parquet_dir: Path):
    """
    A separate process that listens on a queue for data and writes it to Parquet files.
    """
    parquet_converter.logger = logger # Share logger
    batches: Dict[str, List[Any]] = {table_name: [] for table_name in ALL_YF_TABLES}
    BATCH_SIZE = 250 # Number of tickers to accumulate before writing
    FLUSH_TIMEOUT = 10.0 # seconds

    def _flush_batch(data_type: str):
        batch = batches[data_type]
        if not batch: return

        logger.info(f"Flushing batch of {len(batch)} records for '{data_type}'...")
        if data_type == YF_INFO_FETCH_ERRORS:
            df = pd.DataFrame(batch)
        else:
            df = pd.concat(batch, ignore_index=True)

        # This simple approach works because each DataFrame was prepared by the worker
        # and has the correct columns. The loader script will handle type casting.
        parquet_converter.save_dataframe_to_parquet(df, parquet_dir / data_type)
        batch.clear()

    while True:
        data_type = None # Initialize to prevent UnboundLocalError in exception logging
        try:
            # Use the specific queue from multiprocessing for type hints
            item: Any = queue.get(timeout=FLUSH_TIMEOUT)
            if item is None: # Sentinel
                for table in ALL_YF_TABLES: _flush_batch(table)
                logger.info("Writer process received sentinel. Shutting down.")
                break

            data_type, data = item
            if data_type in batches:
                batches[data_type].append(data)
                if len(batches[data_type]) >= BATCH_SIZE:
                    _flush_batch(data_type)

        except (multiprocessing.queues.Empty, AttributeError): # Handle queue.Empty for type safety
            logger.debug("Writer queue timeout, flushing any pending batches...")
            for table in ALL_YF_TABLES: _flush_batch(table)
        except Exception as e:
            logger.error(f"Writer process failed to write data of type {data_type}: {e}", exc_info=True)

def run_info_gathering_pipeline(config: AppConfig):
    """Main orchestration function for the info gathering pipeline."""
    logger.info("--- Starting Yahoo Finance Info to Parquet Pipeline ---")
    logger.info(f"yfinance version: {yf.__version__}")
    start_time = time.time()
    max_workers = config.get_optional_int("YFINANCE_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    logger.info(f"Using up to {max_workers} workers. Retry policy: {os.environ.get('YFINANCE_MAX_RETRIES', '5')} retries, {os.environ.get('YFINANCE_BASE_DELAY', '15.0')}s base delay.")

    # This script now only needs a READ-ONLY connection to get the list of tickers.
    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as con:
            if not con:
                logger.critical("Database connection failed. Exiting.")
                return

            # 1. Get tickers to process
            logger.info("Querying for unique tickers to process...")
            tickers_df = con.sql("SELECT DISTINCT ticker FROM tickers ORDER BY ticker").df()
            tickers = tickers_df['ticker'].tolist()
            logger.info(f"Found {len(tickers)} total unique tickers in DB.")
            if not tickers:
                logger.warning("No tickers found in the database to process.")
                return

            # Exclude untrackable tickers
            untrackable_expiry_days = config.get_optional_int("YFINANCE_UNTRACKABLE_EXPIRY_DAYS", 365)
            untrackable_tickers = get_untrackable_tickers(con, expiry_days=untrackable_expiry_days)
            initial_count = len(tickers)
            tickers = [t for t in tickers if t not in untrackable_tickers]
            skipped_untrackable = initial_count - len(tickers)
            logger.info(f"Preparing to process {len(tickers)} tickers (skipped {skipped_untrackable} untrackable).")

        # DB connection is now closed.

        # 2. Process tickers concurrently, writing to Parquet
        success_count = 0
        error_count = 0
        jobs = [{'ticker': t, 'db_path': config.DB_FILE_STR} for t in tickers]

        manager = multiprocessing.Manager()
        write_queue = manager.Queue()

        with ProcessPoolExecutor(max_workers=1) as writer_executor, \
             ProcessPoolExecutor(max_workers=max_workers) as fetch_executor:

            # Start the single writer process
            writer_future = writer_executor.submit(writer_process, write_queue, config.PARQUET_DIR)

            # Submit all fetch jobs to the executor
            future_to_ticker: Dict[Future[Dict[str, Any]], str] = {fetch_executor.submit(fetch_worker, job): job['ticker'] for job in jobs}

            # Process results as they complete
            progress = tqdm(as_completed(future_to_ticker), total=len(jobs), desc="Gathering Company Info")
            for future in progress:
                ticker_str = future_to_ticker[future]
                try:
                    result = future.result()
                    if result['status'] == 'success':
                        success_count += 1
                        # Put each successful dataframe onto the queue
                        for table_name in ALL_YF_TABLES:
                            if table_name in result and not result[table_name].empty:
                                write_queue.put((table_name, result[table_name]))
                    else: # status == 'error'
                        # The worker now handles untrackable tickers directly.
                        # We just log all other errors to the Parquet error file.
                        error_count += 1
                        message = result.get('message', 'Unknown error')
                        # Avoid double-logging the untrackable ones to the error file
                        if "not found on yfinance" not in message:
                            error_record = {
                                'ticker': result['ticker'],
                                'error_timestamp': datetime.now(timezone.utc),
                                'error_message': message
                            }
                            write_queue.put((YF_INFO_FETCH_ERRORS, error_record))
                        else:
                            # We still count it as an error for the summary, but it's been handled.
                            logger.debug(f"Skipping error log for {ticker_str}, already marked as untrackable.")

                except Exception as exc:
                    logger.error(f"Unhandled exception for ticker {ticker_str}: {exc}", exc_info=True)
                    error_count += 1

            # Signal the writer process to terminate and wait for it
            write_queue.put(None)
            writer_future.result()

    except Exception as pipeline_e:
        logger.critical(f"A critical error occurred in the pipeline: {pipeline_e}", exc_info=True)

    end_time = time.time()
    logger.info("--- Yahoo Finance Info to Parquet Pipeline Finished ---")
    logger.info(f"Total Time: {end_time - start_time:.2f} seconds")
    logger.info(f"Successfully fetched: {success_count} tickers")
    logger.info(f"Errors logged: {error_count} tickers")
    logger.info(f"Data written to Parquet files in: {config.PARQUET_DIR}")

if __name__ == "__main__":
    # CRITICAL: YFinance info gathering is DISABLED by default until we have a prioritization strategy.
    # The rate limits are severe and we cannot afford to waste API calls on low-priority tickers.
    # When enabled, all fetched data is written to Parquet IMMEDIATELY to prevent any loss.
    if os.environ.get("YFINANCE_DISABLED", "1") == "1":
        logger.warning("=" * 80)
        logger.warning("YFinance info gathering is DISABLED (YFINANCE_DISABLED=1).")
        logger.warning("This script will NOT run until you:")
        logger.warning("  1. Develop a ticker prioritization strategy")
        logger.warning("  2. Set YFINANCE_DISABLED=0 in your environment")
        logger.warning("  3. Configure rate limiting via YFINANCE_MAX_RETRIES and YFINANCE_BASE_DELAY")
        logger.warning("=" * 80)
        if os.environ.get("YFINANCE_MINIMAL", "0") == "1":
            try:
                _ = yf.Ticker("AAPL").income_stmt
                logger.info("YFinance minimal info query succeeded (AAPL income_stmt).")
            except Exception as e:
                logger.warning(f"YFinance minimal info query failed: {e}")
        sys.exit(0)

    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
        run_info_gathering_pipeline(app_config)
    except SystemExit as e:
        logger.critical(f"Configuration failed, cannot start: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred at startup: {e}", exc_info=True)
        sys.exit(1)