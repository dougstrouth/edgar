# -*- coding: utf-8 -*-
"""
Stock Data Gatherer Script (v7)

Connects to EDGAR DuckDB, identifies CIKs and date ranges, maps CIKs
to potentially MULTIPLE tickers using sec-cik-mapper, fetches historical
stock data for ALL found tickers using yfinance, loads into DuckDB, and
logs errors. Supports modes, batch loading, and error logging.
"""

import duckdb
import pandas as pd
import yfinance as yf
from sec_cik_mapper import StockMapper
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
import sys
import time
from datetime import datetime, date, timezone, timedelta
from tqdm import tqdm
from typing import Optional, Dict, List, Set, Tuple

# --- Configuration from .env ---
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'
if not dotenv_path.is_file():
    print(f"ERROR: .env file not found at {dotenv_path}", file=sys.stderr)
    sys.exit(1)
load_dotenv(dotenv_path=dotenv_path)

try:
    DEFAULT_DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError as e:
    sys.exit(f"ERROR: Missing required .env variable: {e}")

# --- Logging Setup ---
log_file_path = script_dir / "stock_data_gatherer.log"
log_file_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

# --- Constants ---
STOCK_TABLE_NAME = "stock_history"
ERROR_TABLE_NAME = "stock_fetch_errors"
YFINANCE_DELAY = 0.5 # Optional delay (seconds)
DEFAULT_BATCH_SIZE = 50 # Number of TICKERS to process per DB batch load

# --- Database Functions ---
# Start: DB Functions (unchanged from v5/v6) ---
def connect_db(db_path: Path, read_only: bool = True) -> Optional[duckdb.DuckDBPyConnection]:
    """Connects to the DuckDB database file."""
    if not db_path.is_file():
        logging.error(f"Database file not found at: {db_path}")
        return None
    try:
        conn = duckdb.connect(database=str(db_path), read_only=read_only)
        logging.info(f"Successfully connected to database: {db_path} (Read-Only: {read_only})")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database {db_path}: {e}", exc_info=True)
        return None

def get_companies_and_xbrl_dates(con: duckdb.DuckDBPyConnection, target_ciks: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Queries for CIKs and their XBRL fact date ranges.
    Optionally filters for a specific list of CIKs.
    Returns DataFrame with columns: cik, min_date, max_date
    """
    logging.info("Querying database for CIKs and XBRL fact date ranges...")
    # This query identifies CIKs present in the companies table that also have fact data
    base_query = """
    WITH XbrlDates AS (
        SELECT
            f.cik,
            MIN(f.period_start_date) AS min_start_date,
            MAX(f.period_end_date) AS max_end_date
        FROM xbrl_facts f
        WHERE f.period_start_date IS NOT NULL OR f.period_end_date IS NOT NULL
        GROUP BY f.cik
    )
    SELECT DISTINCT -- Ensure unique CIKs if companies table has duplicates (shouldn't happen)
        c.cik,
        -- Use the earliest of min_start or max_end (if start is missing) as overall min
        COALESCE(xd.min_start_date, xd.max_end_date) as min_date,
        -- Use the latest of max_end or min_start (if end is missing) as overall max
        COALESCE(xd.max_end_date, xd.min_start_date) as max_date
    FROM companies c
    JOIN XbrlDates xd ON c.cik = xd.cik -- Only include CIKs with facts
    WHERE (xd.min_start_date IS NOT NULL OR xd.max_end_date IS NOT NULL)
    """
    params = []
    if target_ciks:
        placeholders = ', '.join(['?'] * len(target_ciks))
        # Filter on the CIK from the companies table
        base_query += f" AND c.cik IN ({placeholders})"
        params.extend(target_ciks)

    base_query += " ORDER BY c.cik;" # Add ordering for consistency

    try:
        df = con.execute(base_query, params).fetchdf()
        if not df.empty:
            df['min_date'] = pd.to_datetime(df['min_date']).dt.date
            df['max_date'] = pd.to_datetime(df['max_date']).dt.date
            logging.info(f"Found {len(df)} CIKs with XBRL fact date ranges matching criteria.")
            logging.debug(f"Sample CIK Date Ranges (first 5):\n{df.head().to_string()}")
        else:
             logging.warning("No CIKs found with XBRL date ranges matching criteria.")
        return df
    except Exception as e:
        logging.error(f"Failed to query company/XBRL date data: {e}", exc_info=True)
        return pd.DataFrame()

def get_ticker_date_range(con: duckdb.DuckDBPyConnection, ticker: str) -> Optional[Tuple[str, date, date]]:
     """Gets the CIK and date range for a specific ticker."""
     logging.debug(f"Getting date range for specific ticker: {ticker}")
     query = """
        WITH XbrlDates AS (
            SELECT
                f.cik,
                MIN(f.period_start_date) AS min_start_date,
                MAX(f.period_end_date) AS max_end_date
            FROM xbrl_facts f
            WHERE f.period_start_date IS NOT NULL OR f.period_end_date IS NOT NULL
            GROUP BY f.cik
        )
        SELECT
            t.cik,
            COALESCE(xd.min_start_date, xd.max_end_date) as min_date,
            COALESCE(xd.max_end_date, xd.min_start_date) as max_date
        FROM tickers t
        JOIN XbrlDates xd ON t.cik = xd.cik
        WHERE t.ticker = ?
        LIMIT 1;
     """
     try:
         result = con.execute(query, [ticker]).fetchone()
         if result:
             cik, min_d, max_d = result
             min_date = pd.to_datetime(min_d).date() if pd.notna(min_d) else None
             max_date = pd.to_datetime(max_d).date() if pd.notna(max_d) else None
             if cik and min_date and max_date:
                 return str(cik), min_date, max_date
             else:
                 logging.warning(f"Found ticker {ticker} but missing CIK or date range (CIK: {cik}, Min: {min_date}, Max: {max_date})")
                 return None
         else:
             logging.warning(f"Ticker {ticker} not found or has no associated fact dates.")
             return None
     except Exception as e:
         logging.error(f"Failed to get date range for ticker {ticker}: {e}", exc_info=True)
         return None


def get_latest_stock_dates(con: duckdb.DuckDBPyConnection, target_tickers: Optional[List[str]] = None) -> Dict[str, date]:
    """Queries for the latest date present for each ticker in stock_history."""
    logging.info("Querying latest existing stock data dates...")
    latest_dates = {}
    base_query = f"SELECT ticker, MAX(date) as max_date FROM {STOCK_TABLE_NAME}"
    params = []
    if target_tickers:
        placeholders = ', '.join(['?'] * len(target_tickers))
        base_query += f" WHERE ticker IN ({placeholders})"
        params.extend(target_tickers)
    base_query += " GROUP BY ticker;"

    try:
        tables = con.execute("SHOW TABLES;").fetchall()
        if any(STOCK_TABLE_NAME.lower() == t[0].lower() for t in tables):
             results = con.execute(base_query, params).fetchall()
             for ticker, max_date_val in results:
                  if max_date_val:
                    max_date = None
                    if isinstance(max_date_val, datetime): max_date = max_date_val.date()
                    elif isinstance(max_date_val, date): max_date = max_date_val
                    else:
                        try: max_date = pd.to_datetime(max_date_val).date()
                        except: logging.warning(f"Could not convert max_date {max_date_val} to date for {ticker}")
                    if max_date: latest_dates[ticker] = max_date

             logging.info(f"Found latest dates for {len(latest_dates)} tickers already in DB.")
        else:
             logging.warning(f"Table '{STOCK_TABLE_NAME}' does not exist. Cannot get latest dates.")
    except Exception as e:
        logging.error(f"Failed to query latest stock dates: {e}", exc_info=True)
    return latest_dates

def setup_tables(con: duckdb.DuckDBPyConnection):
    """Creates the stock_history and stock_fetch_errors tables if they don't exist."""
    logging.info(f"Setting up database tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'")
    create_stock_sql = f"""
    CREATE TABLE IF NOT EXISTS {STOCK_TABLE_NAME} (
        ticker          VARCHAR NOT NULL COLLATE NOCASE,
        date            DATE NOT NULL,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE,
        adj_close       DOUBLE,
        volume          BIGINT,
        PRIMARY KEY (ticker, date)
    );
    """
    create_error_sql = f"""
    CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (
        cik             VARCHAR,
        ticker          VARCHAR COLLATE NOCASE,
        error_timestamp TIMESTAMPTZ NOT NULL,
        error_type      VARCHAR NOT NULL,
        error_message   VARCHAR,
        start_date_req  DATE,
        end_date_req    DATE
    );
    """
    try:
        con.execute(create_stock_sql)
        logging.info(f"Table '{STOCK_TABLE_NAME}' created or already exists.")
        con.execute(create_error_sql)
        logging.info(f"Table '{ERROR_TABLE_NAME}' created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create tables: {e}", exc_info=True)
        raise

def drop_stock_tables(con: duckdb.DuckDBPyConnection):
    """Drops the stock_history and stock_fetch_errors tables."""
    logging.warning(f"Dropping tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'...")
    try:
        con.execute(f"DROP TABLE IF EXISTS {STOCK_TABLE_NAME};")
        logging.info(f"Table '{STOCK_TABLE_NAME}' dropped.")
        con.execute(f"DROP TABLE IF EXISTS {ERROR_TABLE_NAME};")
        logging.info(f"Table '{ERROR_TABLE_NAME}' dropped.")
    except Exception as e:
        logging.error(f"Failed to drop tables: {e}", exc_info=True)
        raise

def clear_stock_tables(con: duckdb.DuckDBPyConnection):
    """Clears all data from the stock_history and stock_fetch_errors tables."""
    logging.warning(f"Clearing all data from tables '{STOCK_TABLE_NAME}' and '{ERROR_TABLE_NAME}'...")
    try:
        tables = con.execute("SHOW TABLES;").fetchall()
        if any(STOCK_TABLE_NAME.lower() == t[0].lower() for t in tables):
            con.execute(f"DELETE FROM {STOCK_TABLE_NAME};")
            logging.info(f"All data deleted from '{STOCK_TABLE_NAME}'.")
        else:
             logging.warning(f"Table '{STOCK_TABLE_NAME}' does not exist, skipping delete.")

        if any(ERROR_TABLE_NAME.lower() == t[0].lower() for t in tables):
             con.execute(f"DELETE FROM {ERROR_TABLE_NAME};")
             logging.info(f"All data deleted from '{ERROR_TABLE_NAME}'.")
        else:
             logging.warning(f"Table '{ERROR_TABLE_NAME}' does not exist, skipping delete.")

    except Exception as e:
        logging.error(f"Failed to clear tables: {e}", exc_info=True)
        raise

def log_fetch_error(con: duckdb.DuckDBPyConnection, cik: Optional[str], ticker: Optional[str], error_type: str, message: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Logs an error record to the stock_fetch_errors table."""
    timestamp = datetime.now(timezone.utc)
    logging.warning(f"Logging error: CIK={cik}, Ticker={ticker}, Type={error_type}, Msg={message}")
    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None

        con.execute(
            f"INSERT INTO {ERROR_TABLE_NAME} (cik, ticker, error_timestamp, error_type, error_message, start_date_req, end_date_req) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [cik, ticker, timestamp, error_type, message, start_date_obj, end_date_obj]
        )
    except Exception as e:
        logging.error(f"CRITICAL: Failed to insert error record into {ERROR_TABLE_NAME} for CIK {cik}/Ticker {ticker}: {e}", exc_info=True)


def load_stock_data_batch(con: duckdb.DuckDBPyConnection, stock_dfs: List[pd.DataFrame], insert_mode: str = "REPLACE"):
    """
    Loads a batch of fetched stock data DataFrames into the DuckDB table.
    Handles different insertion modes.
    """
    if not stock_dfs:
        logging.info("No stock dataframes in batch to load.")
        return True

    try:
        full_batch_df = pd.concat(stock_dfs, ignore_index=True)
    except Exception as concat_e:
         logging.error(f"Error concatenating DataFrames for batch load: {concat_e}", exc_info=True)
         # Log error for tickers in the batch
         for df in stock_dfs:
              if not df.empty:
                   log_fetch_error(con, None, df['ticker'].iloc[0], 'Load Error', f'Failed during pd.concat: {concat_e}')
         return False # Indicate failure

    if full_batch_df.empty:
        logging.warning("Concatenated batch DataFrame is empty, skipping database load.")
        return True

    logging.info(f"Attempting to load batch of {len(full_batch_df)} stock records ({len(stock_dfs)} tickers) into '{STOCK_TABLE_NAME}' using mode '{insert_mode}'...")
    try:
        con.register('stock_batch_df_reg', full_batch_df)

        if insert_mode.upper() == "REPLACE":
             sql = f"INSERT OR REPLACE INTO {STOCK_TABLE_NAME} SELECT * FROM stock_batch_df_reg"
        elif insert_mode.upper() == "IGNORE":
             sql = f"INSERT OR IGNORE INTO {STOCK_TABLE_NAME} SELECT * FROM stock_batch_df_reg"
        else:
             sql = f"INSERT INTO {STOCK_TABLE_NAME} SELECT * FROM stock_batch_df_reg"

        con.execute(sql)
        con.unregister('stock_batch_df_reg')
        logging.info(f"Successfully loaded/updated batch of stock records.")
        return True
    except Exception as e:
        logging.error(f"Database error during batch stock data insertion: {e}", exc_info=True)
        try: con.unregister('stock_batch_df_reg')
        except: pass
         # Log error for tickers in the batch
        for ticker in full_batch_df['ticker'].unique():
             log_fetch_error(con, None, ticker, 'Load Error', f'Batch insert failed: {e}')
        return False
# --- End: DB Functions ---

# --- Stock Data Fetching ---
def fetch_stock_history(ticker: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Fetches historical stock data using yfinance."""
    logging.debug(f"Fetching stock data for {ticker} from {start_date} to {end_date}")
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

        if start_dt > end_dt:
             warning_msg = f"Start date {start_date} is after end date {end_date} for {ticker}. Skipping fetch."
             logging.warning(warning_msg)
             return None, warning_msg

        stock = yf.Ticker(ticker)
        # yfinance end date is exclusive, add 1 day to include the end_date
        history = stock.history(start=start_date, end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'), interval="1d")

        if history.empty:
            warning_msg = f"No data returned by yfinance for {ticker} in range {start_date} to {end_date}."
            logging.warning(warning_msg)
            return None, warning_msg

        history.reset_index(inplace=True)
        history['ticker'] = ticker
        history.rename(columns={
            'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
        }, inplace=True)

        history = history[pd.to_datetime(history['date'], errors='coerce').notna()]
        if history.empty: return None, "Date parsing removed all rows." # Handle case where dates become invalid
        history['date'] = pd.to_datetime(history['date']).dt.date

        db_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        for col in db_columns:
            if col not in history.columns: history[col] = pd.NA
        history['volume'] = pd.to_numeric(history['volume'], errors='coerce').astype('Int64')
        history = history[history['date'].notna()]

        # Filter history >= start_dt and <= end_dt (inclusive)
        history = history[(history['date'] >= start_dt) & (history['date'] <= end_dt)]

        if history.empty: # Check again after date filtering
             warning_msg = f"Data returned by yfinance for {ticker} was outside requested range {start_date} to {end_date}."
             logging.warning(warning_msg)
             return None, warning_msg

        return history[db_columns], None

    except Exception as e:
        error_msg = f"yfinance fetch failed for {ticker}: {type(e).__name__} - {e}"
        logging.error(error_msg, exc_info=False)
        return None, error_msg
# --- End: Stock Data Fetching ---


# --- Main Pipeline Function ---
def run_stock_data_pipeline(
    mode: str = 'initial_load',
    batch_size: int = DEFAULT_BATCH_SIZE,
    append_start_date: Optional[str] = None,
    target_tickers: Optional[List[str]] = None,
    db_file_path: Path = DEFAULT_DB_FILE
):
    """
    Main function to run the stock data gathering pipeline. Handles multiple tickers per CIK.

    Args:
        mode: Operation mode ('initial_load', 'append', 'full_refresh', 'drop_tables').
        batch_size: Number of *tickers* to process before committing a batch load.
        append_start_date: Optional start date (YYYY-MM-DD) for 'append' mode.
        target_tickers: Optional list of specific tickers to process.
        db_file_path: Path to the DuckDB database file.
    """
    start_run_time = time.time()
    logging.info(f"--- Starting Stock Data Pipeline (v7) ---")
    logging.info(f"Database: {db_file_path}")
    logging.info(f"Run Mode: {mode}")
    if target_tickers: logging.info(f"Target Tickers: {target_tickers}")
    if mode == 'append' and append_start_date: logging.info(f"Append Start Date Override: {append_start_date}")
    logging.info(f"Batch Size (Tickers): {batch_size}")

    valid_modes = ['initial_load', 'append', 'full_refresh', 'drop_tables']
    if mode not in valid_modes:
        logging.error(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")
        return

    write_conn = connect_db(db_file_path, read_only=False)
    if not write_conn:
        logging.error(f"Failed to connect to database: {db_file_path}")
        return

    # --- Mode Handling: Drop or Full Refresh Actions ---
    try:
        if mode == 'drop_tables':
            write_conn.begin()
            drop_stock_tables(write_conn)
            write_conn.commit()
            logging.info("Drop tables operation finished.")
            write_conn.close()
            return

        write_conn.begin()
        setup_tables(write_conn)
        write_conn.commit()

        if mode == 'full_refresh':
            write_conn.begin()
            clear_stock_tables(write_conn)
            write_conn.commit()
            logging.info("Full refresh: Cleared existing stock and error data.")

    except Exception as setup_e:
         logging.error(f"Failed during table setup/clear: {setup_e}. Exiting.", exc_info=True)
         try: write_conn.rollback()
         except: pass
         write_conn.close()
         return

    # --- Initialize CIK Mapper (only if needed) ---
    mapper = None
    target_tickers_upper = [t.upper() for t in target_tickers] if target_tickers else None
    if not target_tickers_upper:
         try:
             logging.info("Initializing StockMapper...")
             mapper = StockMapper()
             logging.info("StockMapper initialized successfully.")
         except Exception as e:
             logging.error(f"Could not initialize StockMapper: {e}. Cannot map CIKs.", exc_info=True)
             write_conn.close()
             return

    # --- Identify Tickers and CIK/Date Ranges ---
    # Structure: {ticker: {'cik': cik, 'min_date': min_d, 'max_date': max_d}}
    tickers_to_process_map: Dict[str, Dict] = {}
    try:
        if target_tickers_upper:
            logging.info(f"Getting date ranges for {len(target_tickers_upper)} target tickers...")
            for ticker in tqdm(target_tickers_upper, desc="Getting Ticker Dates"):
                range_info = get_ticker_date_range(write_conn, ticker)
                if range_info:
                    cik, min_d, max_d = range_info
                    tickers_to_process_map[ticker] = {'cik': cik, 'min_date': min_d, 'max_date': max_d}
                else:
                    log_fetch_error(write_conn, None, ticker, 'Missing Info', 'Could not find CIK/date range for target ticker.')
        elif mapper: # Process all CIKs from DB
            companies_data_df = get_companies_and_xbrl_dates(write_conn)
            if not companies_data_df.empty:
                logging.info(f"Mapping CIKs to tickers for {len(companies_data_df)} companies...")
                for _, row in tqdm(companies_data_df.iterrows(), total=len(companies_data_df), desc="Mapping CIKs"):
                    cik = row['cik']
                    min_date = row['min_date']
                    max_date = row['max_date']
                    if pd.isna(min_date) or pd.isna(max_date): continue # Skip if dates are invalid

                    try:
                        tickers_set: Set[str] = mapper.cik_to_tickers.get(cik, set())
                        if not tickers_set:
                             log_fetch_error(write_conn, cik, None, 'No Ticker Found', 'sec-cik-mapper returned no tickers.')
                             continue

                        for ticker in tickers_set:
                             if ticker not in tickers_to_process_map: # Add if not already processed via another CIK mapping
                                 tickers_to_process_map[ticker] = {'cik': cik, 'min_date': min_date, 'max_date': max_date}
                             else:
                                 # Handle cases where a ticker might map from multiple CIKs (rare, but possible)
                                 # Maybe update date range to be the widest? For now, just log.
                                 logging.debug(f"Ticker {ticker} already mapped from CIK {tickers_to_process_map[ticker]['cik']}. Ignoring duplicate mapping from CIK {cik}.")
                    except Exception as map_e:
                        log_fetch_error(write_conn, cik, None, 'Mapper Error', f"CIK lookup failed: {map_e}")
            else:
                 logging.warning("Company/XBRL date query returned no results.")

    except Exception as e:
        logging.error(f"Error during company/ticker identification: {e}", exc_info=True)
        write_conn.close()
        return

    if not tickers_to_process_map:
        logging.warning("No tickers identified for processing based on criteria.")
        write_conn.close()
        return

    # --- Get Latest Dates for Append Mode ---
    latest_dates = {}
    if mode == 'append':
        target_tickers_for_latest_date = list(tickers_to_process_map.keys())
        latest_dates = get_latest_stock_dates(write_conn, target_tickers=target_tickers_for_latest_date)

    # --- Iterate through identified TICKERS, Fetch, Batch Load ---
    total_tickers_to_process = len(tickers_to_process_map)
    processed_tickers_count = 0 # Tickers with successful load
    skipped_tickers_count = 0   # Tickers skipped before fetch or failed fetch/load
    fetch_errors_count = 0
    load_errors_count = 0
    batch_data_dfs: List[pd.DataFrame] = []
    processed_tickers_in_batch = 0

    logging.info(f"Starting fetch and load loop for {total_tickers_to_process} unique tickers...")
    ticker_items = list(tickers_to_process_map.items())

    for i, (ticker, info) in enumerate(tqdm(ticker_items, desc="Fetching Stock Data", unit="ticker")):
        cik = info['cik']
        min_date_overall = info['min_date']
        max_date_overall = info['max_date']
        fetch_start_date = None
        fetch_end_date = datetime.now(timezone.utc).date() # Default end to today

        logging.debug(f"Processing {i + 1}/{total_tickers_to_process}: Ticker {ticker} (from CIK {cik})")

        # Determine fetch date range
        if mode == 'append':
            last_date = latest_dates.get(ticker)
            start_date_override = None
            if append_start_date:
                try: start_date_override = datetime.strptime(append_start_date, '%Y-%m-%d').date()
                except ValueError: logging.error(f"Invalid append_start_date format: {append_start_date}. Ignored.")

            if start_date_override: fetch_start_date = start_date_override
            elif last_date: fetch_start_date = last_date + timedelta(days=1)
            else: fetch_start_date = min_date_overall
            # fetch_end_date already set to today

        else: # initial_load or full_refresh
            fetch_start_date = min_date_overall
            fetch_end_date = max_date_overall # Use XBRL range

        if not fetch_start_date or not fetch_end_date:
            log_fetch_error(write_conn, cik, ticker, 'Date Error', 'Could not determine valid start/end fetch dates.')
            skipped_tickers_count += 1; continue
        if fetch_start_date > fetch_end_date:
             log_fetch_error(write_conn, cik, ticker, 'Date Error', f'Start date {fetch_start_date} > end date {fetch_end_date}.', fetch_start_date.strftime('%Y-%m-%d'), fetch_end_date.strftime('%Y-%m-%d'))
             skipped_tickers_count += 1; continue

        fetch_start_date_str = fetch_start_date.strftime('%Y-%m-%d')
        fetch_end_date_str = fetch_end_date.strftime('%Y-%m-%d')

        # Fetch Data
        stock_history_df, fetch_error_msg = fetch_stock_history(ticker, fetch_start_date_str, fetch_end_date_str)

        if stock_history_df is None:
            fetch_errors_count += 1
            log_fetch_error(write_conn, cik, ticker, 'Fetch Error', fetch_error_msg or "Unknown yfinance error", fetch_start_date_str, fetch_end_date_str)
            skipped_tickers_count += 1
        elif not stock_history_df.empty:
            batch_data_dfs.append(stock_history_df)
            processed_tickers_in_batch += 1 # Count tickers added to batch
            # Don't increment processed_tickers_count until batch load succeeds
        else:
            logging.info(f"No stock data returned/processed for {ticker} in range {fetch_start_date_str} to {fetch_end_date_str}.")
            skipped_tickers_count += 1 # Count as skipped if no data loaded


        # Load Batch Periodically
        is_last_item = (i + 1 == total_tickers_to_process)
        if batch_data_dfs and (processed_tickers_in_batch >= batch_size or is_last_item):
             logging.info(f"Reached batch size ({processed_tickers_in_batch} tickers) or end of list. Loading batch to DB...")
             insert_mode = "REPLACE" if mode != 'full_refresh' else "INSERT"
             load_successful = False
             try:
                  write_conn.begin()
                  load_successful = load_stock_data_batch(write_conn, batch_data_dfs, insert_mode=insert_mode)
                  if load_successful:
                       write_conn.commit()
                       processed_tickers_count += processed_tickers_in_batch # Increment total success count
                  else:
                       write_conn.rollback()
                       load_errors_count += processed_tickers_in_batch
                       # Error logged by load_stock_data_batch
             except Exception as batch_load_e:
                  logging.error(f"Unexpected error during batch load transaction: {batch_load_e}", exc_info=True)
                  try: write_conn.rollback()
                  except: pass
                  load_errors_count += processed_tickers_in_batch
                  log_fetch_error(write_conn, None, f"Batch starting ~{batch_data_dfs[0]['ticker'].iloc[0]}", 'Load Error', f"Unexpected batch load error: {batch_load_e}")
             finally:
                  if not load_successful:
                       # If load failed, these tickers were skipped in terms of successful processing
                       skipped_tickers_count += processed_tickers_in_batch
                  batch_data_dfs = [] # Clear batch
                  processed_tickers_in_batch = 0

        # Optional Delay
        if YFINANCE_DELAY > 0 and not is_last_item:
             time.sleep(YFINANCE_DELAY)

    # --- Final Cleanup & Summary ---
    if write_conn:
        try: write_conn.commit() # Commit any final pending error logs
        except Exception: pass
        try: write_conn.close()
        except Exception: pass
        logging.info("Write database connection closed.")

    end_run_time = time.time()
    logging.info(f"--- Stock Data Pipeline Finished ---")
    logging.info(f"Total Time: {end_run_time - start_run_time:.2f} seconds")
    logging.info(f"Total Unique Tickers Identified: {total_tickers_to_process}")
    logging.info(f"Tickers Successfully Loaded/Updated: {processed_tickers_count}")
    logging.info(f"Tickers Skipped or Errored (No Data Loaded): {skipped_tickers_count + fetch_errors_count}") # Combined count
    logging.info(f"Fetch Errors (logged): {fetch_errors_count}")
    logging.info(f"Load Errors (logged): {load_errors_count}")
    logging.info(f"Check '{ERROR_TABLE_NAME}' table for details on errors.")


# --- Script Execution Control ---
if __name__ == "__main__":
    # --- SET YOUR DESIRED PARAMETERS HERE FOR DIRECT EXECUTION / DEBUGGING ---

    # Example 1: Initial load for all companies, default batch size
    # run_stock_data_pipeline(mode='initial_load', batch_size=DEFAULT_BATCH_SIZE)

    # Example 2: Append data for specific tickers since a certain date
    # run_stock_data_pipeline(
    #     mode='append',
    #     target_tickers=['AAPL', 'MSFT', 'JACS'], # JACS will likely fetch multiple
    #     append_start_date='2024-04-15', # Optional override
    #     batch_size=10
    # )

    # Example 3: Full refresh for all companies (use with caution!)
    # run_stock_data_pipeline(mode='full_refresh')

    # Example 4: Drop the tables
    # run_stock_data_pipeline(mode='drop_tables')

    # Default action when run directly: Append from last known date for all tickers
    run_stock_data_pipeline(
        mode='append',
        batch_size=DEFAULT_BATCH_SIZE,
        db_file_path=DEFAULT_DB_FILE
    )

    logging.info("Script execution finished.")