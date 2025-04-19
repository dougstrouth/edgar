# -*- coding: utf-8 -*-
"""
Stock Info Gatherer Script (v3.3 - Long Financial Format)

Refines financial facts processing: assigns index name first, then melts,
then drops rows ONLY if essential PK components (ticker, report_date,
frequency, item_label) are NULL. Keeps rows with NULL values.
"""

import duckdb
import pandas as pd
import yfinance as yf
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
import sys
import time
from datetime import datetime, date, timezone, timedelta
from tqdm import tqdm
from typing import Optional, Dict, List, Tuple, Any
import numpy as np
import io
import re

# --- Configuration and Logging Setup ---
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'
if not dotenv_path.is_file(): dotenv_path = script_dir.parent / '.env'
if dotenv_path.is_file(): load_dotenv(dotenv_path=dotenv_path)
else: print(f"Warning: .env file not found.", file=sys.stderr)

try: DEFAULT_DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError: DEFAULT_DB_FILE = script_dir.parent / "edgar_metadata.duckdb"; print(f"Warning: DB_FILE defaulting to {DEFAULT_DB_FILE}", file=sys.stderr)

log_file_path = script_dir / "stock_info_gatherer.log"
log_file_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[ logging.FileHandler(log_file_path), logging.StreamHandler()]
)

# --- Constants ---
PROFILE_TABLE_NAME = "company_profile"
METRICS_TABLE_NAME = "company_key_metrics"
ACTIONS_TABLE_NAME = "stock_actions"
FIN_FACTS_TABLE_NAME = "yfinance_financial_facts"
ERROR_TABLE_NAME = "stock_fetch_errors"
YFINANCE_DELAY = 0.6

# --- Database Functions (Unchanged from v3.1) ---
def connect_db(db_path_str: str, read_only: bool = True) -> Optional[duckdb.DuckDBPyConnection]:
    is_memory = (db_path_str == ':memory:')
    db_path = None
    if not is_memory:
        db_path = Path(db_path_str)
        if not db_path.exists(): logging.warning(f"DB path not found: {db_path_str}. Create attempt."); db_path.parent.mkdir(parents=True, exist_ok=True)
    try: conn = duckdb.connect(database=db_path_str, read_only=read_only); logging.info(f"Connected: {db_path_str} (RO: {read_only})"); return conn
    except Exception as e: logging.error(f"Failed connect {db_path_str}: {e}", exc_info=True); return None

def setup_info_tables(con: duckdb.DuckDBPyConnection):
    # Schema unchanged from v3.1
    logging.info("Setting up supplementary tables (long format financials)...")
    tables_sql = {
        PROFILE_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {PROFILE_TABLE_NAME} (ticker VARCHAR PRIMARY KEY COLLATE NOCASE, cik VARCHAR, fetch_timestamp TIMESTAMPTZ NOT NULL, quoteType VARCHAR, exchange VARCHAR, longName VARCHAR, sector VARCHAR, industry VARCHAR, longBusinessSummary TEXT, fullTimeEmployees INTEGER, website VARCHAR, beta DOUBLE, exchangeTimezoneName VARCHAR, exchangeTimezoneShortName VARCHAR, auditRisk INTEGER, boardRisk INTEGER, compensationRisk INTEGER, shareHolderRightsRisk INTEGER, overallRisk INTEGER, governanceEpochDate BIGINT, compensationAsOfEpochDate BIGINT);""",
        METRICS_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {METRICS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, fetch_date DATE NOT NULL, marketCap BIGINT, enterpriseValue BIGINT, trailingPE DOUBLE, forwardPE DOUBLE, priceToBook DOUBLE, priceToSalesTrailing12Months DOUBLE, enterpriseToRevenue DOUBLE, enterpriseToEbitda DOUBLE, dividendRate DOUBLE, dividendYield DOUBLE, beta DOUBLE, fiftyTwoWeekHigh DOUBLE, fiftyTwoWeekLow DOUBLE, sharesOutstanding BIGINT, floatShares BIGINT, bookValue DOUBLE, regularMarketPrice DOUBLE, previousClose DOUBLE, volume BIGINT, averageVolume BIGINT, averageVolume10days BIGINT, exDividendDate DATE, PRIMARY KEY (ticker, fetch_date));""",
        FIN_FACTS_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_FACTS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, frequency VARCHAR NOT NULL, statement_type VARCHAR NOT NULL, item_label VARCHAR NOT NULL, value DOUBLE PRECISION, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, report_date, frequency, item_label));""",
        ACTIONS_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {ACTIONS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, action_date DATE NOT NULL, action_type VARCHAR NOT NULL, value DOUBLE NOT NULL, PRIMARY KEY (ticker, action_date, action_type));""",
        ERROR_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);"""}
    try:
        for name, sql in tables_sql.items(): con.execute(sql); logging.debug(f"Table '{name}' setup ok.")
    except Exception as e: logging.error(f"Failed setup_info_tables: {e}", exc_info=True); raise

def drop_info_tables(con: duckdb.DuckDBPyConnection):
    tables_to_drop = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_FACTS_TABLE_NAME]
    logging.warning(f"Dropping tables: {', '.join(tables_to_drop)}")
    try: [con.execute(f"DROP TABLE IF EXISTS {t};") for t in tables_to_drop]
    except Exception as e: logging.error(f"Failed drop: {e}", exc_info=True); raise

def clear_info_tables(con: duckdb.DuckDBPyConnection):
    tables_to_clear = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_FACTS_TABLE_NAME]
    logging.warning(f"Clearing data: {', '.join(tables_to_clear)}")
    try:
        db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        for t in tables_to_clear:
            if t.lower() in db_tables: con.execute(f"DELETE FROM {t};"); logging.info(f"Cleared {t}.")
            else: logging.warning(f"Table {t} not found.")
    except Exception as e: logging.error(f"Failed clear: {e}", exc_info=True); raise

def log_info_fetch_error(con: duckdb.DuckDBPyConnection, cik: Optional[str], ticker: str, error_type: str, message: str):
    timestamp = datetime.now(timezone.utc)
    logging.warning(f"Log err: CIK={cik}, Ticker={ticker}, Type={error_type}, Msg={message}")
    try: con.execute(f"INSERT INTO {ERROR_TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?)", [cik, ticker, timestamp, error_type, message, None, None])
    except Exception as e:
        if "TransactionContext Error" in str(e): logging.error(f"Err log fail (TX Abort) Ticker {ticker}: {e}")
        else: logging.error(f"CRITICAL insert err log CIK {cik}/Ticker {ticker}: {e}", exc_info=True)

def get_db_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    try: cols = con.execute(f"PRAGMA table_info('{table_name}');").fetchall(); return [col[1] for col in cols]
    except duckdb.duckdb.TransactionException as te: raise
    except Exception as e: logging.error(f"Failed get cols {table_name}: {e}", exc_info=True); return []

def load_generic_data(con: duckdb.DuckDBPyConnection, data_df: pd.DataFrame, table_name: str, pk_cols: List[str], mode: str) -> bool:
    if data_df is None or data_df.empty: logging.debug(f"DF {table_name} empty."); return True
    ticker = data_df['ticker'].iloc[0] if 'ticker' in data_df.columns else 'N/A'
    logging.info(f"Loading {len(data_df)} rows into {table_name} for {ticker} (Mode: {mode})")
    db_cols = []; load_df = pd.DataFrame()
    try: db_cols = get_db_columns(con, table_name)
    except duckdb.duckdb.TransactionException: raise
    except Exception: logging.error(f"Load fail {ticker} {table_name}: No DB cols."); return False
    if not db_cols: logging.error(f"Load fail {ticker} {table_name}: No DB cols."); return False
    try:
        cols_in_df = list(data_df.columns); cols_in_df_lower = {c.lower() for c in cols_in_df}
        db_cols_lower_map = {c.lower(): c for c in db_cols}; cols_to_load_db_case = []
        extra_df_cols = []
        for col_df in cols_in_df:
            if col_df.lower() in db_cols_lower_map: cols_to_load_db_case.append(db_cols_lower_map[col_df.lower()])
            else: extra_df_cols.append(col_df)
        missing_db_cols_in_df = [db_col for db_col_lower, db_col in db_cols_lower_map.items() if db_col_lower not in cols_in_df_lower]
        if extra_df_cols: logging.warning(f"DF {table_name} ({ticker}) extra cols: {extra_df_cols}. Ignored.")
        rename_map = {col_df: db_cols_lower_map[col_df.lower()] for col_df in cols_in_df if col_df.lower() in db_cols_lower_map}
        load_df = data_df.rename(columns=rename_map)[cols_to_load_db_case].copy()
        if missing_db_cols_in_df:
            logging.warning(f"Cols missing DF->{table_name} ({ticker}): {missing_db_cols_in_df}. NULL.")
            for col in missing_db_cols_in_df: load_df[col] = None
        load_df = load_df[db_cols]; load_df = load_df.astype(object).where(pd.notnull(load_df), None)
        for pk_col in pk_cols:
            if pk_col not in load_df.columns or load_df[pk_col].isnull().any(): logging.error(f"Load fail {ticker} {table_name}: PK '{pk_col}' NULL."); return False
    except Exception as df_prep_e: logging.error(f"Error prep DF {table_name} ({ticker}): {df_prep_e}", exc_info=True); return False
    reg_name = f'{table_name}_reg_{int(time.time()*1000)}'
    try:
        con.register(reg_name, load_df); cols_str = ", ".join([f'"{c}"' for c in db_cols])
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}" if mode != 'full_refresh' else f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}"
        con.execute(sql); con.unregister(reg_name); logging.debug(f"Loaded {table_name} for {ticker}."); return True
    except Exception as e:
        logging.error(f"DB error loading {table_name} for {ticker}: {e}", exc_info=False)
        try: con.unregister(reg_name)
        except: pass; return False

# --- Data Fetching Functions (Unchanged from v3.1) ---
def fetch_company_info(ticker_obj: yf.Ticker) -> Optional[Dict[str, Any]]:
    try:
        info = ticker_obj.info
        if not info or not isinstance(info, dict): return None
        cleaned_info = {}
        for key, value in info.items():
            if isinstance(value, (list, dict)): logging.debug(f"Skip complex type key '{key}'"); cleaned_info[key] = None; continue
            if isinstance(value, np.ndarray):
                 if value.size == 1: value = value.item()
                 else: logging.debug(f"Skip array key '{key}'"); cleaned_info[key] = None; continue
            if pd.isna(value): cleaned_info[key] = None
            elif isinstance(value, (np.int64, np.int32, np.int16)): cleaned_info[key] = int(value)
            elif isinstance(value, (np.float64, np.float32)): cleaned_info[key] = float(value)
            elif isinstance(value, np.bool_): cleaned_info[key] = bool(value)
            else: cleaned_info[key] = value
        return cleaned_info
    except Exception as e: logging.error(f"Failed processing .info {ticker_obj.ticker}: {type(e).__name__}-{e}", exc_info=False); return None

def fetch_financial_statement(ticker_obj: yf.Ticker, statement_type: str, frequency: str) -> Optional[pd.DataFrame]:
    """Fetches RAW financials, balance sheet, or cash flow. Returns df with dates as cols."""
    logging.debug(f"Fetching raw {frequency} {statement_type} for {ticker_obj.ticker}")
    data = None
    try:
        method_map = { ('financials', 'quarterly'): ticker_obj.quarterly_financials, ('financials', 'annual'): ticker_obj.financials, ('balance_sheet', 'quarterly'): ticker_obj.quarterly_balance_sheet, ('balance_sheet', 'annual'): ticker_obj.balance_sheet, ('cashflow', 'quarterly'): ticker_obj.quarterly_cashflow, ('cashflow', 'annual'): ticker_obj.cashflow, }
        data = method_map.get((statement_type, frequency))
        if data is None or data.empty: logging.info(f"No raw {frequency} {statement_type} data found {ticker_obj.ticker}."); return None
        return data
    except Exception as e: logging.error(f"Failed fetch raw {frequency} {statement_type} {ticker_obj.ticker}: {type(e).__name__}-{e}", exc_info=False); return None

def fetch_stock_actions(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    logging.debug(f"Fetching actions for {ticker_obj.ticker}")
    try:
        actions = ticker_obj.actions
        if actions is None: logging.info(f"No actions (None) {ticker_obj.ticker}."); return None
        if actions.empty: logging.info(f"No actions (empty df) {ticker_obj.ticker}."); return None
        actions.index.name = 'action_date'; actions.reset_index(inplace=True)
        actions['ticker'] = ticker_obj.ticker; actions['action_date'] = pd.to_datetime(actions['action_date']).dt.date
        actions_melted = actions.melt(id_vars=['ticker', 'action_date'], value_vars=['Dividends', 'Stock Splits'], var_name='action_type', value_name='value')
        actions_melted = actions_melted[actions_melted['value'] != 0.0]; actions_melted.dropna(subset=['value'], inplace=True)
        actions_melted['action_type'] = actions_melted['action_type'].replace({'Stock Splits': 'Split'})
        return actions_melted[['ticker', 'action_date', 'action_type', 'value']]
    except Exception as e: logging.error(f"Failed actions {ticker_obj.ticker}: {type(e).__name__} - {e}", exc_info=False); return None

# --- Financial Fact Processing and Loading (MODIFIED) ---
def process_and_load_financial_facts(
    con: duckdb.DuckDBPyConnection,
    raw_df: pd.DataFrame,
    ticker: str,
    statement_type: str,
    frequency: str,
    mode: str
    ) -> bool:
    """Transforms raw financial DataFrame to long format and loads, keeping NULL values."""
    if raw_df is None or raw_df.empty:
        logging.debug(f"Raw DF empty {ticker} {frequency} {statement_type}. Skip process/load.")
        return True

    logging.info(f"Processing {frequency} {statement_type} for {ticker} ({len(raw_df)} items)")
    table_name = FIN_FACTS_TABLE_NAME
    pk_cols = ['ticker', 'report_date', 'frequency', 'item_label']

    try:
        # 1. Assign index name FIRST
        raw_df.index.name = 'item_label' # Use the final desired name directly

        # 2. Melt DataFrame (Dates as columns -> Date rows)
        # Make sure index name is set before resetting
        if raw_df.index.name is None: raw_df.index.name = 'item_label'
        long_df = raw_df.reset_index().melt(
            id_vars=['item_label'], var_name='report_date', value_name='value'
            )

        # 3. Clean and Convert Data Types
        long_df['item_label'] = long_df['item_label'].astype(str).str.strip() # Ensure label is string, strip whitespace
        long_df['report_date'] = pd.to_datetime(long_df['report_date'], errors='coerce').dt.date
        long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce') # Keep potentially resulting NaN values

        # 4. Add Metadata Columns BEFORE dropping NAs
        long_df['ticker'] = ticker
        long_df['frequency'] = frequency
        long_df['statement_type'] = statement_type
        long_df['fetch_timestamp'] = datetime.now(timezone.utc)

        # *** MODIFICATION: Only drop rows if PK components are missing ***
        # Ensure PK columns are not None/NaT before dropping
        long_df.dropna(subset=['ticker', 'report_date', 'frequency', 'item_label'], inplace=True)
        # Keep rows where 'value' might be NaN/None

        if long_df.empty:
            logging.warning(f"DataFrame became empty after melting/PK cleaning for {ticker} {frequency} {statement_type}.")
            return True # Not an error if all rows had invalid PK components

        # 5. Prepare for Loading (Select DB columns)
        db_cols = get_db_columns(con, table_name)
        if not db_cols: logging.error(f"Cannot load {table_name}, get cols failed."); return False
        # Select columns present in DB schema (case-insensitive check just in case)
        final_df = long_df[[col for col in db_cols if col.lower() in map(str.lower, long_df.columns)]].copy()
        # Ensure final column names match DB schema (case-sensitive for some DBs, DuckDB often lenient but good practice)
        final_df.columns = [col for col in db_cols if col.lower() in map(str.lower, long_df.columns)]
        # Convert Pandas NA/NaT to None
        final_df = final_df.astype(object).where(pd.notnull(final_df), None)

        # Final check for PK nulls (more robust)
        if final_df[pk_cols].isnull().values.any(): # Check underlying numpy array for nulls
            logging.error(f"Load fail {ticker} {table_name}: PK cols contain NULL after processing.")
            logging.debug(f"Problematic final_df PKs:\n {final_df[final_df[pk_cols].isnull().any(axis=1)][pk_cols]}")
            return False

        # 6. Load using INSERT OR REPLACE
        reg_name = f'{table_name}_reg_{int(time.time()*1000)}'
        con.register(reg_name, final_df)
        cols_str = ", ".join([f'"{c}"' for c in db_cols]) # Quote column names
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}" if mode != 'full_refresh' else f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}"
        con.execute(sql)
        con.unregister(reg_name)
        logging.debug(f"Loaded {len(final_df)} facts into {table_name} for {ticker} {frequency} {statement_type}.")
        return True

    except Exception as e:
        logging.error(f"Failed process/load {frequency} {statement_type} for {ticker}: {e}", exc_info=True)
        return False

# --- Main Pipeline Function ---
def run_stock_info_pipeline(
    mode: str = 'initial_load',
    target_tickers: Optional[List[str]] = None,
    db_file_path_str: str = str(DEFAULT_DB_FILE),
    update_profile_if_older_than_days: int = 30
):
    # (Main loop structure unchanged, relies on process_and_load_financial_facts return)
    start_run_time = time.time()
    logging.info(f"--- Starting Stock Info Pipeline (v3.2 - Long Format - Keep Nulls Fix) ---")
    logging.info(f"Database: {db_file_path_str}")
    logging.info(f"Run Mode: {mode}")
    if target_tickers: logging.info(f"Target Tickers: {target_tickers}")
    logging.info(f"Update Profile Threshold (Days): {update_profile_if_older_than_days}")

    valid_modes=['initial_load','append','full_refresh','drop_tables']
    if mode not in valid_modes: logging.error(f"Invalid mode '{mode}'."); return
    is_memory_db = (db_file_path_str == ':memory:')
    if is_memory_db and not target_tickers: logging.error("Must provide target_tickers when using ':memory:'."); return

    write_conn = connect_db(db_file_path_str, read_only=False)
    if not write_conn: logging.error(f"Failed connect DB."); return

    # Mode Handling & Setup
    try:
        if mode == 'drop_tables': write_conn.begin(); drop_info_tables(write_conn); write_conn.commit(); logging.info("Drop finished."); write_conn.close(); return
        write_conn.begin(); setup_info_tables(write_conn); write_conn.commit()
        if mode == 'full_refresh': write_conn.begin(); clear_info_tables(write_conn); write_conn.commit(); logging.info("Full refresh clear done.")
    except Exception as setup_e:
         logging.error(f"Failed setup/clear: {setup_e}. Exiting.", exc_info=True)
         try:
             if write_conn and not write_conn.closed: write_conn.rollback(); logging.warning("Rolled back setup tx.")
         except Exception as rb_e: logging.error(f"Rollback attempt failed: {rb_e}")
         finally:
             if write_conn and not write_conn.closed: write_conn.close()
         return

    # Identify Tickers
    tickers_to_process = []
    if target_tickers: tickers_to_process = [t.upper() for t in target_tickers]; logging.info(f"Processing {len(tickers_to_process)} target tickers.")
    else:
        source_table = "stock_history"; count = 0
        try: count = write_conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
        except: logging.warning(f"{source_table} not found/err, trying 'tickers'.")
        if count == 0: source_table = "tickers"
        try: ticker_rows = write_conn.execute(f"SELECT DISTINCT ticker FROM {source_table} WHERE ticker IS NOT NULL ORDER BY ticker").fetchall(); tickers_to_process = [row[0] for row in ticker_rows]; logging.info(f"Found {len(tickers_to_process)} unique tickers in '{source_table}'.")
        except Exception as e: logging.error(f"Failed query tickers: {e}. Need target_tickers.", exc_info=True); write_conn.close(); return
    if not tickers_to_process: logging.warning("No tickers identified."); write_conn.close(); return

    # Get profile timestamps
    profile_timestamps = {};
    try: results = write_conn.execute(f"SELECT ticker, fetch_timestamp FROM {PROFILE_TABLE_NAME}").fetchall(); profile_timestamps = {t: ts for t, ts in results}; logging.info(f"Found {len(profile_timestamps)} profile timestamps.")
    except Exception as e: logging.warning(f"Could not query profile timestamps: {e}")

    # Iterate, Fetch, Load
    total_tickers_to_process = len(tickers_to_process)
    processed_tickers_count = 0; skipped_tickers_count = 0
    fetch_errors_count = 0; load_errors_count = 0

    logging.info(f"Starting fetch/load loop for {total_tickers_to_process} tickers...")
    for i, ticker in enumerate(tqdm(tickers_to_process, desc="Gathering Stock Info", unit="ticker")):
        logging.debug(f"Processing {i + 1}/{total_tickers_to_process}: Ticker {ticker}")
        ticker_fetch_success = True; load_failed_flag = False
        profile_df, metrics_df, actions_df = None, None, None
        raw_fin_dfs = {}

        try:
            ticker_obj = yf.Ticker(ticker)
            # Fetch Profile & Metrics
            needs_profile_fetch = True; last_fetch_ts = profile_timestamps.get(ticker)
            if last_fetch_ts and update_profile_if_older_than_days >= 0 and (datetime.now(timezone.utc) - last_fetch_ts) < timedelta(days=update_profile_if_older_than_days): needs_profile_fetch = False; logging.debug(f"Skip profile fetch {ticker}.")
            if needs_profile_fetch:
                info_dict = fetch_company_info(ticker_obj)
                if info_dict is None: fetch_errors_count += 1; ticker_fetch_success = False; log_info_fetch_error(write_conn, None, ticker, 'Info Fetch Error', 'fetch_company_info returned None')
                else:
                    try: # Prepare DFs
                        db_prof_cols = get_db_columns(write_conn, PROFILE_TABLE_NAME); prof_data = {k: info_dict.get(k) for k in db_prof_cols if k not in ['fetch_timestamp', 'cik']}; prof_data['ticker'] = ticker; prof_data['fetch_timestamp'] = datetime.now(timezone.utc)
                        try: prof_data['cik'] = write_conn.execute("SELECT cik FROM tickers WHERE ticker=? LIMIT 1", [ticker]).fetchone()[0]
                        except: prof_data['cik'] = None
                        profile_df = pd.DataFrame([prof_data])
                        db_met_cols = get_db_columns(write_conn, METRICS_TABLE_NAME); met_data = {k: info_dict.get(k) for k in db_met_cols if k != 'fetch_date'}; met_data['ticker'] = ticker; met_data['fetch_date'] = date.today()
                        for date_key in ['exDividendDate']:
                            if date_key in met_data and isinstance(met_data[date_key], (int, float)):
                                try: met_data[date_key] = datetime.fromtimestamp(met_data[date_key], timezone.utc).date()
                                except (TypeError, ValueError, OSError) as de: logging.warning(f"Epoch conv fail {date_key}({met_data[date_key]}) {ticker}: {de}"); met_data[date_key] = None
                            elif date_key in met_data and not isinstance(met_data[date_key], (date, type(None))):
                                try: met_data[date_key] = pd.to_datetime(met_data[date_key]).date()
                                except: met_data[date_key] = None
                        metrics_df = pd.DataFrame([met_data])
                    except Exception as prep_e: logging.error(f"Error prep profile/metrics {ticker}: {prep_e}", exc_info=True); fetch_errors_count += 1; ticker_fetch_success = False; log_info_fetch_error(write_conn, None, ticker, 'Data Prep Error', f'Failed prep profile/metrics: {prep_e}'); profile_df, metrics_df = None, None

            # Fetch Financials (Raw)
            fin_map_keys = [ ('Income Statement', 'annual'), ('Income Statement', 'quarterly'), ('Balance Sheet', 'annual'), ('Balance Sheet', 'quarterly'), ('Cash Flow', 'annual'), ('Cash Flow', 'quarterly') ]
            yfinance_stmt_map = {'Income Statement':'financials', 'Balance Sheet':'balance_sheet', 'Cash Flow':'cashflow'}
            for stmt_type, freq in fin_map_keys:
                 yfinance_type = yfinance_stmt_map[stmt_type]
                 raw_df = fetch_financial_statement(ticker_obj, yfinance_type, freq)
                 if raw_df is None: fetch_errors_count += 1 # Count fetch failure
                 raw_fin_dfs[(stmt_type, freq)] = raw_df

            # Fetch Actions
            actions_df = fetch_stock_actions(ticker_obj)
            if actions_df is None: fetch_errors_count += 1

            # Determine overall fetch success
            ticker_fetch_success = (profile_df is not None) or (metrics_df is not None) or (actions_df is not None) or any(df is not None for df in raw_fin_dfs.values())

            # --- Load Phase ---
            if not ticker_fetch_success: logging.warning(f"Skip load {ticker} due to all fetches failing."); skipped_tickers_count += 1; continue

            try:
                write_conn.begin(); load_failed_flag = False
                # Load Profile & Metrics
                if needs_profile_fetch and profile_df is not None and not profile_df.empty:
                    if not load_generic_data(write_conn, profile_df, PROFILE_TABLE_NAME, ['ticker'], mode): load_failed_flag = True
                if needs_profile_fetch and metrics_df is not None and not metrics_df.empty:
                     if not load_generic_data(write_conn, metrics_df, METRICS_TABLE_NAME, ['ticker', 'fetch_date'], mode): load_failed_flag = True

                # Process and Load Financial Facts
                for (stmt_type, freq), raw_df_fin in raw_fin_dfs.items():
                    if raw_df_fin is not None: # Only process if fetch was successful
                        if not process_and_load_financial_facts(write_conn, raw_df_fin, ticker, stmt_type, freq, mode):
                            load_failed_flag = True # Mark failure if processing/loading facts fails

                # Load Actions
                if actions_df is not None and not actions_df.empty:
                    if not load_generic_data(write_conn, actions_df, ACTIONS_TABLE_NAME, ['ticker', 'action_date', 'action_type'], mode): load_failed_flag = True

                # Commit or Rollback
                if not load_failed_flag: write_conn.commit(); processed_tickers_count += 1
                else: logging.warning(f"Rolling back {ticker} due to load errors."); write_conn.rollback(); load_errors_count += 1; skipped_tickers_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Load Failure', 'One or more loads failed.')
            except Exception as load_tx_e:
                 logging.error(f"Error load tx {ticker}: {load_tx_e}", exc_info=True); load_errors_count += 1; skipped_tickers_count += 1
                 try: write_conn.rollback(); logging.info(f"Rolled back {ticker}.")
                 except Exception as rb_e: logging.error(f"Rollback fail {ticker}: {rb_e}")
                 log_info_fetch_error(write_conn, None, ticker, 'Load Transaction Error', str(load_tx_e))
        except Exception as ticker_e: # Catch errors like invalid ticker instantiation
            logging.error(f"Major error {ticker}: {ticker_e}", exc_info=True); fetch_errors_count +=1; skipped_tickers_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Ticker Processing Error', str(ticker_e))
            try: write_conn.rollback() # Ensure no transaction left open
            except: pass
        if YFINANCE_DELAY > 0: time.sleep(YFINANCE_DELAY)

    # Final Cleanup & Summary
    if write_conn:
        try: write_conn.commit()
        except: pass
        try: write_conn.close(); logging.info("DB connection closed.")
        except Exception as close_e: logging.error(f"Error closing DB: {close_e}")
    end_run_time = time.time()
    logging.info(f"--- Stock Info Pipeline Finished ---"); logging.info(f"Total Time: {end_run_time - start_run_time:.2f} sec")
    logging.info(f"Tickers Attempted: {total_tickers_to_process}"); logging.info(f"Tickers OK: {processed_tickers_count}"); logging.info(f"Tickers Skip/Err: {skipped_tickers_count}")
    logging.info(f"Fetch Errors: {fetch_errors_count}"); logging.info(f"Load Errors (Tickers w fail): {load_errors_count}"); logging.info(f"Check '{ERROR_TABLE_NAME}'.")

# --- Script Execution Control ---
if __name__ == "__main__":
    run_stock_info_pipeline(
        mode='initial_load',
        target_tickers=['AAPL', 'MSFT', 'GOOG', 'NONEXISTENTTICKER'],
        db_file_path_str=':memory:',
        update_profile_if_older_than_days=-1 # Force profile fetch
    )
    logging.info("Script execution finished.")