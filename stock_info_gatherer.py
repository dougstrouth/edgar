# -*- coding: utf-8 -*-
"""
Stock Info Gatherer Script (v2.5)

Correctly fixes AttributeError: set_value. Adds more debug logging to
fetch_financial_statement processing.
Connects to EDGAR DuckDB, fetches supplementary info, loads data.
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

# --- Configuration and Logging Setup ---
script_dir = Path(__file__).resolve().parent
# Try loading .env from script directory first, then parent
dotenv_path = script_dir / '.env'
if not dotenv_path.is_file(): dotenv_path = script_dir.parent / '.env'
if dotenv_path.is_file(): load_dotenv(dotenv_path=dotenv_path)
else: print(f"Warning: .env file not found in {script_dir} or {script_dir.parent}", file=sys.stderr)

try:
    DEFAULT_DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError:
    # Fallback if DB_FILE not in .env (e.g., use a default path)
    DEFAULT_DB_FILE = script_dir.parent / "edgar_metadata.duckdb"
    print(f"Warning: 'DB_FILE' not found in .env. Defaulting to {DEFAULT_DB_FILE}", file=sys.stderr)


log_file_path = script_dir / "stock_info_gatherer.log"
log_file_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO, # Set to INFO for normal runs, DEBUG for deep diagnosis
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[ logging.FileHandler(log_file_path), logging.StreamHandler()]
)

# --- Constants ---
PROFILE_TABLE_NAME = "company_profile"
METRICS_TABLE_NAME = "company_key_metrics"
ACTIONS_TABLE_NAME = "stock_actions"
FIN_INCOME_A_TABLE_NAME = "financials_income_annual"
FIN_INCOME_Q_TABLE_NAME = "financials_income_quarterly"
FIN_BALANCE_A_TABLE_NAME = "financials_balance_annual"
FIN_BALANCE_Q_TABLE_NAME = "financials_balance_quarterly"
FIN_CASHFLOW_A_TABLE_NAME = "financials_cashflow_annual"
FIN_CASHFLOW_Q_TABLE_NAME = "financials_cashflow_quarterly"
ERROR_TABLE_NAME = "stock_fetch_errors"
YFINANCE_DELAY = 0.6

# --- Database Functions (Unchanged from v2.2) ---
def connect_db(db_path_str: str, read_only: bool = True) -> Optional[duckdb.DuckDBPyConnection]:
    is_memory = (db_path_str == ':memory:')
    db_path = None
    if not is_memory:
        db_path = Path(db_path_str)
        if not db_path.is_file():
            logging.warning(f"DB file not found at: {db_path_str}. Will attempt to create.")
            # Ensure parent dir exists for creation attempt
            db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = duckdb.connect(database=db_path_str, read_only=read_only)
        logging.info(f"Connected: {db_path_str} (RO: {read_only})")
        return conn
    except Exception as e:
        logging.error(f"Failed connect {db_path_str}: {e}", exc_info=True)
        return None

def setup_info_tables(con: duckdb.DuckDBPyConnection):
    # (Schema definitions unchanged)
    logging.info("Setting up supplementary tables...")
    tables_sql = {
        PROFILE_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {PROFILE_TABLE_NAME} (ticker VARCHAR PRIMARY KEY COLLATE NOCASE, cik VARCHAR, fetch_timestamp TIMESTAMPTZ NOT NULL, quoteType VARCHAR, exchange VARCHAR, longName VARCHAR, sector VARCHAR, industry VARCHAR, longBusinessSummary TEXT, fullTimeEmployees INTEGER, website VARCHAR, beta DOUBLE, exchangeTimezoneName VARCHAR, exchangeTimezoneShortName VARCHAR, auditRisk INTEGER, boardRisk INTEGER, compensationRisk INTEGER, shareHolderRightsRisk INTEGER, overallRisk INTEGER, governanceEpochDate BIGINT, compensationAsOfEpochDate BIGINT);""",
        METRICS_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {METRICS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, fetch_date DATE NOT NULL, marketCap BIGINT, enterpriseValue BIGINT, trailingPE DOUBLE, forwardPE DOUBLE, priceToBook DOUBLE, priceToSalesTrailing12Months DOUBLE, enterpriseToRevenue DOUBLE, enterpriseToEbitda DOUBLE, dividendRate DOUBLE, dividendYield DOUBLE, beta DOUBLE, fiftyTwoWeekHigh DOUBLE, fiftyTwoWeekLow DOUBLE, sharesOutstanding BIGINT, floatShares BIGINT, bookValue DOUBLE, regularMarketPrice DOUBLE, previousClose DOUBLE, volume BIGINT, averageVolume BIGINT, averageVolume10days BIGINT, exDividendDate DATE, PRIMARY KEY (ticker, fetch_date));""",
        FIN_INCOME_A_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_INCOME_A_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, TotalRevenue BIGINT, CostOfRevenue BIGINT, GrossProfit BIGINT, OperatingIncome BIGINT, NetIncome BIGINT, NetIncomeFromContinuingOperations BIGINT, NetIncomeApplicableToCommonShares BIGINT, EBITDA BIGINT, EBIT BIGINT, InterestExpense BIGINT, IncomeBeforeTax BIGINT, IncomeTaxExpense BIGINT, ResearchDevelopment BIGINT, SellingGeneralAdministrative BIGINT, BasicEPS DOUBLE, DilutedEPS DOUBLE, PRIMARY KEY (ticker, report_date));""",
        FIN_INCOME_Q_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_INCOME_Q_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, TotalRevenue BIGINT, CostOfRevenue BIGINT, GrossProfit BIGINT, OperatingIncome BIGINT, NetIncome BIGINT, NetIncomeFromContinuingOperations BIGINT, NetIncomeApplicableToCommonShares BIGINT, EBITDA BIGINT, EBIT BIGINT, InterestExpense BIGINT, IncomeBeforeTax BIGINT, IncomeTaxExpense BIGINT, ResearchDevelopment BIGINT, SellingGeneralAdministrative BIGINT, BasicEPS DOUBLE, DilutedEPS DOUBLE, PRIMARY KEY (ticker, report_date));""",
        FIN_BALANCE_A_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_BALANCE_A_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, TotalAssets BIGINT, CurrentAssets BIGINT, TotalLiabilities BIGINT, CurrentLiabilities BIGINT, TotalEquityGrossMinorityInterest BIGINT, StockholdersEquity BIGINT, CommonStockEquity BIGINT, CashAndCashEquivalents BIGINT, AccountsReceivable BIGINT, Inventory BIGINT, PropertyPlantEquipmentNet BIGINT, AccountsPayable BIGINT, ShortTermDebt BIGINT, LongTermDebt BIGINT, RetainedEarnings BIGINT, PRIMARY KEY (ticker, report_date));""",
        FIN_BALANCE_Q_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_BALANCE_Q_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, TotalAssets BIGINT, CurrentAssets BIGINT, TotalLiabilities BIGINT, CurrentLiabilities BIGINT, TotalEquityGrossMinorityInterest BIGINT, StockholdersEquity BIGINT, CommonStockEquity BIGINT, CashAndCashEquivalents BIGINT, AccountsReceivable BIGINT, Inventory BIGINT, PropertyPlantEquipmentNet BIGINT, AccountsPayable BIGINT, ShortTermDebt BIGINT, LongTermDebt BIGINT, RetainedEarnings BIGINT, PRIMARY KEY (ticker, report_date));""",
        FIN_CASHFLOW_A_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_CASHFLOW_A_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, OperatingCashFlow BIGINT, InvestingCashFlow BIGINT, FinancingCashFlow BIGINT, EndCashPosition BIGINT, FreeCashFlow BIGINT, CapitalExpenditure BIGINT, IssuanceOfDebt BIGINT, RepaymentOfDebt BIGINT, RepurchaseOfCapitalStock BIGINT, DividendPaid BIGINT, PRIMARY KEY (ticker, report_date));""",
        FIN_CASHFLOW_Q_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {FIN_CASHFLOW_Q_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, OperatingCashFlow BIGINT, InvestingCashFlow BIGINT, FinancingCashFlow BIGINT, EndCashPosition BIGINT, FreeCashFlow BIGINT, CapitalExpenditure BIGINT, IssuanceOfDebt BIGINT, RepaymentOfDebt BIGINT, RepurchaseOfCapitalStock BIGINT, DividendPaid BIGINT, PRIMARY KEY (ticker, report_date));""",
        ACTIONS_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {ACTIONS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, action_date DATE NOT NULL, action_type VARCHAR NOT NULL, value DOUBLE NOT NULL, PRIMARY KEY (ticker, action_date, action_type));""",
        ERROR_TABLE_NAME: f"""CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);"""}
    try:
        for name, sql in tables_sql.items(): con.execute(sql); logging.debug(f"Table '{name}' setup ok.")
    except Exception as e: logging.error(f"Failed setup_info_tables: {e}", exc_info=True); raise

def drop_info_tables(con: duckdb.DuckDBPyConnection):
    tables = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_INCOME_A_TABLE_NAME, FIN_INCOME_Q_TABLE_NAME, FIN_BALANCE_A_TABLE_NAME, FIN_BALANCE_Q_TABLE_NAME, FIN_CASHFLOW_A_TABLE_NAME, FIN_CASHFLOW_Q_TABLE_NAME]
    logging.warning(f"Dropping tables: {', '.join(tables)}")
    try: [con.execute(f"DROP TABLE IF EXISTS {t};") for t in tables]
    except Exception as e: logging.error(f"Failed drop: {e}", exc_info=True); raise

def clear_info_tables(con: duckdb.DuckDBPyConnection):
    tables = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_INCOME_A_TABLE_NAME, FIN_INCOME_Q_TABLE_NAME, FIN_BALANCE_A_TABLE_NAME, FIN_BALANCE_Q_TABLE_NAME, FIN_CASHFLOW_A_TABLE_NAME, FIN_CASHFLOW_Q_TABLE_NAME]
    logging.warning(f"Clearing data: {', '.join(tables)}")
    try:
        db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        for t in tables:
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

def load_data_to_table(con: duckdb.DuckDBPyConnection, data_df: pd.DataFrame, table_name: str, pk_cols: List[str], mode: str) -> bool:
    """Generic function to load a DataFrame into a table. Returns True on success, False on failure."""
    if data_df is None or data_df.empty: logging.debug(f"DF {table_name} empty."); return True
    ticker = data_df['ticker'].iloc[0] if 'ticker' in data_df.columns else 'N/A'
    logging.info(f"Loading {len(data_df)} rows into {table_name} for ticker {ticker} (Mode: {mode})")
    db_cols = []; load_df = pd.DataFrame()
    try: db_cols = get_db_columns(con, table_name)
    except duckdb.duckdb.TransactionException: raise
    except Exception: logging.error(f"Load fail {ticker} {table_name}: No DB cols."); return False
    if not db_cols: logging.error(f"Load fail {ticker} {table_name}: No DB cols."); return False

    try:
        cols_to_load = [col for col in db_cols if col in data_df.columns]
        load_df = data_df[cols_to_load].copy()
        missing_cols = [col for col in db_cols if col not in load_df.columns]
        # *** CORRECTED FIX for set_value AttributeError ***
        if missing_cols:
            logging.warning(f"Cols missing DF->{table_name} ({ticker}): {missing_cols}. Inserting NULL.")
            for col in missing_cols:
                load_df[col] = None # Use standard assignment here
        # *** End Correction ***
        load_df = load_df[db_cols] # Ensure column order
        load_df = load_df.astype(object).where(pd.notnull(load_df), None)
        for pk_col in pk_cols:
            if pk_col not in load_df.columns or load_df[pk_col].isnull().any():
                logging.error(f"Load fail {ticker} {table_name}: PK '{pk_col}' contains NULL values.")
                return False
    except Exception as df_prep_e: logging.error(f"Error prepping DF {table_name} ({ticker}): {df_prep_e}", exc_info=True); return False

    reg_name = f'{table_name}_reg_{int(time.time()*1000)}'
    try:
        con.register(reg_name, load_df)
        cols_str = ", ".join([f'"{c}"' for c in db_cols])
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}" if mode != 'full_refresh' else f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}"
        con.execute(sql); con.unregister(reg_name); logging.debug(f"Loaded {table_name} for {ticker}."); return True
    except Exception as e:
        logging.error(f"DB error loading {table_name} for {ticker}: {e}", exc_info=False)
        try: con.unregister(reg_name)
        except: pass; return False

# --- Data Fetching Functions ---

def fetch_company_info(ticker_obj: yf.Ticker) -> Optional[Dict[str, Any]]:
    # (Function remains the same as v2.2)
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
    """Fetches and transforms financials, balance sheet, or cash flow."""
    logging.debug(f"Fetching {frequency} {statement_type} for {ticker_obj.ticker}")
    data = None
    try:
        method_map = {
            ('financials', 'quarterly'): ticker_obj.quarterly_financials, ('financials', 'annual'): ticker_obj.financials,
            ('balance_sheet', 'quarterly'): ticker_obj.quarterly_balance_sheet, ('balance_sheet', 'annual'): ticker_obj.balance_sheet,
            ('cashflow', 'quarterly'): ticker_obj.quarterly_cashflow, ('cashflow', 'annual'): ticker_obj.cashflow,
        }
        data = method_map.get((statement_type, frequency))

        if data is None or data.empty: logging.info(f"No {frequency} {statement_type} data found for {ticker_obj.ticker}."); return None

        # --- Debug Logging ---
        logging.debug(f"Raw {frequency} {statement_type} data head for {ticker_obj.ticker}:\n{data.head()}")
        logging.debug(f"Raw data index: {data.index}")
        logging.debug(f"Raw data columns: {data.columns}")
        buf = io.StringIO(); data.info(buf=buf); logging.debug(f"Raw data info:\n{buf.getvalue()}")
        # --- End Debug Logging ---

        data_transposed = data.transpose()
        logging.debug(f"Transposed columns: {data_transposed.columns}")
        logging.debug(f"Transposed index: {data_transposed.index}")

        if isinstance(data_transposed.index, pd.MultiIndex):
            logging.warning(f"Unexpected MultiIndex {frequency} {statement_type} {ticker_obj.ticker}. Using level 0.")
            data_transposed.index = data_transposed.index.get_level_values(0)
        if not isinstance(data_transposed.index, pd.DatetimeIndex):
            data_transposed.index = pd.to_datetime(data_transposed.index, errors='coerce')
        if data_transposed.index.hasnans:
             logging.warning(f"Invalid dates in index {frequency} {statement_type} {ticker_obj.ticker}. Dropping.")
             data_transposed = data_transposed[data_transposed.index.notna()]
        if data_transposed.empty: logging.warning(f"Date processing removed rows {frequency} {statement_type} {ticker_obj.ticker}."); return None

        data_transposed.index = data_transposed.index.date
        data_transposed.index.name = 'report_date'
        data_transposed.reset_index(inplace=True)
        logging.debug(f"DF after reset_index head:\n{data_transposed.head()}")


        data_transposed['ticker'] = ticker_obj.ticker
        data_transposed['fetch_timestamp'] = datetime.now(timezone.utc)

        def clean_col_name(col):
            if isinstance(col, (datetime, date)): return str(col)
            name = str(col).strip(); name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
            name = '_'.join(filter(None, name.split('_'))); name = name.replace('__','_') # Avoid double underscores
            if name and name[0].isdigit(): name = '_' + name
            return name if name else f'col_{hash(col)}'
        original_columns = list(data_transposed.columns) # Keep track before cleaning
        data_transposed.columns = [clean_col_name(col) for col in data_transposed.columns]
        logging.debug(f"Cleaned columns: {list(data_transposed.columns)}")


        if 'report_date' not in data_transposed.columns or data_transposed['report_date'].isnull().any():
             logging.error(f"Invalid 'report_date' post-processing {ticker_obj.ticker}, {frequency} {statement_type}.")
             return None

        # Convert data types more carefully
        numeric_cols_cleaned = [col for col in data_transposed.columns if col not in ['ticker', 'report_date', 'fetch_timestamp']]
        for col in numeric_cols_cleaned:
            # Check if the column actually exists (it should, but defensive check)
            if col in data_transposed:
                 try:
                     # Convert to numeric, coercing errors. IMPORTANT: Use float64 first for broad compatibility.
                     data_transposed[col] = pd.to_numeric(data_transposed[col], errors='coerce')#.astype('float64') # Keep as float for now

                     # --- Optional Int Conversion (handle with care) ---
                     # Only convert to Int64 if NO NaNs exist *after* coercion AND all values are integers
                     # if data_transposed[col].notna().all() and data_transposed[col].dropna().apply(lambda x: float(x).is_integer()).all():
                     #     try: data_transposed[col] = data_transposed[col].astype(pd.Int64Dtype())
                     #     except Exception as int_e: logging.warning(f"Could not convert {col} to Int64: {int_e}")
                 except KeyError as ke:
                     logging.error(f"KeyError during numeric conversion for column '{col}' in {ticker_obj.ticker} {frequency} {statement_type}. Available columns: {list(data_transposed.columns)}. Error: {ke}", exc_info=True)
                     raise # Re-raise the specific KeyError
                 except Exception as num_e:
                     logging.error(f"Error converting column '{col}' to numeric for {ticker_obj.ticker} {frequency} {statement_type}: {num_e}", exc_info=True)
                     # Keep column as object, NAs might exist from coercion
            else:
                 logging.warning(f"Cleaned column '{col}' not found in DataFrame for {ticker_obj.ticker}. Original columns: {original_columns}")

        return data_transposed

    except KeyError as ke: # Catch KeyError specifically here
        logging.error(f"KeyError processing {frequency} {statement_type} for {ticker_obj.ticker}: {ke}. Structure unexpected.", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Failed fetch/process {frequency} {statement_type} for {ticker_obj.ticker}: {type(e).__name__} - {e}", exc_info=False)
        return None


def fetch_stock_actions(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    # (Function remains the same as v2.2)
    logging.debug(f"Fetching actions for {ticker_obj.ticker}")
    try:
        actions = ticker_obj.actions
        if actions is None: logging.info(f"No actions data (None) for {ticker_obj.ticker}."); return None
        if actions.empty: logging.info(f"No actions data (empty df) for {ticker_obj.ticker}."); return None
        actions.index.name = 'action_date'; actions.reset_index(inplace=True)
        actions['ticker'] = ticker_obj.ticker; actions['action_date'] = pd.to_datetime(actions['action_date']).dt.date
        actions_melted = actions.melt(id_vars=['ticker', 'action_date'], value_vars=['Dividends', 'Stock Splits'], var_name='action_type', value_name='value')
        actions_melted = actions_melted[actions_melted['value'] != 0.0]; actions_melted.dropna(subset=['value'], inplace=True)
        actions_melted['action_type'] = actions_melted['action_type'].replace({'Stock Splits': 'Split'})
        return actions_melted[['ticker', 'action_date', 'action_type', 'value']]
    except Exception as e: logging.error(f"Failed actions {ticker_obj.ticker}: {type(e).__name__} - {e}", exc_info=False); return None

# --- Main Pipeline Function ---
def run_stock_info_pipeline(
    mode: str = 'initial_load',
    target_tickers: Optional[List[str]] = None,
    db_file_path_str: str = str(DEFAULT_DB_FILE),
    update_profile_if_older_than_days: int = 30
):
    """Main function to run the supplementary stock info gathering pipeline."""
    # (Structure mostly same as v2.2, error handling relies on load func return)
    start_run_time = time.time()
    logging.info(f"--- Starting Stock Info Pipeline (v2.5) ---")
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

    # --- Mode Handling & Setup ---
    try: # Outer try for setup/clear operations
        if mode == 'drop_tables':
            write_conn.begin()
            drop_info_tables(write_conn)
            write_conn.commit()
            logging.info("Drop tables operation finished.")
            write_conn.close()
            return # Exit after dropping

        # Setup tables (ensure they exist for other modes)
        # Do setup outside transaction in case it fails partially
        # No, keep in transaction to ensure all tables are created or none are
        write_conn.begin()
        setup_info_tables(write_conn)
        write_conn.commit()

        # Clear tables if refreshing
        if mode == 'full_refresh':
            write_conn.begin()
            clear_info_tables(write_conn)
            write_conn.commit()
            logging.info("Full refresh: Cleared existing supplementary stock data.")

    except Exception as setup_e:
        # Handle errors during setup, clear, or drop
        logging.error(f"Failed during table setup/clear: {setup_e}. Exiting.", exc_info=True)
        # Attempt rollback (might fail if connection is bad or no transaction active)
        try:
            # Ensure rollback is attempted only if connection seems usable
            if write_conn and not write_conn.closed:
                 write_conn.rollback()
                 logging.warning("Rolled back transaction due to setup/clear error.")
        except Exception as rb_e:
            # Log if rollback itself fails, but proceed to close/exit
            logging.error(f"Rollback attempt failed after setup/clear error: {rb_e}")
        # Finally, always try to close and then exit
        if write_conn and not write_conn.closed:
            write_conn.close()
            logging.info("Closed DB connection after setup/clear error.")
        return # Exit the pipeline function

    # --- If setup/clear was successful, continue below ---
    # ... (rest of the function starting with CIK Mapper Init) ...
    # Identify Tickers
    tickers_to_process = []
    if target_tickers: tickers_to_process = [t.upper() for t in target_tickers]; logging.info(f"Processing {len(tickers_to_process)} target tickers.")
    else:
        source_table = "stock_history"; count = 0
        try: count = write_conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
        except: logging.warning(f"{source_table} not found, trying 'tickers'.")
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
        ticker_fetch_success = True; ticker_load_success = True
        profile_df, metrics_df, actions_df = None, None, None
        fin_dfs = {}

        try:
            ticker_obj = yf.Ticker(ticker)
            # Fetch Profile & Metrics
            needs_profile_fetch = True; last_fetch_ts = profile_timestamps.get(ticker)
            if last_fetch_ts and update_profile_if_older_than_days >= 0 and (datetime.now(timezone.utc) - last_fetch_ts) < timedelta(days=update_profile_if_older_than_days): needs_profile_fetch = False; logging.debug(f"Skip profile fetch {ticker}.")
            if needs_profile_fetch:
                info_dict = fetch_company_info(ticker_obj)
                if info_dict is None: fetch_errors_count += 1; ticker_fetch_success = False; log_info_fetch_error(write_conn, None, ticker, 'Info Fetch Error', 'fetch_company_info returned None')
                else:
                    try:
                        db_prof_cols = get_db_columns(write_conn, PROFILE_TABLE_NAME); prof_data = {k: info_dict.get(k) for k in db_prof_cols if k not in ['fetch_timestamp', 'cik']}; prof_data['ticker'] = ticker; prof_data['fetch_timestamp'] = datetime.now(timezone.utc)
                        try: prof_data['cik'] = write_conn.execute("SELECT cik FROM tickers WHERE ticker=? LIMIT 1", [ticker]).fetchone()[0]
                        except: prof_data['cik'] = None
                        profile_df = pd.DataFrame([prof_data])
                        db_met_cols = get_db_columns(write_conn, METRICS_TABLE_NAME); met_data = {k: info_dict.get(k) for k in db_met_cols if k != 'fetch_date'}; met_data['ticker'] = ticker; met_data['fetch_date'] = date.today()
                        for date_key in ['exDividendDate']:
                            if date_key in met_data and isinstance(met_data[date_key], (int, float)):
                                try: met_data[date_key] = datetime.fromtimestamp(met_data[date_key], timezone.utc).date()
                                except (TypeError, ValueError, OSError) as de: logging.warning(f"Epoch conv fail {date_key} ({met_data[date_key]}) {ticker}: {de}"); met_data[date_key] = None
                            elif date_key in met_data and not isinstance(met_data[date_key], (date, type(None))):
                                try: met_data[date_key] = pd.to_datetime(met_data[date_key]).date()
                                except: met_data[date_key] = None
                        metrics_df = pd.DataFrame([met_data])
                    except Exception as prep_e: logging.error(f"Error prep profile/metrics {ticker}: {prep_e}", exc_info=True); fetch_errors_count += 1; ticker_fetch_success = False; log_info_fetch_error(write_conn, None, ticker, 'Data Prep Error', f'Failed prep profile/metrics: {prep_e}'); profile_df, metrics_df = None, None
            # Fetch Financials
            fin_map_keys = {FIN_INCOME_A_TABLE_NAME: ('financials', 'annual'), FIN_INCOME_Q_TABLE_NAME: ('financials', 'quarterly'), FIN_BALANCE_A_TABLE_NAME: ('balance_sheet', 'annual'), FIN_BALANCE_Q_TABLE_NAME: ('balance_sheet', 'quarterly'), FIN_CASHFLOW_A_TABLE_NAME: ('cashflow', 'annual'), FIN_CASHFLOW_Q_TABLE_NAME: ('cashflow', 'quarterly')}
            for table_name, (stmt_type, freq) in fin_map_keys.items():
                 df = fetch_financial_statement(ticker_obj, stmt_type, freq); fin_dfs[table_name] = df
                 if df is None: fetch_errors_count += 1; ticker_fetch_success = False
            # Fetch Actions
            actions_df = fetch_stock_actions(ticker_obj)
            if actions_df is None: fetch_errors_count += 1; ticker_fetch_success = False

            # --- Load Phase ---
            if not ticker_fetch_success: logging.warning(f"Skip load {ticker} due to fetch errors."); skipped_tickers_count += 1; continue
            try:
                write_conn.begin(); load_failed_flag = False
                if needs_profile_fetch and profile_df is not None and not profile_df.empty:
                    if not load_data_to_table(write_conn, profile_df, PROFILE_TABLE_NAME, ['ticker'], mode): load_failed_flag = True
                if needs_profile_fetch and metrics_df is not None and not metrics_df.empty:
                     if not load_data_to_table(write_conn, metrics_df, METRICS_TABLE_NAME, ['ticker', 'fetch_date'], mode): load_failed_flag = True
                for table_name, df in fin_dfs.items():
                     if df is not None and not df.empty:
                         if not load_data_to_table(write_conn, df, table_name, ['ticker', 'report_date'], mode): load_failed_flag = True
                if actions_df is not None and not actions_df.empty:
                    if not load_data_to_table(write_conn, actions_df, ACTIONS_TABLE_NAME, ['ticker', 'action_date', 'action_type'], mode): load_failed_flag = True

                if not load_failed_flag: write_conn.commit(); processed_tickers_count += 1
                else: logging.warning(f"Rolling back {ticker} due to load errors."); write_conn.rollback(); load_errors_count += 1; skipped_tickers_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Load Failure', 'One or more loads failed.')
            except Exception as load_tx_e:
                 logging.error(f"Error load tx {ticker}: {load_tx_e}", exc_info=True)
                 try: write_conn.rollback(); logging.info(f"Rolled back {ticker}.")
                 except Exception as rb_e: logging.error(f"Rollback fail {ticker}: {rb_e}")
                 load_errors_count += 1; skipped_tickers_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Load Transaction Error', str(load_tx_e))
        except Exception as ticker_e:
            logging.error(f"Major error {ticker}: {ticker_e}", exc_info=True); fetch_errors_count +=1; skipped_tickers_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Ticker Processing Error', str(ticker_e))
            try: write_conn.rollback()
            except: pass
        if YFINANCE_DELAY > 0: time.sleep(YFINANCE_DELAY)

    # Final Cleanup & Summary
    if write_conn:
        try: write_conn.commit() # Commit any pending error logs
        except: pass
        try: write_conn.close(); logging.info("DB connection closed.")
        except Exception as close_e: logging.error(f"Error closing DB: {close_e}")
    end_run_time = time.time()
    logging.info(f"--- Stock Info Pipeline Finished ---"); logging.info(f"Total Time: {end_run_time - start_run_time:.2f} sec")
    logging.info(f"Tickers Attempted: {total_tickers_to_process}"); logging.info(f"Tickers OK: {processed_tickers_count}"); logging.info(f"Tickers Skip/Err: {skipped_tickers_count}")
    logging.info(f"Fetch Errors: {fetch_errors_count}"); logging.info(f"Load Errors: {load_errors_count}"); logging.info(f"Check '{ERROR_TABLE_NAME}'.")

# --- Script Execution Control ---
if __name__ == "__main__":
    run_stock_info_pipeline(
        mode='initial_load',
        target_tickers=['AAPL', 'MSFT', 'GOOG', 'NONEXISTENTTICKER'],
        db_file_path_str=':memory:',
        update_profile_if_older_than_days=-1 # Force profile fetch
    )
    logging.info("Script execution finished.")