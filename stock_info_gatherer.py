# -*- coding: utf-8 -*-
"""
Stock Info Gatherer Script (v4.0 - Clean Implementation)

Fetches supplementary company info (profile, ISIN, metrics), calendar events,
analyst recommendations, holder data (major, institutional, mutual fund),
stock actions (dividends/splits), and financial statement data (long format)
using yfinance. Stores data in respective DuckDB tables.
Prioritizes correct syntax and implementation of agreed-upon logic.
"""

from database_conn import ManagedDatabaseConnection, get_db_connection # Import both
import duckdb # Keep original duckdb import if needed for types etc.import pandas as pd
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
RECS_TABLE_NAME = "analyst_recommendations"
HOLDERS_MAJOR_TABLE_NAME = "major_holders"
HOLDERS_INST_TABLE_NAME = "institutional_holders"
HOLDERS_MF_TABLE_NAME = "mutual_fund_holders"
CALENDAR_TABLE_NAME = "calendar_events"
ERROR_TABLE_NAME = "stock_fetch_errors"
YFINANCE_DELAY = 0.6 # Be polite to Yahoo Finance

# --- Database Functions ---
def connect_db(db_path_str: str, read_only: bool = True) -> Optional[duckdb.DuckDBPyConnection]:
    is_memory = (db_path_str == ':memory:')
    db_path = None
    if not is_memory:
        db_path = Path(db_path_str)
        if not db_path.exists():
             logging.warning(f"DB path not found: {db_path_str}. Attempting creation.")
             try: db_path.parent.mkdir(parents=True, exist_ok=True)
             except Exception as dir_e: logging.error(f"Cannot create dir {db_path.parent}: {dir_e}"); return None
    try: conn = duckdb.connect(database=db_path_str, read_only=read_only); logging.info(f"Connected: {db_path_str} (RO: {read_only})"); return conn
    except Exception as e: logging.error(f"Failed connect {db_path_str}: {e}", exc_info=True); return None

def setup_info_tables(con: duckdb.DuckDBPyConnection):
    """Creates/Alters tables for supplementary stock info."""
    logging.info("Setting up supplementary tables...")
    # Define tables, including ISIN directly in profile
    profile_sql = f"""CREATE TABLE IF NOT EXISTS {PROFILE_TABLE_NAME} (ticker VARCHAR PRIMARY KEY COLLATE NOCASE, cik VARCHAR, fetch_timestamp TIMESTAMPTZ NOT NULL, quoteType VARCHAR, exchange VARCHAR, longName VARCHAR, sector VARCHAR, industry VARCHAR, longBusinessSummary TEXT, fullTimeEmployees INTEGER, website VARCHAR, beta DOUBLE, exchangeTimezoneName VARCHAR, exchangeTimezoneShortName VARCHAR, auditRisk INTEGER, boardRisk INTEGER, compensationRisk INTEGER, shareHolderRightsRisk INTEGER, overallRisk INTEGER, governanceEpochDate BIGINT, compensationAsOfEpochDate BIGINT, isin VARCHAR);"""
    metrics_sql = f"""CREATE TABLE IF NOT EXISTS {METRICS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, fetch_date DATE NOT NULL, marketCap BIGINT, enterpriseValue BIGINT, trailingPE DOUBLE, forwardPE DOUBLE, priceToBook DOUBLE, priceToSalesTrailing12Months DOUBLE, enterpriseToRevenue DOUBLE, enterpriseToEbitda DOUBLE, dividendRate DOUBLE, dividendYield DOUBLE, beta DOUBLE, fiftyTwoWeekHigh DOUBLE, fiftyTwoWeekLow DOUBLE, sharesOutstanding BIGINT, floatShares BIGINT, bookValue DOUBLE, regularMarketPrice DOUBLE, previousClose DOUBLE, volume BIGINT, averageVolume BIGINT, averageVolume10days BIGINT, exDividendDate DATE, PRIMARY KEY (ticker, fetch_date));"""
    fin_facts_sql = f"""CREATE TABLE IF NOT EXISTS {FIN_FACTS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, frequency VARCHAR NOT NULL, statement_type VARCHAR NOT NULL, item_label VARCHAR NOT NULL, value DOUBLE PRECISION, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, report_date, frequency, item_label));"""
    actions_sql = f"""CREATE TABLE IF NOT EXISTS {ACTIONS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, action_date DATE NOT NULL, action_type VARCHAR NOT NULL, value DOUBLE NOT NULL, PRIMARY KEY (ticker, action_date, action_type));"""
    error_sql = f"""CREATE TABLE IF NOT EXISTS {ERROR_TABLE_NAME} (cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);"""
    calendar_sql = f"""CREATE TABLE IF NOT EXISTS {CALENDAR_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, event_type VARCHAR NOT NULL, event_start_date DATE, event_end_date DATE, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, event_type));"""
    recs_sql = f"""CREATE TABLE IF NOT EXISTS {RECS_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, recommendation_timestamp TIMESTAMPTZ NOT NULL, firm VARCHAR NOT NULL, to_grade VARCHAR, from_grade VARCHAR, action VARCHAR, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, recommendation_timestamp, firm));"""
    holders_major_sql = f"""CREATE TABLE IF NOT EXISTS {HOLDERS_MAJOR_TABLE_NAME} (ticker VARCHAR PRIMARY KEY COLLATE NOCASE, pct_insiders DOUBLE PRECISION, pct_institutions DOUBLE PRECISION, fetch_timestamp TIMESTAMPTZ NOT NULL);"""
    holders_inst_sql = f"""CREATE TABLE IF NOT EXISTS {HOLDERS_INST_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, holder VARCHAR NOT NULL, shares BIGINT, pct_out DOUBLE PRECISION, value BIGINT, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, report_date, holder));"""
    holders_mf_sql = f"""CREATE TABLE IF NOT EXISTS {HOLDERS_MF_TABLE_NAME} (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, holder VARCHAR NOT NULL, shares BIGINT, pct_out DOUBLE PRECISION, value BIGINT, fetch_timestamp TIMESTAMPTZ NOT NULL, PRIMARY KEY (ticker, report_date, holder));"""
    all_sql = [(PROFILE_TABLE_NAME, profile_sql), (METRICS_TABLE_NAME, metrics_sql), (FIN_FACTS_TABLE_NAME, fin_facts_sql), (ACTIONS_TABLE_NAME, actions_sql), (ERROR_TABLE_NAME, error_sql), (CALENDAR_TABLE_NAME, calendar_sql), (RECS_TABLE_NAME, recs_sql), (HOLDERS_MAJOR_TABLE_NAME, holders_major_sql), (HOLDERS_INST_TABLE_NAME, holders_inst_sql), (HOLDERS_MF_TABLE_NAME, holders_mf_sql)]
    try:
        for name, sql in all_sql: con.execute(sql); logging.debug(f"Table '{name}' setup ok.")
    except Exception as e: logging.error(f"Failed setup_info_tables: {e}", exc_info=True); raise

def drop_info_tables(con: duckdb.DuckDBPyConnection):
    tables = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_FACTS_TABLE_NAME, RECS_TABLE_NAME, HOLDERS_MAJOR_TABLE_NAME, HOLDERS_INST_TABLE_NAME, HOLDERS_MF_TABLE_NAME, CALENDAR_TABLE_NAME, ERROR_TABLE_NAME] # Include error table if managed here
    logging.warning(f"Dropping tables: {', '.join(tables)}")
    try: [con.execute(f"DROP TABLE IF EXISTS {t};") for t in tables]
    except Exception as e: logging.error(f"Failed drop: {e}", exc_info=True); raise

def clear_info_tables(con: duckdb.DuckDBPyConnection):
    tables = [PROFILE_TABLE_NAME, METRICS_TABLE_NAME, ACTIONS_TABLE_NAME, FIN_FACTS_TABLE_NAME, RECS_TABLE_NAME, HOLDERS_MAJOR_TABLE_NAME, HOLDERS_INST_TABLE_NAME, HOLDERS_MF_TABLE_NAME, CALENDAR_TABLE_NAME]
    logging.warning(f"Clearing data: {', '.join(tables)}")
    try:
        db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        for t in tables:
            if t.lower() in db_tables: con.execute(f"DELETE FROM {t};"); logging.info(f"Cleared {t}.")
            else: logging.warning(f"Table {t} not found.")
        # Optionally clear errors related ONLY to this script
        if ERROR_TABLE_NAME.lower() in db_tables:
             con.execute(f"DELETE FROM {ERROR_TABLE_NAME} WHERE error_type LIKE '%Info%' OR error_type LIKE '%Financials%' OR error_type LIKE '%Actions%' OR error_type LIKE '%Calendar%' OR error_type LIKE '%Recs%' OR error_type LIKE '%Holders%'")
             logging.info(f"Cleared relevant errors from {ERROR_TABLE_NAME}")
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

def load_data(con: duckdb.DuckDBPyConnection, data_df: pd.DataFrame, table_name: str, mode: str) -> bool:
    """Loads a prepared DataFrame into the specified table using INSERT OR REPLACE or INSERT."""
    if data_df is None or data_df.empty: logging.debug(f"DF {table_name} empty."); return True
    ticker = data_df['ticker'].iloc[0] if 'ticker' in data_df.columns else 'N/A'
    logging.info(f"Loading {len(data_df)} rows into {table_name} for {ticker} (Mode: {mode})")

    # Ensure DF columns match DB (caller should prepare DF with correct names/order)
    # Convert Pandas NA/NaT to None just before loading
    load_df_final = data_df.astype(object).where(pd.notnull(data_df), None)

    reg_name = f'{table_name}_reg_{int(time.time()*1000)}'
    try:
        # Use SQL to directly select columns from registered DF matching table columns
        db_cols = get_db_columns(con, table_name)
        if not db_cols: logging.error(f"Cannot load {table_name}, get cols failed."); return False
        db_cols_str = ", ".join([f'"{c}"' for c in db_cols])
        select_cols_str = ", ".join([f'"{c}"' for c in db_cols if c in load_df_final.columns]) # Select only cols existing in DF

        con.register(reg_name, load_df_final)
        # Use INSERT OR REPLACE based on primary key defined in the table schema
        sql = f"INSERT OR REPLACE INTO {table_name} ({db_cols_str}) SELECT {db_cols_str} FROM {reg_name}" if mode != 'full_refresh' else f"INSERT INTO {table_name} ({db_cols_str}) SELECT {db_cols_str} FROM {reg_name}"

        con.execute(sql)
        con.unregister(reg_name)
        logging.debug(f"Loaded {table_name} for {ticker}.")
        return True
    except Exception as e:
        logging.error(f"DB error loading {table_name} for {ticker}: {e}", exc_info=False)
        try: con.unregister(reg_name)
        except: pass
        return False

# --- Data Fetching Functions ---
# (fetch_company_info, fetch_financial_statement, fetch_stock_actions are unchanged from v3.8)
def fetch_company_info(ticker_obj: yf.Ticker) -> Optional[Dict[str, Any]]:
    try: info = ticker_obj.info; return info if info and isinstance(info, dict) else None
    except Exception as e: logging.error(f"Failed fetch .info {ticker_obj.ticker}: {type(e).__name__}-{e}", exc_info=False); return None

def fetch_financial_statement(ticker_obj: yf.Ticker, statement_type: str, frequency: str) -> Optional[pd.DataFrame]:
    logging.debug(f"Fetching raw {frequency} {statement_type} for {ticker_obj.ticker}")
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
        if actions is None or actions.empty: logging.info(f"No actions {ticker_obj.ticker}."); return None
        actions.index.name = 'action_date'; actions.reset_index(inplace=True)
        actions['ticker'] = ticker_obj.ticker; actions['action_date'] = pd.to_datetime(actions['action_date']).dt.date
        actions_melted = actions.melt(id_vars=['ticker', 'action_date'], value_vars=['Dividends', 'Stock Splits'], var_name='action_type', value_name='value')
        actions_melted = actions_melted[actions_melted['value'] != 0.0]; actions_melted.dropna(subset=['value'], inplace=True)
        actions_melted['action_type'] = actions_melted['action_type'].replace({'Stock Splits': 'Split'})
        return actions_melted[['ticker', 'action_date', 'action_type', 'value']]
    except Exception as e: logging.error(f"Failed actions {ticker_obj.ticker}: {type(e).__name__} - {e}", exc_info=False); return None

# --- Fetch Functions for ISIN, Calendar, Recs, Holders ---
def fetch_isin(ticker_obj: yf.Ticker) -> Optional[str]:
    logging.debug(f"Fetching ISIN for {ticker_obj.ticker}")
    try: isin = ticker_obj.isin; return isin if isinstance(isin, str) and isin != '-' else None
    except Exception as e: logging.error(f"Failed ISIN {ticker_obj.ticker}: {e}", exc_info=False); return None

def fetch_calendar(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    # *** FIX: Correct dict handling ***
    logging.debug(f"Fetching calendar for {ticker_obj.ticker}")
    try:
        cal_data = ticker_obj.calendar
        if not cal_data or not isinstance(cal_data, dict): logging.info(f"No calendar data/wrong format {ticker_obj.ticker}."); return None
        cal_processed = []
        # Earnings Date processing
        earnings_info = cal_data.get('Earnings')
        if earnings_info and isinstance(earnings_info.get('Earnings Date'), (list, tuple)) and len(earnings_info['Earnings Date']) > 0:
             start_ts = earnings_info['Earnings Date'][0]; end_ts = earnings_info['Earnings Date'][1] if len(earnings_info['Earnings Date']) > 1 else start_ts
             start_date = pd.to_datetime(start_ts, errors='coerce', unit='s').date() if pd.notna(start_ts) else None
             end_date = pd.to_datetime(end_ts, errors='coerce', unit='s').date() if pd.notna(end_ts) else start_date
             if start_date: cal_processed.append({'event_type': 'Earnings', 'event_start_date': start_date, 'event_end_date': end_date})
        # Dividend Date processing
        div_ts = cal_data.get('Dividend Date')
        if pd.notna(div_ts):
             div_date = pd.to_datetime(div_ts, errors='coerce', unit='s').date()
             if div_date: cal_processed.append({'event_type': 'Dividend', 'event_start_date': div_date, 'event_end_date': div_date})
        # Ex-Dividend Date processing
        ex_div_ts = cal_data.get('Ex-Dividend Date')
        if pd.notna(ex_div_ts):
             ex_div_date = pd.to_datetime(ex_div_ts, errors='coerce', unit='s').date()
             if ex_div_date: cal_processed.append({'event_type': 'Ex-Dividend', 'event_start_date': ex_div_date, 'event_end_date': ex_div_date})
        if not cal_processed: logging.info(f"No processable events found in calendar for {ticker_obj.ticker}."); return None
        df = pd.DataFrame(cal_processed); df['ticker'] = ticker_obj.ticker; df['fetch_timestamp'] = datetime.now(timezone.utc)
        cal_db_cols = ['ticker', 'event_type', 'event_start_date', 'event_end_date', 'fetch_timestamp']
        return df[[c for c in cal_db_cols if c in df.columns]]
    except Exception as e: logging.error(f"Failed calendar {ticker_obj.ticker}: {e}", exc_info=True); return None

def fetch_recommendations(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    # *** FIX: Robust column handling ***
    logging.debug(f"Fetching recommendations for {ticker_obj.ticker}")
    try:
        recs = ticker_obj.recommendations
        if recs is None or recs.empty: logging.info(f"No recs {ticker_obj.ticker}."); return None
        logging.debug(f"Raw recommendations columns: {recs.columns}")
        recs.index.name = 'recommendation_timestamp_idx'
        recs.reset_index(inplace=True)
        recs['ticker'] = ticker_obj.ticker
        recs['fetch_timestamp'] = datetime.now(timezone.utc)
        # Define mapping from common yfinance names to DB names
        # Use lower case for matching resilience
        col_map = { 'recommendation_timestamp_idx': 'recommendation_timestamp', 'firm': 'firm', 'to grade': 'to_grade', 'from grade': 'from_grade', 'action': 'action'}
        final_df = pd.DataFrame({'ticker': recs['ticker'], 'fetch_timestamp': recs['fetch_timestamp']}) # Start with base cols

        for yf_col_pattern, db_col in col_map.items():
            matched_col = next((c for c in recs.columns if str(c).lower() == yf_col_pattern.lower()), None)
            if matched_col:
                 final_df[db_col] = recs[matched_col]
            else:
                 logging.warning(f"Expected rec column like '{yf_col_pattern}' not found for {ticker_obj.ticker}")
                 final_df[db_col] = None # Add column as None if missing

        # Convert timestamp
        ts_col = 'recommendation_timestamp'
        if ts_col in final_df.columns:
            final_df[ts_col] = pd.to_datetime(final_df[ts_col], errors='coerce', utc=True)
            if final_df[ts_col].dt.tz is None: final_df[ts_col] = final_df[ts_col].dt.tz_localize(timezone.utc)
            else: final_df[ts_col] = final_df[ts_col].dt.tz_convert(timezone.utc)
        else: logging.warning(f"'{ts_col}' missing for {ticker_obj.ticker}."); return None # Need for PK

        # Check PKs
        pk_cols = ['ticker', 'recommendation_timestamp', 'firm']
        if not all(pk in final_df.columns for pk in pk_cols): logging.warning(f"PK cols missing recs {ticker_obj.ticker}"); return None
        final_df.dropna(subset=pk_cols, inplace=True)
        if final_df.empty: return None

        db_cols_order = ['ticker', 'recommendation_timestamp', 'firm', 'to_grade', 'from_grade', 'action', 'fetch_timestamp']
        return final_df[[c for c in db_cols_order if c in final_df.columns]]
    except Exception as e: logging.error(f"Failed recs {ticker_obj.ticker}: {e}", exc_info=True); return None

def fetch_major_holders(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
     # *** FIX: More robust parsing using labels ***
     logging.debug(f"Fetching major holders for {ticker_obj.ticker}")
     try:
         holders_data = ticker_obj.major_holders
         if holders_data is None or holders_data.empty: logging.info(f"No major holders {ticker_obj.ticker}."); return None
         logging.debug(f"Raw major_holders data type: {type(holders_data)}\n{holders_data}")
         pct_insiders = None; pct_institutions = None
         # yfinance often returns a 2-column DataFrame [Value, Label]
         if isinstance(holders_data, pd.DataFrame) and holders_data.shape[1] >= 2:
             # Standardize column access attempt
             holders_data.columns = ['Value', 'Label']
             for _, row in holders_data.iterrows():
                 label = str(row.get('Label', '')).lower() # Safe access with .get()
                 value_str = str(row.get('Value', '')).replace('%','')
                 try:
                     value_float = float(value_str) / 100.0
                     if 'insider' in label: pct_insiders = value_float
                     elif 'institutions' in label: pct_institutions = value_float
                 except (ValueError, TypeError): continue
         else: logging.warning(f"Unexpected format {type(holders_data)} for major holders {ticker_obj.ticker}")
         if pct_insiders is None and pct_institutions is None: return None
         data = {'ticker': ticker_obj.ticker, 'pct_insiders': pct_insiders, 'pct_institutions': pct_institutions, 'fetch_timestamp': datetime.now(timezone.utc)}
         return pd.DataFrame([data])
     except Exception as e: logging.error(f"Failed major holders {ticker_obj.ticker}: {e}", exc_info=True); return None

def _process_holders_df(df: pd.DataFrame, ticker: str) -> Optional[pd.DataFrame]:
    # *** FIX: Correct rename map key 'pctHeld' ***
    if df is None or df.empty: return None
    expected_cols_map = { 'Date Reported': 'report_date', 'Holder': 'holder', 'Shares': 'shares', 'pctHeld': 'pct_out', 'Value': 'value'}
    logging.debug(f"Processing holders {ticker}. In cols: {list(df.columns)}")
    cols_to_keep = [yf_col for yf_col in expected_cols_map if yf_col in df.columns]
    if not all(ec in cols_to_keep for ec in ['Date Reported', 'Holder', 'Shares']): # Check required minimums *before* processing
         logging.warning(f"Holder DF {ticker} missing core cols. Has: {list(df.columns)}")
         return None
    df_processed = df[cols_to_keep].copy(); df_processed.rename(columns=expected_cols_map, inplace=True)
    df_processed['ticker'] = ticker; df_processed['fetch_timestamp'] = datetime.now(timezone.utc)
    if 'report_date' in df_processed.columns: df_processed['report_date'] = pd.to_datetime(df_processed['report_date'], errors='coerce').dt.date
    if 'pct_out' in df_processed.columns: df_processed['pct_out'] = pd.to_numeric(df_processed['pct_out'], errors='coerce')
    if 'shares' in df_processed.columns: df_processed['shares'] = pd.to_numeric(df_processed['shares'], errors='coerce').astype('Int64')
    if 'value' in df_processed.columns: df_processed['value'] = pd.to_numeric(df_processed['value'], errors='coerce').astype('Int64')
    pk_cols = ['ticker', 'report_date', 'holder']; df_processed.dropna(subset=[pk for pk in pk_cols if pk in df_processed.columns], inplace=True)
    if df_processed.empty: return None
    db_cols = ['ticker', 'report_date', 'holder', 'shares', 'pct_out', 'value', 'fetch_timestamp']
    return df_processed[[c for c in db_cols if c in df_processed.columns]]

def fetch_institutional_holders(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    logging.debug(f"Fetching inst holders {ticker_obj.ticker}")
    try: holders = ticker_obj.institutional_holders; return _process_holders_df(holders, ticker_obj.ticker)
    except Exception as e: logging.error(f"Failed inst holders {ticker_obj.ticker}: {e}", exc_info=False); return None

def fetch_mutualfund_holders(ticker_obj: yf.Ticker) -> Optional[pd.DataFrame]:
    logging.debug(f"Fetching MF holders {ticker_obj.ticker}")
    try: holders = ticker_obj.mutualfund_holders; return _process_holders_df(holders, ticker_obj.ticker)
    except Exception as e: logging.error(f"Failed MF holders {ticker_obj.ticker}: {e}", exc_info=False); return None

# --- Financial Fact Processing ---
def process_and_load_financial_facts( con: duckdb.DuckDBPyConnection, raw_df: pd.DataFrame, ticker: str, statement_type: str, frequency: str, mode: str ) -> bool:
    # *** FIX: Correct KeyError ['item_label'] ***
    if raw_df is None or raw_df.empty: logging.debug(f"Raw DF empty {ticker} {frequency} {statement_type}."); return True
    logging.info(f"Processing {frequency} {statement_type} for {ticker} ({len(raw_df)} items)")
    table_name = FIN_FACTS_TABLE_NAME; pk_cols = ['ticker', 'report_date', 'frequency', 'item_label']
    try:
        # Assign index name FIRST
        raw_df.index.name = 'item_label'
        # Minimal cleaning of index values
        raw_df.index = raw_df.index.map(lambda x: str(x).strip() if pd.notna(x) else None)
        # Drop rows where the label is None or empty (violates PK) - uses index name assigned above
        raw_df.dropna(axis=0, subset=['item_label'], inplace=True)
        raw_df = raw_df[raw_df.index != ''] # Check for empty string after strip
        if raw_df.empty: logging.warning(f"Index clean removed rows {ticker} {frequency} {statement_type}."); return True
        # Melt DataFrame
        long_df = raw_df.reset_index().melt(id_vars=['item_label'], var_name='report_date', value_name='value')
        # Clean and Convert Data Types
        long_df['report_date'] = pd.to_datetime(long_df['report_date'], errors='coerce').dt.date
        long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce')
        # Add Metadata Columns BEFORE dropping NAs based on PK
        long_df['ticker'] = ticker; long_df['frequency'] = frequency; long_df['statement_type'] = statement_type; long_df['fetch_timestamp'] = datetime.now(timezone.utc)
        # Drop only if essential PK components are missing (value can be NaN)
        long_df.dropna(subset=['ticker', 'report_date', 'frequency', 'item_label'], inplace=True)
        if long_df.empty: logging.warning(f"Melt/PK clean removed rows {ticker} {frequency} {statement_type}."); return True
        # Prepare for Loading
        db_cols = get_db_columns(con, table_name)
        if not db_cols: logging.error(f"Cannot load {table_name}, get cols failed."); return False
        final_df = long_df[[col for col in db_cols if col in long_df.columns]].copy()
        for col in db_cols: # Add any missing DB cols as None
            if col not in final_df.columns: final_df[col] = None
        final_df = final_df[db_cols]; final_df = final_df.astype(object).where(pd.notnull(final_df), None)
        if final_df[pk_cols].isnull().values.any(): logging.error(f"Load fail {ticker} {table_name}: PK NULL."); logging.debug(f"Problem PKs:\n {final_df[final_df[pk_cols].isnull().any(axis=1)][pk_cols]}"); return False
        # Load using INSERT OR REPLACE
        reg_name = f'{table_name}_reg_{int(time.time()*1000)}'; con.register(reg_name, final_df)
        cols_str = ", ".join([f'"{c}"' for c in db_cols])
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}" if mode != 'full_refresh' else f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}"
        con.execute(sql); con.unregister(reg_name); logging.debug(f"Loaded {len(final_df)} facts {table_name} {ticker} {frequency} {statement_type}."); return True
    except Exception as e: logging.error(f"Failed process/load {frequency} {statement_type} {ticker}: {e}", exc_info=True); return False


# --- Main Pipeline Function ---
def run_stock_info_pipeline(
    mode: str = 'initial_load',
    target_tickers: Optional[List[str]] = None,
    db_file_path_str: str = str(DEFAULT_DB_FILE),
    update_profile_if_older_than_days: int = 30
):
    """Main function including ISIN, Calendar, Recs, Holders."""
    start_run_time = time.time()
    logging.info(f"--- Starting Stock Info Pipeline (v3.9 - Focused Fixes) ---") # Version bump
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
        load_failed_flag = False # Reset flags per ticker
        profile_df, metrics_df, actions_df = None, None, None
        cal_df, recs_df, major_df, inst_df, mf_df = None, None, None, None, None
        raw_fin_dfs = {}
        fetch_results = {} # Track success of individual fetches

        try:
            ticker_obj = yf.Ticker(ticker)

            # --- Fetch Phase ---
            # Fetch Profile, ISIN & Metrics
            needs_profile_fetch = True; last_fetch_ts = profile_timestamps.get(ticker)
            if last_fetch_ts and update_profile_if_older_than_days >= 0 and (datetime.now(timezone.utc) - last_fetch_ts) < timedelta(days=update_profile_if_older_than_days): needs_profile_fetch = False; logging.debug(f"Skip profile fetch {ticker}.")
            if needs_profile_fetch:
                info_dict = fetch_company_info(ticker_obj); fetch_results['info'] = info_dict is not None
                if info_dict is None: fetch_errors_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Info Fetch Error', 'fetch_company_info None')
                else:
                    try: # Prepare DFs
                        isin = fetch_isin(ticker_obj); fetch_results['isin'] = isin is not None
                        if isin is None: fetch_errors_count += 1
                        db_prof_cols = get_db_columns(write_conn, PROFILE_TABLE_NAME); prof_data = {k: info_dict.get(k) for k in db_prof_cols if k not in ['fetch_timestamp', 'cik', 'isin']}; prof_data['ticker'] = ticker; prof_data['fetch_timestamp'] = datetime.now(timezone.utc); prof_data['isin'] = isin
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
                        fetch_results['metrics'] = True # Mark as success if prep doesn't fail
                    except Exception as prep_e: logging.error(f"Error prep profile/metrics {ticker}: {prep_e}", exc_info=True); fetch_errors_count += 1; log_info_fetch_error(write_conn, None, ticker, 'Data Prep Error', f'Failed prep profile/metrics: {prep_e}'); profile_df, metrics_df = None, None; fetch_results['info']=False; fetch_results['metrics']=False

            # Fetch Financials (Raw)
            fin_map_keys = [ ('Income Statement', 'annual'), ('Income Statement', 'quarterly'), ('Balance Sheet', 'annual'), ('Balance Sheet', 'quarterly'), ('Cash Flow', 'annual'), ('Cash Flow', 'quarterly') ]
            yfinance_stmt_map = {'Income Statement':'financials', 'Balance Sheet':'balance_sheet', 'Cash Flow':'cashflow'}
            for stmt_type, freq in fin_map_keys:
                 yfinance_type = yfinance_stmt_map[stmt_type]
                 raw_df = fetch_financial_statement(ticker_obj, yfinance_type, freq)
                 fetch_results[f'fin_{stmt_type}_{freq}'] = raw_df is not None
                 if raw_df is None: fetch_errors_count += 1
                 raw_fin_dfs[(stmt_type, freq)] = raw_df

            # Fetch Actions, Calendar, Recs, Holders
            actions_df = fetch_stock_actions(ticker_obj);         fetch_results['actions'] = actions_df is not None;       if actions_df is None: fetch_errors_count += 1
            cal_df = fetch_calendar(ticker_obj);                 fetch_results['calendar'] = cal_df is not None;       if cal_df is None: fetch_errors_count += 1
            recs_df = fetch_recommendations(ticker_obj);        fetch_results['recs'] = recs_df is not None;          if recs_df is None: fetch_errors_count += 1
            major_df = fetch_major_holders(ticker_obj);         fetch_results['major_h'] = major_df is not None;      if major_df is None: fetch_errors_count += 1
            inst_df = fetch_institutional_holders(ticker_obj);  fetch_results['inst_h'] = inst_df is not None;       if inst_df is None: fetch_errors_count += 1
            mf_df = fetch_mutualfund_holders(ticker_obj);       fetch_results['mf_h'] = mf_df is not None;         if mf_df is None: fetch_errors_count += 1

            # Determine if *any* data was fetched successfully
            any_data_fetched = any(fetch_results.values())

            # --- Load Phase ---
            if not any_data_fetched: logging.warning(f"Skip load {ticker} due to all fetches failing or returning no data."); skipped_tickers_count += 1; continue

            try:
                write_conn.begin(); load_failed_flag = False # Start transaction
                # Load Profile & Metrics
                if needs_profile_fetch and profile_df is not None and not profile_df.empty:
                    if not load_generic_data(write_conn, profile_df, PROFILE_TABLE_NAME, ['ticker'], mode): load_failed_flag = True
                if needs_profile_fetch and metrics_df is not None and not metrics_df.empty:
                     if not load_generic_data(write_conn, metrics_df, METRICS_TABLE_NAME, ['ticker', 'fetch_date'], mode): load_failed_flag = True

                # Process and Load Financial Facts
                for (stmt_type, freq), raw_df_fin in raw_fin_dfs.items():
                    if raw_df_fin is not None:
                        if not process_and_load_financial_facts(write_conn, raw_df_fin, ticker, stmt_type, freq, mode):
                            load_failed_flag = True

                # Load Actions, Calendar, Recs, Holders (Only if data exists)
                if actions_df is not None and not actions_df.empty:
                    if not load_generic_data(write_conn, actions_df, ACTIONS_TABLE_NAME, ['ticker', 'action_date', 'action_type'], mode): load_failed_flag = True
                if cal_df is not None and not cal_df.empty:
                    if not load_generic_data(write_conn, cal_df, CALENDAR_TABLE_NAME, ['ticker', 'event_type'], mode): load_failed_flag = True
                if recs_df is not None and not recs_df.empty:
                    if not load_generic_data(write_conn, recs_df, RECS_TABLE_NAME, ['ticker', 'recommendation_timestamp', 'firm'], mode): load_failed_flag = True
                if major_df is not None and not major_df.empty:
                     if not load_generic_data(write_conn, major_df, HOLDERS_MAJOR_TABLE_NAME, ['ticker'], mode): load_failed_flag = True
                if inst_df is not None and not inst_df.empty:
                     if not load_generic_data(write_conn, inst_df, HOLDERS_INST_TABLE_NAME, ['ticker', 'report_date', 'holder'], mode): load_failed_flag = True
                if mf_df is not None and not mf_df.empty:
                     if not load_generic_data(write_conn, mf_df, HOLDERS_MF_TABLE_NAME, ['ticker', 'report_date', 'holder'], mode): load_failed_flag = True

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