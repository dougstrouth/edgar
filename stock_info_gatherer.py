# -*- coding: utf-8 -*-
"""
Stock Info Gatherer Script (v3 - Fetch, Parse, Load - Final with Modes & DB Tickers)

Fetches supplementary company info using yfinance, parses it into
structured DataFrames, and loads it into DuckDB tables. Supports 'append'
(INSERT OR REPLACE) and 'full_refresh' (DELETE THEN INSERT) modes.
*** Fetches tickers to process primarily from the 'tickers' table in the DB. ***
*** Formats CIK from profile data to 10 digits padded. ***

Handles Profile/Metrics, Actions, Financials, Holders, Recs, Calendar.
Uses utility modules for config and logging. Implements DB storage.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timezone, timedelta

import pandas as pd
import yfinance as yf
import duckdb

# --- Import Utilities ---
try:
    from config_utils import AppConfig
    from logging_utils import setup_logging
    from database_conn import ManagedDatabaseConnection
except ImportError as e: print(f"FATAL: Could not import utility modules: {e}", file=sys.stderr); sys.exit(1)

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
YFINANCE_DELAY = 0.7
INFO_TABLES = ["yf_profile_metrics", "yf_stock_actions", "yf_major_holders", "yf_institutional_holders", "yf_mutual_fund_holders", "yf_recommendations", "yf_calendar_events", "yf_financial_facts"]
ERROR_TABLE_NAME = "stock_fetch_errors"

# --- Fetcher Functions (Finalized Versions) ---
# fetch_profile_metrics, fetch_actions, fetch_financials, fetch_holders, fetch_recommendations, fetch_calendar
# (Same as previous version)
def fetch_profile_metrics(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch .info for {ticker_symbol}")
    try:
        info_data = ticker_obj.info
        if not info_data: logger.warning(f"No .info dictionary returned for {ticker_symbol}."); return None
        if isinstance(info_data, dict) and ('symbol' in info_data or 'longName' in info_data or len(info_data)>1): logger.info(f"Successfully fetched .info for {ticker_symbol} (Keys: {len(info_data)})"); return info_data
        else: logger.warning(f"Empty or unexpected format for .info for {ticker_symbol}: {info_data}"); return None
    except Exception as e: logger.error(f"Failed fetching .info for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); return None

def fetch_actions(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[pd.DataFrame]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch actions for {ticker_symbol}")
    try:
        actions = ticker_obj.actions
        if actions is None or actions.empty: logger.info(f"No actions data found for {ticker_symbol}."); return None
        logger.info(f"Successfully fetched actions data for {ticker_symbol} (Entries: {len(actions)})"); return actions
    except Exception as e: logger.error(f"Failed fetch actions for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); return None

def fetch_financials(ticker_obj: yf.Ticker, logger: logging.Logger) -> Dict[str, Optional[pd.DataFrame]]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch financial statements for {ticker_symbol}"); fetched_data = {}
    statement_map = { 'financials_annual': (ticker_obj.financials, 'annual'), 'financials_quarterly': (ticker_obj.quarterly_financials, 'quarterly'), 'balance_sheet_annual': (ticker_obj.balance_sheet, 'annual'), 'balance_sheet_quarterly': (ticker_obj.quarterly_balance_sheet, 'quarterly'), 'cashflow_annual': (ticker_obj.cashflow, 'annual'), 'cashflow_quarterly': (ticker_obj.quarterly_cashflow, 'quarterly'), }
    for name, (method, freq) in statement_map.items():
        data = None
        try:
            data = method
            if data is not None and not data.empty: logger.info(f"Successfully fetched {name} for {ticker_symbol} (Shape: {data.shape})"); fetched_data[name] = data
            else: logger.info(f"No data found for {name} for {ticker_symbol}."); fetched_data[name] = None
        except Exception as e: logger.error(f"Failed fetch {name} for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); fetched_data[name] = None
        time.sleep(0.1)
    return fetched_data

def fetch_holders(ticker_obj: yf.Ticker, logger: logging.Logger) -> Dict[str, Optional[Any]]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch holders for {ticker_symbol}"); fetched_data = {'major': None, 'institutional': None, 'mutualfund': None}
    holder_map = { 'major': ticker_obj.major_holders, 'institutional': ticker_obj.institutional_holders, 'mutualfund': ticker_obj.mutualfund_holders, }
    for name, data_property in holder_map.items():
         data = None
         try:
            data = data_property
            if data is not None and not data.empty: logger.info(f"Successfully fetched {name}_holders for {ticker_symbol} (Shape/Len: {getattr(data,'shape',len(data) if data is not None else 'N/A')})"); fetched_data[name] = data
            else: logger.info(f"No data found for {name}_holders for {ticker_symbol}."); fetched_data[name] = None
         except Exception as e: logger.error(f"Failed fetch {name}_holders for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); fetched_data[name] = None
         time.sleep(0.1)
    return fetched_data

def fetch_recommendations(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[pd.DataFrame]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch recommendations for {ticker_symbol}")
    try:
        recs = ticker_obj.recommendations
        if recs is None or recs.empty: logger.info(f"No recommendations data found for {ticker_symbol}."); return None
        logger.info(f"Successfully fetched recommendations for {ticker_symbol} (Entries: {len(recs)})"); return recs
    except Exception as e: logger.error(f"Failed fetch recommendations for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); return None

def fetch_calendar(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[Any]:
    ticker_symbol = ticker_obj.ticker; logger.debug(f"Attempting to fetch calendar for {ticker_symbol}")
    try:
        cal_data = ticker_obj.calendar; is_empty = False
        if cal_data is None: is_empty = True
        elif isinstance(cal_data, pd.DataFrame): is_empty = cal_data.empty
        elif isinstance(cal_data, dict): is_empty = not cal_data
        else: logger.warning(f"Unexpected data type for calendar {ticker_symbol}: {type(cal_data)}"); is_empty = True
        if is_empty: logger.info(f"No calendar data found for {ticker_symbol}."); return None
        logger.info(f"Successfully fetched calendar data for {ticker_symbol}"); return cal_data
    except Exception as e: logger.error(f"Failed fetch calendar for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False); return None


# --- Parsing Functions (CIK Formatting Added) ---
# parse_actions, parse_major_holders, _parse_detailed_holders,
# parse_institutional_holders, parse_mutual_fund_holders,
# parse_recommendations, parse_calendar, parse_financial_statement remain the same
# Only parse_profile_metrics is modified below

def parse_profile_metrics(info_dict: Optional[Dict], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Parses the .info dictionary into a DataFrame for the yf_profile_metrics table.
    **Formats CIK to 10 digits, zero-padded.**
    """
    if not info_dict: return None; logger.debug(f"Parsing .info data for {ticker}")
    profile_cols = ['cik', 'isin', 'quoteType', 'exchange', 'longName', 'sector', 'industry', 'longBusinessSummary', 'fullTimeEmployees', 'website', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 'overallRisk', 'governanceEpochDate', 'compensationAsOfEpochDate', 'marketCap', 'enterpriseValue', 'beta', 'trailingPE', 'forwardPE', 'priceToBook', 'priceToSalesTrailing12Months', 'enterpriseToRevenue', 'enterpriseToEbitda', 'fiftyTwoWeekHigh', 'fiftyTwoWeekLow', 'sharesOutstanding', 'floatShares', 'bookValue', 'dividendRate', 'dividendYield', 'exDividendDate', 'lastFiscalYearEnd', 'nextFiscalYearEnd', 'mostRecentQuarter', 'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'lastDividendValue', 'lastDividendDate']
    parsed_data = {'ticker': ticker.upper(), 'fetch_timestamp': fetch_ts} # Use upper ticker
    for col in profile_cols:
        parsed_data[col] = info_dict.get(col)
        # --- ADDED CIK Formatting ---
        if col == 'cik' and parsed_data[col] is not None:
            try:
                parsed_data[col] = str(parsed_data[col]).zfill(10)
                logger.debug(f"Formatted CIK for {ticker}: {parsed_data[col]}")
            except Exception as cik_e:
                logger.warning(f"Could not format CIK '{info_dict.get('cik')}' for {ticker}: {cik_e}")
                parsed_data[col] = None # Set to None if formatting fails
        # --- End CIK Formatting ---

    for date_key in ['exDividendDate', 'lastFiscalYearEnd', 'nextFiscalYearEnd', 'mostRecentQuarter']:
         if date_key in parsed_data and parsed_data[date_key] is not None:
             try:
                 if isinstance(parsed_data[date_key], (int, float)): parsed_data[date_key] = datetime.fromtimestamp(parsed_data[date_key], timezone.utc).date()
                 else: parsed_data[date_key] = pd.to_datetime(parsed_data[date_key], errors='coerce').date()
             except (TypeError, ValueError, OSError) as e: logger.warning(f"Could not parse date '{parsed_data[date_key]}' for {date_key} in {ticker}: {e}"); parsed_data[date_key] = None
    try: df = pd.DataFrame([parsed_data])
    except Exception as df_e: logger.error(f"Failed to create DataFrame from parsed_data for {ticker}: {df_e}", exc_info=True); return None
    int_keys = ['fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 'overallRisk', 'marketCap', 'enterpriseValue', 'sharesOutstanding', 'floatShares', 'governanceEpochDate', 'compensationAsOfEpochDate', 'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'lastDividendDate']
    for int_key in int_keys:
         if int_key in df.columns:
             try: df[int_key] = pd.to_numeric(df[int_key], errors='coerce').astype('Int64')
             except (ValueError, TypeError) as e: logger.warning(f"Could not convert column {int_key} to Int64 for {ticker}: {e}. Values may be pd.NA."); pass
    # Ensure ticker column is upper case in final DF
    if 'ticker' in df.columns: df['ticker'] = df['ticker'].str.upper()
    return df

# Include the other finalized parsing functions here...
def parse_actions(actions_df: Optional[pd.DataFrame], ticker: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
    if actions_df is None or actions_df.empty: return None; logger.debug(f"Parsing actions data for {ticker}")
    try:
        actions = actions_df.copy(); actions.index.name = 'action_date'; actions.reset_index(inplace=True); actions['ticker'] = ticker.upper()
        actions['action_date'] = pd.to_datetime(actions['action_date'], errors='coerce', utc=True).dt.date
        actions_melted = actions.melt(id_vars=['ticker', 'action_date'], value_vars=['Dividends', 'Stock Splits'], var_name='action_type', value_name='value')
        actions_melted = actions_melted[actions_melted['value'] != 0.0]; actions_melted.dropna(subset=['value', 'action_date', 'action_type'], inplace=True)
        if actions_melted.empty: logger.info(f"No non-zero actions found after parsing for {ticker}"); return None
        actions_melted['action_type'] = actions_melted['action_type'].replace({'Dividends': 'Dividend', 'Stock Splits': 'Split'})
        actions_melted['value'] = pd.to_numeric(actions_melted['value'], errors='coerce'); actions_melted.dropna(subset=['value'], inplace=True)
        final_cols = ['ticker', 'action_date', 'action_type', 'value']; return actions_melted[final_cols]
    except Exception as e: logger.error(f"Failed to parse actions for {ticker}: {e}", exc_info=True); return None

def parse_major_holders(major_holders_data: Optional[Any], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    if major_holders_data is None: return None; logger.debug(f"Parsing major holders data for {ticker} (type: {type(major_holders_data)})")
    pct_insiders = None; pct_institutions = None
    if isinstance(major_holders_data, pd.DataFrame) and not major_holders_data.empty:
        if major_holders_data.shape[1] >= 1:
            val_col_idx = 0; lbl_col_idx = 0; val_col_name = major_holders_data.columns[val_col_idx]
            logger.debug(f"Using index for labels and column '{val_col_name}' (idx {val_col_idx}) for values.")
            label_source = major_holders_data.index
            for i, label_str_raw in enumerate(label_source):
                label_str = str(label_str_raw).lower(); value_data = major_holders_data.iloc[i, val_col_idx]
                try:
                    value_float = float(value_data)
                    if not pd.isna(value_float):
                        if 'insider' in label_str: pct_insiders = value_float; logger.debug(f"Parsed pct_insiders: {pct_insiders} from label '{label_str}'")
                        elif 'institutionspercentheld' == label_str.replace(' ',''): pct_institutions = value_float; logger.debug(f"Parsed pct_institutions: {pct_institutions} from label '{label_str}'")
                except (ValueError, TypeError) as e: logger.warning(f"Could not parse value '{value_data}' for label '{label_str}' in {ticker}: {e}"); continue
        else: logger.warning(f"Major holders DataFrame for {ticker} has zero columns. Cannot parse.")
    elif isinstance(major_holders_data, list): logger.warning(f"Major holders for {ticker} was a list, parsing not implemented yet.")
    else: logger.warning(f"Unexpected data type for major holders {ticker}: {type(major_holders_data)}")
    if pct_insiders is not None or pct_institutions is not None:
        data = { 'ticker': ticker.upper(), 'fetch_timestamp': fetch_ts, 'pct_insiders': pct_insiders, 'pct_institutions': pct_institutions }
        df = pd.DataFrame([data]); df['pct_insiders'] = df['pct_insiders'].astype('float64'); df['pct_institutions'] = df['pct_institutions'].astype('float64'); return df
    else:
        if isinstance(major_holders_data, pd.DataFrame) and not major_holders_data.empty: logger.warning(f"Parsing major_holders for {ticker} yielded no insider or institution percentages.")
        return None

def _parse_detailed_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    if holders_df is None or holders_df.empty: return None; logger.debug(f"Parsing detailed holders data for {ticker}")
    try:
        holders = holders_df.copy(); col_map = {'holder': 'holder', 'shares': 'shares', '% out': 'pct_out', 'date reported': 'report_date', 'value': 'value'}; rename_dict = {}; found_cols = []
        for yf_col_name in holders.columns:
            yf_col_lower = str(yf_col_name).lower()
            for map_key, db_col in col_map.items():
                 if yf_col_lower == map_key or (map_key == '% out' and yf_col_lower == 'pctheld'): rename_dict[yf_col_name] = db_col; found_cols.append(db_col); break
        if not all(c in found_cols for c in ['holder', 'report_date', 'shares']): logger.warning(f"Missing essential columns ('holder', 'report_date', 'shares') in detailed holders for {ticker}. Found: {list(holders.columns)}"); return None
        holders.rename(columns=rename_dict, inplace=True); holders['ticker'] = ticker.upper(); holders['fetch_timestamp'] = fetch_ts
        holders['report_date'] = pd.to_datetime(holders['report_date'], errors='coerce').dt.date; holders['shares'] = pd.to_numeric(holders['shares'], errors='coerce').astype('Int64')
        if 'pct_out' in holders.columns: holders['pct_out'] = pd.to_numeric(holders['pct_out'], errors='coerce')
        if 'value' in holders.columns: holders['value'] = pd.to_numeric(holders['value'], errors='coerce').astype('Int64')
        required_cols = ['ticker', 'holder', 'report_date']; holders.dropna(subset=[col for col in required_cols if col in holders.columns], inplace=True)
        if holders.empty: return None
        db_cols = ['ticker', 'holder', 'report_date', 'fetch_timestamp', 'shares', 'pct_out', 'value']; final_df = holders[[col for col in db_cols if col in holders.columns]].copy(); return final_df if not final_df.empty else None
    except Exception as e: logger.error(f"Failed to parse detailed holders for {ticker}: {e}", exc_info=True); return None

def parse_institutional_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    return _parse_detailed_holders(holders_df, ticker, fetch_ts, logger)

def parse_mutual_fund_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    return _parse_detailed_holders(holders_df, ticker, fetch_ts, logger)

def parse_recommendations(recs_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses the raw recommendations DataFrame with stricter firm filtering."""
    if recs_df is None or recs_df.empty: return None
    logger.debug(f"Parsing recommendations data for {ticker}")

    # --- Add temporary debug print for raw columns ---
    print(f"DEBUG Recommendation Raw Columns for {ticker}: {recs_df.columns.tolist()}")
    # ---

    try:
        recs = recs_df.copy(); recs.index.name = 'recommendation_timestamp'; recs.reset_index(inplace=True); recs['ticker'] = ticker.upper(); recs['fetch_timestamp'] = fetch_ts
        col_map = {'firm': 'firm', 'to grade': 'to_grade', 'from grade': 'from_grade', 'action': 'action'}; rename_dict = {}; current_cols_lower = {str(c).lower(): c for c in recs.columns}
        for map_key_lower, db_col in col_map.items():
             if map_key_lower in current_cols_lower: rename_dict[current_cols_lower[map_key_lower]] = db_col
        recs.rename(columns=rename_dict, inplace=True)

        # --- Defensive Check: Ensure 'firm' column exists AFTER rename attempt ---
        if 'firm' not in recs.columns:
            logger.error(f"Critical: 'firm' column ('Firm' in source?) missing after rename attempt for {ticker}. Cannot process recommendations.")
            return None # Return None if 'firm' doesn't exist
        # --- End Defensive Check ---

        if 'recommendation_timestamp' in recs.columns:
             recs['recommendation_timestamp'] = pd.to_datetime(recs['recommendation_timestamp'], errors='coerce', utc=True)
             if recs['recommendation_timestamp'].dt.tz is None: logger.warning(f"Recommendation timestamp for {ticker} was timezone naive, assuming UTC."); recs['recommendation_timestamp'] = recs['recommendation_timestamp'].dt.tz_localize('UTC')
             else: recs['recommendation_timestamp'] = recs['recommendation_timestamp'].dt.tz_convert('UTC')

        pk_cols = ['ticker', 'recommendation_timestamp', 'firm'];
        # Ensure PK columns are not null
        recs.dropna(subset=[col for col in pk_cols if col in recs.columns], inplace=True)
        # Also ensure firm is not an empty string after potential conversions
        recs = recs[recs['firm'].astype(str).str.strip() != '']

        if recs.empty: logger.info(f"No valid recommendation rows remaining after final firm filter for {ticker}"); return None

        db_cols = ['ticker', 'recommendation_timestamp', 'firm', 'fetch_timestamp', 'to_grade', 'from_grade', 'action'];
        final_df = recs[[col for col in db_cols if col in recs.columns]].copy(); return final_df
    except Exception as e: logger.error(f"Failed to parse recommendations for {ticker}: {e}", exc_info=True); return None

def parse_calendar(cal_data: Optional[Any], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    if cal_data is None or (isinstance(cal_data, dict) and not cal_data) or (isinstance(cal_data, pd.DataFrame) and cal_data.empty): return None
    logger.debug(f"Parsing calendar data for {ticker} (type: {type(cal_data)})"); parsed_events = []
    try:
        if isinstance(cal_data, pd.DataFrame):
            cal_data.columns = [str(c).replace(' ','') for c in cal_data.columns]
            if 'EarningsDate' in cal_data.columns and isinstance(cal_data['EarningsDate'].iloc[0], (list, tuple)) and len(cal_data['EarningsDate'].iloc[0]) > 0:
                 start_ts = cal_data['EarningsDate'].iloc[0][0]; end_ts = cal_data['EarningsDate'].iloc[0][1] if len(cal_data['EarningsDate'].iloc[0]) > 1 else start_ts
                 start_date = pd.to_datetime(start_ts, errors='coerce', unit='s').date() if pd.notna(start_ts) else None; end_date = pd.to_datetime(end_ts, errors='coerce', unit='s').date() if pd.notna(end_ts) else start_date
                 if start_date: parsed_events.append({'event_type': 'Earnings', 'event_start_date': start_date, 'event_end_date': end_date, 'earnings_avg': cal_data['EarningsAverage'].iloc[0] if 'EarningsAverage' in cal_data.columns else None, 'earnings_low': cal_data['EarningsLow'].iloc[0] if 'EarningsLow' in cal_data.columns else None, 'earnings_high': cal_data['EarningsHigh'].iloc[0] if 'EarningsHigh' in cal_data.columns else None, 'revenue_avg': cal_data['RevenueAverage'].iloc[0] if 'RevenueAverage' in cal_data.columns else None, 'revenue_low': cal_data['RevenueLow'].iloc[0] if 'RevenueLow' in cal_data.columns else None, 'revenue_high': cal_data['RevenueHigh'].iloc[0] if 'RevenueHigh' in cal_data.columns else None,})
            if 'ExDividendDate' in cal_data.columns and pd.notna(cal_data['ExDividendDate'].iloc[0]):
                ex_div_date = pd.to_datetime(cal_data['ExDividendDate'].iloc[0], errors='coerce', unit='s').date()
                if ex_div_date: parsed_events.append({'event_type': 'ExDividendDate', 'event_start_date': ex_div_date, 'event_end_date': ex_div_date})
        elif isinstance(cal_data, dict):
            earnings_info = cal_data.get('Earnings')
            if isinstance(earnings_info, dict) and 'Earnings Date' in earnings_info and isinstance(earnings_info['Earnings Date'], (list, tuple)) and len(earnings_info['Earnings Date']) > 0:
                start_ts = earnings_info['Earnings Date'][0]; end_ts = earnings_info['Earnings Date'][1] if len(earnings_info['Earnings Date']) > 1 else start_ts
                start_date = pd.to_datetime(start_ts, errors='coerce', unit='s').date() if pd.notna(start_ts) else None; end_date = pd.to_datetime(end_ts, errors='coerce', unit='s').date() if pd.notna(end_ts) else start_date
                if start_date: parsed_events.append({'event_type': 'Earnings', 'event_start_date': start_date, 'event_end_date': end_date, 'earnings_avg': earnings_info.get('Earnings Average'), 'earnings_low': earnings_info.get('Earnings Low'), 'earnings_high': earnings_info.get('Earnings High'), 'revenue_avg': earnings_info.get('Revenue Average'), 'revenue_low': earnings_info.get('Revenue Low'), 'revenue_high': earnings_info.get('Revenue High'),})
            ex_div_ts = cal_data.get('Ex-Dividend Date')
            if pd.notna(ex_div_ts):
                ex_div_date = pd.to_datetime(ex_div_ts, errors='coerce', unit='s').date()
                if ex_div_date: parsed_events.append({'event_type': 'ExDividendDate', 'event_start_date': ex_div_date, 'event_end_date': ex_div_date})
        if not parsed_events: logger.info(f"No parsable calendar events found for {ticker}"); return None
        df = pd.DataFrame(parsed_events); df['ticker'] = ticker.upper(); df['fetch_timestamp'] = fetch_ts
        num_cols = ['earnings_avg', 'earnings_low', 'earnings_high', 'revenue_avg', 'revenue_low', 'revenue_high']
        for col in num_cols:
             if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce'); df[col] = df[col].astype('Int64') if 'revenue' in col else df[col].astype('float64') # Ensure numeric types
        db_cols = ['ticker', 'event_type', 'fetch_timestamp', 'event_start_date', 'event_end_date', 'earnings_avg', 'earnings_low', 'earnings_high', 'revenue_avg', 'revenue_low', 'revenue_high']
        final_df = df[[col for col in db_cols if col in df.columns]].copy(); return final_df if not final_df.empty else None
    except Exception as e: logger.error(f"Failed to parse calendar data for {ticker}: {e}", exc_info=True); return None

def parse_financial_statement(stmt_df: Optional[pd.DataFrame], ticker: str, stmt_type: str, freq: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    if stmt_df is None or stmt_df.empty: return None; logger.debug(f"Parsing {freq} {stmt_type} for {ticker}")
    try:
        if stmt_df.index.name is None: 
            stmt_df.index.name = 'item_label'; 
        else: stmt_df.index.name = 'item_label'
        long_df = stmt_df.reset_index(); long_df = long_df.melt(id_vars=['item_label'], var_name='report_date', value_name='value')
        long_df['report_date'] = pd.to_datetime(long_df['report_date'], errors='coerce').dt.date; long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce')
        long_df['ticker'] = ticker.upper(); long_df['frequency'] = freq; long_df['statement_type'] = stmt_type; long_df['fetch_timestamp'] = fetch_ts
        pk_cols = ['ticker', 'report_date', 'frequency', 'item_label']; long_df.dropna(subset=[col for col in pk_cols if col in long_df.columns], inplace=True)
        if long_df.empty: logger.warning(f"No valid rows after melting/cleaning {freq} {stmt_type} for {ticker}"); return None
        db_cols = ['ticker', 'report_date', 'frequency', 'statement_type', 'item_label', 'fetch_timestamp', 'value']
        final_df = long_df[[col for col in db_cols if col in long_df.columns]].copy(); return final_df
    except Exception as e: logger.error(f"Failed to parse {freq} {stmt_type} for {ticker}: {e}", exc_info=True); return None


# --- Database Setup and Loading Functions ---
def setup_info_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger):
    """Creates/Alters tables for supplementary stock info using SCHEMA_SQL."""
    logger.info("Setting up supplementary info tables..."); SCHEMA_SQL = { "yf_profile_metrics": "CREATE TABLE IF NOT EXISTS yf_profile_metrics(ticker VARCHAR PRIMARY KEY COLLATE NOCASE, fetch_timestamp TIMESTAMPTZ NOT NULL, cik VARCHAR, isin VARCHAR, quoteType VARCHAR, exchange VARCHAR, longName VARCHAR, sector VARCHAR, industry VARCHAR, longBusinessSummary TEXT, fullTimeEmployees INTEGER, website VARCHAR, exchangeTimezoneName VARCHAR, exchangeTimezoneShortName VARCHAR, auditRisk INTEGER, boardRisk INTEGER, compensationRisk INTEGER, shareHolderRightsRisk INTEGER, overallRisk INTEGER, governanceEpochDate BIGINT, compensationAsOfEpochDate BIGINT, marketCap BIGINT, enterpriseValue BIGINT, beta DOUBLE, trailingPE DOUBLE, forwardPE DOUBLE, priceToBook DOUBLE, priceToSalesTrailing12Months DOUBLE, enterpriseToRevenue DOUBLE, enterpriseToEbitda DOUBLE, fiftyTwoWeekHigh DOUBLE, fiftyTwoWeekLow DOUBLE, sharesOutstanding BIGINT, floatShares BIGINT, bookValue DOUBLE, dividendRate DOUBLE, dividendYield DOUBLE, exDividendDate DATE, lastFiscalYearEnd DATE, nextFiscalYearEnd DATE, mostRecentQuarter DATE, earningsTimestamp BIGINT, earningsTimestampStart BIGINT, earningsTimestampEnd BIGINT, lastDividendValue DOUBLE, lastDividendDate BIGINT);", "yf_stock_actions": "CREATE TABLE IF NOT EXISTS yf_stock_actions(ticker VARCHAR NOT NULL COLLATE NOCASE, action_date DATE NOT NULL, action_type VARCHAR NOT NULL, value DOUBLE NOT NULL, PRIMARY KEY (ticker, action_date, action_type));", "yf_major_holders": "CREATE TABLE IF NOT EXISTS yf_major_holders(ticker VARCHAR PRIMARY KEY COLLATE NOCASE, fetch_timestamp TIMESTAMPTZ NOT NULL, pct_insiders DOUBLE, pct_institutions DOUBLE);", "yf_institutional_holders": "CREATE TABLE IF NOT EXISTS yf_institutional_holders(ticker VARCHAR NOT NULL COLLATE NOCASE, holder VARCHAR NOT NULL, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, shares BIGINT, pct_out DOUBLE, value BIGINT, PRIMARY KEY (ticker, holder, report_date));", "yf_mutual_fund_holders": "CREATE TABLE IF NOT EXISTS yf_mutual_fund_holders(ticker VARCHAR NOT NULL COLLATE NOCASE, holder VARCHAR NOT NULL, report_date DATE NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, shares BIGINT, pct_out DOUBLE, value BIGINT, PRIMARY KEY (ticker, holder, report_date));", "yf_recommendations": "CREATE TABLE IF NOT EXISTS yf_recommendations(ticker VARCHAR NOT NULL COLLATE NOCASE, recommendation_timestamp TIMESTAMPTZ NOT NULL, firm VARCHAR NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, to_grade VARCHAR, from_grade VARCHAR, action VARCHAR, PRIMARY KEY (ticker, recommendation_timestamp, firm));", "yf_calendar_events": "CREATE TABLE IF NOT EXISTS yf_calendar_events(ticker VARCHAR NOT NULL COLLATE NOCASE, event_type VARCHAR NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, event_start_date DATE, event_end_date DATE, earnings_avg DOUBLE, earnings_low DOUBLE, earnings_high DOUBLE, revenue_avg BIGINT, revenue_low BIGINT, revenue_high BIGINT, PRIMARY KEY (ticker, event_type));", "yf_financial_facts": "CREATE TABLE IF NOT EXISTS yf_financial_facts(ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, frequency VARCHAR NOT NULL, statement_type VARCHAR NOT NULL, item_label VARCHAR NOT NULL, fetch_timestamp TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION, PRIMARY KEY (ticker, report_date, frequency, item_label));", "stock_fetch_errors": "CREATE TABLE IF NOT EXISTS stock_fetch_errors(cik VARCHAR, ticker VARCHAR COLLATE NOCASE, error_timestamp TIMESTAMPTZ NOT NULL, error_type VARCHAR NOT NULL, error_message VARCHAR, start_date_req DATE, end_date_req DATE);" }
    try:
        for name, sql in SCHEMA_SQL.items(): con.execute(sql); logger.debug(f"Table '{name}' setup ok.")
        logger.info("Supplementary info tables setup complete.")
    except Exception as e: logger.error(f"Failed setup_info_tables: {e}", exc_info=True); raise

def get_db_columns(con: duckdb.DuckDBPyConnection, table_name: str, logger: logging.Logger) -> List[str]:
    """Gets column names for a table."""
    try: cols = con.execute(f"PRAGMA table_info('{table_name}');").fetchall(); return [col[1] for col in cols]
    except duckdb.Error as e: logger.error(f"DuckDB Error getting columns for {table_name}: {e}"); raise
    except Exception as e: logger.error(f"Unexpected error getting columns for {table_name}: {e}", exc_info=True); return []

def load_dataframe_to_db( db_conn: duckdb.DuckDBPyConnection, df: Optional[pd.DataFrame], table_name: str, primary_keys: List[str], logger: logging.Logger, mode: str = 'append') -> bool:
    """ Loads a DataFrame into the specified DuckDB table using INSERT OR REPLACE or plain INSERT."""
    if df is None or df.empty: logger.debug(f"DataFrame for '{table_name}' is empty or None. Skipping load."); return True
    ticker = df['ticker'].iloc[0].upper() if 'ticker' in df.columns else 'N/A'; reg_name = f'{table_name}_reg_{int(time.time()*1000)}'
    logger.info(f"Loading {len(df)} rows into {table_name} for {ticker} (Mode: {mode})")
    try:
        db_cols = get_db_columns(db_conn, table_name, logger)
        if not db_cols: logger.error(f"Cannot load data into {table_name}: Failed to get DB columns."); return False
        df_load = df[[col for col in db_cols if col in df.columns]].copy(); df_load['ticker'] = df_load['ticker'].str.upper() # Ensure ticker is upper case
        for col in db_cols:
            if col not in df_load.columns: df_load[col] = pd.NA
        df_load = df_load[db_cols]; df_load = df_load.astype(object).where(pd.notnull(df_load), None)
        db_conn.register(reg_name, df_load)
        cols_str = ", ".join([f'"{c}"' for c in db_cols])
        if mode == 'append': sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}"
        else: sql = f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {reg_name}" # For full_refresh
        db_conn.execute(sql); logger.debug(f"Successfully loaded {len(df_load)} rows into {table_name} for {ticker}."); return True
    except duckdb.Error as e: logger.error(f"DuckDB error loading {table_name} for {ticker}: {e}", exc_info=False); return False
    except Exception as e: logger.error(f"Unexpected error loading {table_name} for {ticker}: {e}", exc_info=True); return False
    finally:
        try: db_conn.unregister(reg_name)
        except Exception: pass

def clear_info_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger):
    """Deletes all data from tables managed by this script."""
    tables_to_clear = INFO_TABLES; logger.warning(f"Clearing data from tables: {', '.join(tables_to_clear)}")
    try:
        db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        for t in tables_to_clear:
            if t.lower() in db_tables: con.execute(f"DELETE FROM {t};"); logger.info(f"Cleared {t}.")
            else: logger.warning(f"Table {t} not found for clearing.")
        if ERROR_TABLE_NAME.lower() in db_tables:
             relevant_error_types = ['Info Fetch Error', 'Data Prep Error', 'Financials Fetch Error', 'Actions Fetch Error', 'Holders Fetch Error', 'Recs Fetch Error', 'Calendar Fetch Error', 'Load Failure', 'Load Transaction Error', 'Ticker Processing Error']
             placeholders = ', '.join(['?'] * len(relevant_error_types)); con.execute(f"DELETE FROM {ERROR_TABLE_NAME} WHERE error_type IN ({placeholders})", relevant_error_types); logger.info(f"Cleared relevant errors from {ERROR_TABLE_NAME}")
    except Exception as e: logger.error(f"Failed clear_info_tables: {e}", exc_info=True); raise

# --- Orchestration (Includes Parsing & Loading) ---

def gather_and_store_ticker_info( tickers: List[str], db_conn: duckdb.DuckDBPyConnection, logger: logging.Logger, mode: str = 'append'):
    """ Iterates through tickers, fetches all domains, parses the data, and loads it into the database within transactions."""
    logger.info(f"Starting Full Info gathering, parsing & loading for {len(tickers)} tickers (Mode: {mode}).")
    overall_success_count = 0; overall_fail_count = 0

    for ticker_symbol_raw in tickers:
        ticker_symbol = ticker_symbol_raw.upper() # Ensure using upper case
        logger.info(f"--- Processing ticker: {ticker_symbol} ---")
        raw_results = {}; parsed_dfs: Dict[str, Optional[pd.DataFrame]] = {}; fetch_ts = datetime.now(timezone.utc)
        # Ticker success flag moved inside transaction scope

        try: # Outer try for fetch/parse errors before transaction starts
            # --- Fetch Phase ---
            logger.debug(f"Fetching data for {ticker_symbol}...")
            ticker_obj = yf.Ticker(ticker_symbol)
            raw_results['profile'] = fetch_profile_metrics(ticker_obj, logger); time.sleep(0.1)
            raw_results['actions'] = fetch_actions(ticker_obj, logger); time.sleep(0.1)
            raw_results['financials'] = fetch_financials(ticker_obj, logger); time.sleep(0.1)
            raw_results['holders'] = fetch_holders(ticker_obj, logger); time.sleep(0.1)
            raw_results['recommendations'] = fetch_recommendations(ticker_obj, logger); time.sleep(0.1)
            raw_results['calendar'] = fetch_calendar(ticker_obj, logger)
            logger.info(f"Finished fetching for {ticker_symbol}.")

            # --- Parse Phase ---
            logger.info(f"Parsing data for {ticker_symbol}...")
            parsed_dfs['yf_profile_metrics'] = parse_profile_metrics(raw_results.get('profile'), ticker_symbol, fetch_ts, logger)
            parsed_dfs['yf_stock_actions'] = parse_actions(raw_results.get('actions'), ticker_symbol, logger)
            all_fin_facts = []
            if raw_results.get('financials'):
                 fin_map = {'Income Statement': ['financials_annual', 'financials_quarterly'], 'Balance Sheet': ['balance_sheet_annual', 'balance_sheet_quarterly'], 'Cash Flow': ['cashflow_annual', 'cashflow_quarterly']}
                 for stmt_type, keys in fin_map.items():
                      for key in keys:
                           raw_df = raw_results['financials'].get(key); freq = 'annual' if 'annual' in key else 'quarterly'
                           parsed_stmt_df = parse_financial_statement(raw_df, ticker_symbol, stmt_type, freq, fetch_ts, logger)
                           if parsed_stmt_df is not None: all_fin_facts.append(parsed_stmt_df)
            parsed_dfs['yf_financial_facts'] = pd.concat(all_fin_facts, ignore_index=True) if all_fin_facts else None
            holders_data = raw_results.get('holders')
            if holders_data:
                 parsed_dfs['yf_major_holders'] = parse_major_holders(holders_data.get('major'), ticker_symbol, fetch_ts, logger)
                 parsed_dfs['yf_institutional_holders'] = parse_institutional_holders(holders_data.get('institutional'), ticker_symbol, fetch_ts, logger)
                 parsed_dfs['yf_mutual_fund_holders'] = parse_mutual_fund_holders(holders_data.get('mutualfund'), ticker_symbol, fetch_ts, logger)
            else: parsed_dfs['yf_major_holders'] = parsed_dfs['yf_institutional_holders'] = parsed_dfs['yf_mutual_fund_holders'] = None
            parsed_dfs['yf_recommendations'] = parse_recommendations(raw_results.get('recommendations'), ticker_symbol, fetch_ts, logger)
            parsed_dfs['yf_calendar_events'] = parse_calendar(raw_results.get('calendar'), ticker_symbol, fetch_ts, logger)
            logger.info(f"Finished parsing for {ticker_symbol}.")

            # --- Load Phase (Transaction specific to this ticker) ---
            logger.info(f"Loading data into DB for {ticker_symbol}...")
            ticker_load_successful = True # Assume success for this ticker initially
            try:
                db_conn.begin() # Start transaction for this ticker's data
                table_pk_map = { 'yf_profile_metrics': ['ticker'], 'yf_stock_actions': ['ticker', 'action_date', 'action_type'], 'yf_major_holders': ['ticker'], 'yf_institutional_holders': ['ticker', 'holder', 'report_date'], 'yf_mutual_fund_holders': ['ticker', 'holder', 'report_date'], 'yf_recommendations': ['ticker', 'recommendation_timestamp', 'firm'], 'yf_calendar_events': ['ticker', 'event_type'], 'yf_financial_facts': ['ticker', 'report_date', 'frequency', 'item_label'] }

                for table_name, df in parsed_dfs.items():
                    if table_name in table_pk_map: # Only load defined tables
                        primary_keys = table_pk_map[table_name]
                        if df is not None and 'ticker' in df.columns: df['ticker'] = df['ticker'].str.upper() # Ensure ticker case consistency
                        if not load_dataframe_to_db(db_conn, df, table_name, primary_keys, logger, mode):
                            ticker_load_successful = False; logger.error(f"Load failed for table {table_name}, ticker {ticker_symbol}."); break # Stop loading this ticker on first failure

                # Commit or Rollback based on flag
                if ticker_load_successful:
                    db_conn.commit()
                    logger.info(f"Successfully committed data for ticker {ticker_symbol}.")
                    overall_success_count += 1
                else:
                    # Rollback happens here if load_dataframe_to_db failed
                    db_conn.rollback()
                    logger.warning(f"Rolled back transaction for ticker {ticker_symbol} due to load errors.")
                    overall_fail_count += 1

            except Exception as load_tx_e: # Catch errors during the transaction itself
                 logger.error(f"Error during DB transaction for {ticker_symbol}: {load_tx_e}", exc_info=True)
                 overall_fail_count += 1
                 try: db_conn.rollback() # Attempt rollback
                 except Exception as rb_e: logger.error(f"Rollback failed for {ticker_symbol} after transaction error: {rb_e}")


        except Exception as e: # Catch errors during fetch/parse for this ticker
            logger.error(f"CRITICAL error during processing ticker {ticker_symbol}: {e}", exc_info=True); overall_fail_count += 1
            # --- SAFER ROLLBACK: No explicit rollback needed here, as transaction hasn't started ---
            # try:
            #     # Wrap rollback in case connection is dead or no transaction active
            #     db_conn.rollback()
            #     logger.warning(f"Rolled back transaction for {ticker_symbol} due to unexpected error in fetch/parse.")
            # except duckdb.Error as rb_err:
            #     if "no transaction is active" in str(rb_err):
            #         logger.debug(f"No active transaction to rollback for {ticker_symbol} after fetch/parse error.")
            #     else:
            #         logger.error(f"Rollback failed for {ticker_symbol} after critical fetch/parse error: {rb_err}")
            # except Exception as rb_e:
            #      logger.error(f"General Rollback failed for {ticker_symbol} after critical fetch/parse error: {rb_e}")


        logger.debug(f"Sleeping for {YFINANCE_DELAY} seconds before next ticker..."); time.sleep(YFINANCE_DELAY) # Delay between tickers

    logger.info(f"Finished processing all tickers. Success: {overall_success_count}, Failed: {overall_fail_count}")
# --- Main Execution ---
if __name__ == "__main__":
    try: config = AppConfig(calling_script_path=Path(__file__)); logger.info(f"Config loaded. DB file: {config.DB_FILE_STR}")
    except SystemExit as e: logger.critical(f"Configuration failed: {e}"); sys.exit(1)
    except Exception as e: logger.critical(f"Unexpected error loading config: {e}", exc_info=True); sys.exit(1)

    run_mode = config.get_optional_var("STOCK_INFO_RUN_MODE", 'append').lower()
    if run_mode not in ['append', 'full_refresh']: logger.warning(f"Invalid STOCK_INFO_RUN_MODE '{run_mode}'. Defaulting to 'append'."); run_mode = 'append'
    logger.info(f"Running in mode: {run_mode}")

    target_tickers_str = config.get_optional_var("TARGET_TICKERS", None) # Allow targeting specific tickers via env var
    tickers_to_process = None
    if target_tickers_str:
        tickers_to_process = [t.strip().upper() for t in target_tickers_str.split(',')]
        logger.info(f"Processing specific target tickers: {tickers_to_process}")

    logger.info(f"--- Starting Stock Info Gatherer (v3 - Fetch, Parse, Load - Final) ---")

    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as conn:
            if conn:
                setup_info_tables(conn, logger) # Ensure tables exist
                if run_mode == 'full_refresh':
                     logger.warning("Running in 'full_refresh' mode. Clearing existing data from yf_* tables...")
                     try: conn.begin(); clear_info_tables(conn, logger); conn.commit()
                     except Exception as clear_e: logger.error(f"Failed to clear tables for full_refresh: {clear_e}", exc_info=True); conn.rollback(); logger.critical("Exiting due to table clear failure in full_refresh mode."); sys.exit(1)

                # Get tickers from DB if not specified
                if tickers_to_process is None:
                     try:
                          ticker_df = conn.execute("SELECT DISTINCT ticker FROM tickers ORDER BY ticker").fetchdf() # Get tickers from EDGAR data
                          tickers_to_process = ticker_df['ticker'].tolist()
                          logger.info(f"Processing {len(tickers_to_process)} tickers found in DB 'tickers' table.")
                     except Exception as e:
                          logger.error(f"Error fetching ticker list from DB: {e}. Cannot proceed without tickers.", exc_info=True)
                          tickers_to_process = [] # Set to empty to prevent processing

                if tickers_to_process:
                    gather_and_store_ticker_info(tickers_to_process, conn, logger, mode=run_mode)
                else:
                    logger.warning("No tickers specified or found in DB. Nothing to process.")
            else: logger.critical("Database connection failed. Cannot proceed."); sys.exit(1)
    except Exception as e: logger.critical(f"An error occurred during the main execution: {e}", exc_info=True); sys.exit(1)

    logger.info("--- Stock Info Gatherer (v3 - Fetch, Parse, Load) Finished ---")