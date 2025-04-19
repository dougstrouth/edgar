# -*- coding: utf-8 -*-
"""
Stock Info Gatherer Script (v3 - Iterative Skeleton)

Fetches supplementary company info using yfinance for a sample of tickers,
focusing on one data domain at a time for verification. This version fetches
Profile/Metrics, Actions, Financial Statements, Holders, Recommendations, AND Calendar Events.

Uses:
- config_utils.AppConfig for potential future config needs.
- logging_utils.setup_logging for standardized logging.
- yfinance for data fetching.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timezone, timedelta

import pandas as pd
import yfinance as yf

# --- Import Utilities ---
try:
    from config_utils import AppConfig
    from logging_utils import setup_logging
except ImportError as e:
    print(f"FATAL: Could not import utility modules (config_utils, logging_utils): {e}", file=sys.stderr)
    print("Ensure these files exist and are in the correct location.", file=sys.stderr)
    sys.exit(1)


# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
YFINANCE_DELAY = 0.7 # Seconds delay between ticker requests or major data type fetches

# --- Fetcher Functions (Candidates for yfinance_fetchers.py) ---

def fetch_profile_metrics(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """Fetches the .info dictionary containing profile and key metrics."""
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch .info for {ticker_symbol}")
    try:
        info_data = ticker_obj.info
        if not info_data:
            logger.warning(f"No .info dictionary returned for {ticker_symbol}.")
            return None
        if isinstance(info_data, dict) and ('symbol' in info_data or 'longName' in info_data or len(info_data)>1):
             logger.info(f"Successfully fetched .info for {ticker_symbol} (Keys: {len(info_data)})")
             return info_data
        else:
             logger.warning(f"Empty or unexpected format for .info for {ticker_symbol}: {info_data}")
             return None
    except Exception as e:
        logger.error(f"Failed fetching .info for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
        return None

def fetch_actions(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Fetches stock actions (dividends, splits)."""
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch actions for {ticker_symbol}")
    try:
        actions = ticker_obj.actions
        if actions is None or actions.empty:
            logger.info(f"No actions data found for {ticker_symbol}.")
            return None
        logger.info(f"Successfully fetched actions data for {ticker_symbol} (Entries: {len(actions)})")
        return actions
    except Exception as e:
        logger.error(f"Failed fetch actions for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
        return None

def fetch_financials(ticker_obj: yf.Ticker, logger: logging.Logger) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Fetches financials, balance sheet, cash flow (annual & quarterly).
    Returns a dictionary where keys are statement names (e.g., 'financials_annual')
    and values are the fetched DataFrames (or None if fetch failed/empty).
    """
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch financial statements for {ticker_symbol}")
    fetched_data = {}
    statement_map = {
        'financials_annual': (ticker_obj.financials, 'annual'),
        'financials_quarterly': (ticker_obj.quarterly_financials, 'quarterly'),
        'balance_sheet_annual': (ticker_obj.balance_sheet, 'annual'),
        'balance_sheet_quarterly': (ticker_obj.quarterly_balance_sheet, 'quarterly'),
        'cashflow_annual': (ticker_obj.cashflow, 'annual'),
        'cashflow_quarterly': (ticker_obj.quarterly_cashflow, 'quarterly'),
    }
    for name, (method, freq) in statement_map.items():
        data = None
        try:
            data = method
            if data is not None and not data.empty:
                logger.info(f"Successfully fetched {name} for {ticker_symbol} (Shape: {data.shape})")
                fetched_data[name] = data
            else:
                logger.info(f"No data found for {name} for {ticker_symbol}.")
                fetched_data[name] = None
        except Exception as e:
             logger.error(f"Failed fetch {name} for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
             fetched_data[name] = None
        time.sleep(0.1)
    return fetched_data

def fetch_holders(ticker_obj: yf.Ticker, logger: logging.Logger) -> Dict[str, Optional[Any]]:
    """
    Fetches major, institutional, and mutual fund holders.
    Returns a dictionary with keys 'major', 'institutional', 'mutualfund'
    and values being the raw fetched data (DataFrame or None).
    """
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch holders for {ticker_symbol}")
    fetched_data = {'major': None, 'institutional': None, 'mutualfund': None}
    holder_map = {
        'major': ticker_obj.major_holders,
        'institutional': ticker_obj.institutional_holders,
        'mutualfund': ticker_obj.mutualfund_holders,
    }
    for name, data_property in holder_map.items():
         data = None
         try:
            data = data_property
            if data is not None and not data.empty:
                logger.info(f"Successfully fetched {name}_holders for {ticker_symbol} (Shape/Len: {getattr(data,'shape',len(data) if data is not None else 'N/A')})")
                fetched_data[name] = data
            else:
                 logger.info(f"No data found for {name}_holders for {ticker_symbol}.")
                 fetched_data[name] = None
         except Exception as e:
             logger.error(f"Failed fetch {name}_holders for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
             fetched_data[name] = None
         time.sleep(0.1)
    return fetched_data

def fetch_recommendations(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Fetches analyst recommendations."""
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch recommendations for {ticker_symbol}")
    try:
        recs = ticker_obj.recommendations
        if recs is None or recs.empty:
            logger.info(f"No recommendations data found for {ticker_symbol}.")
            return None
        logger.info(f"Successfully fetched recommendations for {ticker_symbol} (Entries: {len(recs)})")
        return recs
    except Exception as e:
        logger.error(f"Failed fetch recommendations for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
        return None

def fetch_calendar(ticker_obj: yf.Ticker, logger: logging.Logger) -> Optional[Any]:
    """
    Fetches calendar events (earnings, dividends).
    Handles return types being DataFrame or dict.
    """
    ticker_symbol = ticker_obj.ticker
    logger.debug(f"Attempting to fetch calendar for {ticker_symbol}")
    try:
        cal_data = ticker_obj.calendar

        # --- CORRECTED CHECK for None, Empty DataFrame, or Empty Dict ---
        is_empty = False
        if cal_data is None:
            is_empty = True
        elif isinstance(cal_data, pd.DataFrame):
            is_empty = cal_data.empty
        elif isinstance(cal_data, dict):
            is_empty = not cal_data # An empty dict evaluates to False
        else:
            logger.warning(f"Unexpected data type for calendar {ticker_symbol}: {type(cal_data)}")
            is_empty = True # Treat unexpected types as empty/invalid

        if is_empty:
            logger.info(f"No calendar data found for {ticker_symbol}.")
            return None
        # --- End CORRECTION ---

        logger.info(f"Successfully fetched calendar data for {ticker_symbol}")
        # Return raw data structure (dict or DataFrame) for now
        return cal_data
    except Exception as e:
        logger.error(f"Failed fetch calendar for {ticker_symbol}: {type(e).__name__} - {e}", exc_info=False)
        return None
# --- Parsing Functions (Convert Raw Fetched Data to Structured DataFrames) ---

def parse_profile_metrics(info_dict: Optional[Dict], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses the .info dictionary into a DataFrame for the yf_profile_metrics table."""
    if not info_dict:
        return None
    logger.debug(f"Parsing .info data for {ticker}")

    # Define columns based on the CREATE TABLE statement (subset of info_dict keys)
    profile_cols = [
        'cik', 'isin', 'quoteType', 'exchange', 'longName', 'sector', 'industry',
        'longBusinessSummary', 'fullTimeEmployees', 'website', 'exchangeTimezoneName',
        'exchangeTimezoneShortName', 'auditRisk', 'boardRisk', 'compensationRisk',
        'shareHolderRightsRisk', 'overallRisk', 'governanceEpochDate', 'compensationAsOfEpochDate',
        'marketCap', 'enterpriseValue', 'beta', 'trailingPE', 'forwardPE', 'priceToBook',
        'priceToSalesTrailing12Months', 'enterpriseToRevenue', 'enterpriseToEbitda',
        'fiftyTwoWeekHigh', 'fiftyTwoWeekLow', 'sharesOutstanding', 'floatShares', 'bookValue',
        'dividendRate', 'dividendYield', 'exDividendDate', 'lastFiscalYearEnd',
        'nextFiscalYearEnd', 'mostRecentQuarter', 'earningsTimestamp', 'earningsTimestampStart',
        'earningsTimestampEnd', 'lastDividendValue', 'lastDividendDate'
    ]
    parsed_data = {'ticker': ticker, 'fetch_timestamp': fetch_ts}

    for col in profile_cols:
        parsed_data[col] = info_dict.get(col) # get() returns None if key is missing

    # --- Date Conversions (before DataFrame creation is fine) ---
    for date_key in ['exDividendDate', 'lastFiscalYearEnd', 'nextFiscalYearEnd', 'mostRecentQuarter']:
         if date_key in parsed_data and parsed_data[date_key] is not None:
             try:
                 if isinstance(parsed_data[date_key], (int, float)):
                     parsed_data[date_key] = datetime.fromtimestamp(parsed_data[date_key], timezone.utc).date()
                 else:
                      parsed_data[date_key] = pd.to_datetime(parsed_data[date_key], errors='coerce').date()
             except (TypeError, ValueError, OSError) as e:
                 logger.warning(f"Could not parse date '{parsed_data[date_key]}' for {date_key} in {ticker}: {e}")
                 parsed_data[date_key] = None

    # --- Create DataFrame FIRST ---
    try:
        df = pd.DataFrame([parsed_data])
    except Exception as df_e:
         logger.error(f"Failed to create DataFrame from parsed_data for {ticker}: {df_e}", exc_info=True)
         return None

    # --- CORRECTED: Convert integer types *on the DataFrame* ---
    int_keys = ['fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk',
                'shareHolderRightsRisk', 'overallRisk', 'marketCap', 'enterpriseValue',
                'sharesOutstanding', 'floatShares', # Keep epoch dates as potentially large numbers if needed
                'governanceEpochDate', 'compensationAsOfEpochDate', 'earningsTimestamp',
                'earningsTimestampStart', 'earningsTimestampEnd', 'lastDividendDate']

    for int_key in int_keys:
         if int_key in df.columns: # Check if the column exists in the DataFrame
             try:
                  # Apply astype to the DataFrame column, using pd.NA for unparseable
                  df[int_key] = pd.to_numeric(df[int_key], errors='coerce').astype('Int64')
             except (ValueError, TypeError) as e: # Removed AttributeError catch as it shouldn't happen on Series
                  logger.warning(f"Could not convert column {int_key} to Int64 for {ticker}: {e}. Values may be pd.NA.")
                  # pd.to_numeric with errors='coerce' already turns failures into NaN/NA
                  # so the .astype('Int64') should handle those correctly.
                  # We log the warning but don't need to manually set NA here.
                  pass # Continue attempt with other columns

    # Ensure columns match the table definition order if necessary later
    # For now, just return the processed DataFrame
    return df
def parse_actions(actions_df: Optional[pd.DataFrame], ticker: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses the raw actions DataFrame into the structure for the yf_stock_actions table."""
    if actions_df is None or actions_df.empty:
        return None
    logger.debug(f"Parsing actions data for {ticker}")
    try:
        actions = actions_df.copy()
        actions.index.name = 'action_date'
        actions.reset_index(inplace=True)
        actions['ticker'] = ticker
        # Convert timezone-aware to naive UTC date
        actions['action_date'] = pd.to_datetime(actions['action_date'], errors='coerce', utc=True).dt.date
        actions_melted = actions.melt(
            id_vars=['ticker', 'action_date'],
            value_vars=['Dividends', 'Stock Splits'],
            var_name='action_type',
            value_name='value'
        )
        # Filter out rows where the action value is 0 or NaN (meaning no action of that type occurred on that date)
        actions_melted = actions_melted[actions_melted['value'] != 0.0]
        actions_melted.dropna(subset=['value', 'action_date', 'action_type'], inplace=True) # Drop if key info missing

        if actions_melted.empty:
            logger.info(f"No non-zero actions found after parsing for {ticker}")
            return None

        # Rename 'Dividends' -> 'Dividend', 'Stock Splits' -> 'Split'
        actions_melted['action_type'] = actions_melted['action_type'].replace({'Dividends': 'Dividend', 'Stock Splits': 'Split'})

        # Ensure correct types
        actions_melted['value'] = pd.to_numeric(actions_melted['value'], errors='coerce')
        actions_melted.dropna(subset=['value'], inplace=True) # Ensure value is numeric

        # Keep only necessary columns
        final_cols = ['ticker', 'action_date', 'action_type', 'value']
        return actions_melted[final_cols]
    except Exception as e:
        logger.error(f"Failed to parse actions for {ticker}: {e}", exc_info=True)
        return None
# --- CORRECTED parse_major_holders (using index as label) ---
def parse_major_holders(major_holders_data: Optional[Any], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses the raw major holders data into a DataFrame, assuming index is the label."""
    if major_holders_data is None: return None
    logger.debug(f"Parsing major holders data for {ticker} (type: {type(major_holders_data)})")

    # --- Add DEBUG PRINT again to be sure ---
    print(f"\nDEBUG PRINT for {ticker} - Major Holders Data:")
    print(f"Type: {type(major_holders_data)}")
    print(f"Index: {getattr(major_holders_data, 'index', 'N/A')}")
    print(f"Columns: {getattr(major_holders_data, 'columns', 'N/A')}")
    print(f"Data:\n{major_holders_data}\n")
    # --- END DEBUG PRINT ---

    pct_insiders = None
    pct_institutions = None

    # Expecting a DataFrame where the *index* contains the labels (Breakdown)
    # and the *first column* contains the values
    if isinstance(major_holders_data, pd.DataFrame) and not major_holders_data.empty:
        # Check if it has at least ONE column for values
        if major_holders_data.shape[1] >= 1:
            val_col_idx = 0 # Assume value is the first column
            val_col_name = major_holders_data.columns[val_col_idx]
            logger.debug(f"Using index for labels and column '{val_col_name}' (idx {val_col_idx}) for values.")

            # Iterate through the index (labels) and corresponding values
            for label_str_raw, value_data in zip(major_holders_data.index, major_holders_data.iloc[:, val_col_idx]):
                label_str = str(label_str_raw).lower()
                # Value seems to be already a float/number based on previous debug output
                try:
                    value_float = float(value_data)
                    if not pd.isna(value_float): # Check for NaN explicitly
                        # Check label for keywords
                        if 'insider' in label_str:
                            pct_insiders = value_float
                            logger.debug(f"Parsed pct_insiders: {pct_insiders} from label '{label_str}'")
                        # Use specific label check now that we see the index name
                        elif 'institutionspercentheld' == label_str.replace(' ',''):
                            pct_institutions = value_float
                            logger.debug(f"Parsed pct_institutions: {pct_institutions} from label '{label_str}'")
                    # Ignore other index rows like institutionsFloatPercentHeld, institutionsCount
                except (ValueError, TypeError) as e:
                     logger.warning(f"Could not parse value '{value_data}' for label '{label_str}' in {ticker}: {e}")
                     continue # Skip this row if parsing fails
        else:
            logger.warning(f"Major holders DataFrame for {ticker} has zero columns. Cannot parse.")

    elif isinstance(major_holders_data, list):
         logger.warning(f"Major holders for {ticker} was a list, parsing not fully implemented yet.")
         pass
    else:
          logger.warning(f"Unexpected data type for major holders {ticker}: {type(major_holders_data)}")

    # Create DataFrame only if some data was found
    if pct_insiders is not None or pct_institutions is not None:
        data = { 'ticker': ticker, 'fetch_timestamp': fetch_ts, 'pct_insiders': pct_insiders, 'pct_institutions': pct_institutions }
        df = pd.DataFrame([data]); df['pct_insiders'] = df['pct_insiders'].astype('float64'); df['pct_institutions'] = df['pct_institutions'].astype('float64'); return df
    else:
        if isinstance(major_holders_data, pd.DataFrame) and not major_holders_data.empty:
             logger.warning(f"Parsing major_holders for {ticker} yielded no insider or institution percentages.")
        return None
# --- End CORRECTED parse_major_holders ---


def _parse_detailed_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Helper function to parse Institutional or Mutual Fund holders DataFrame."""
    if holders_df is None or holders_df.empty:
        return None
    logger.debug(f"Parsing detailed holders data for {ticker}")
    try:
        holders = holders_df.copy()
        # Expected columns from yfinance and target DB names
        # Use lowercase for matching resilience
        col_map = {
            'holder': 'holder',
            'shares': 'shares',
            '% out': 'pct_out', # Note the space in yfinance sometimes
            'date reported': 'report_date',
            'value': 'value'
        }
        rename_dict = {}
        found_cols = []
        for yf_col_name in holders.columns:
            yf_col_lower = str(yf_col_name).lower()
            for map_key, db_col in col_map.items():
                 # Check for variations like '% Out' vs 'pctheld'
                 if yf_col_lower == map_key or (map_key == '% out' and yf_col_lower == 'pctheld'):
                      rename_dict[yf_col_name] = db_col
                      found_cols.append(db_col)
                      break

        if not all(c in found_cols for c in ['holder', 'report_date', 'shares']):
             logger.warning(f"Missing essential columns ('holder', 'report_date', 'shares') in detailed holders for {ticker}. Found: {list(holders.columns)}")
             return None

        holders.rename(columns=rename_dict, inplace=True)

        holders['ticker'] = ticker
        holders['fetch_timestamp'] = fetch_ts

        # Clean data types, preserving NaNs
        holders['report_date'] = pd.to_datetime(holders['report_date'], errors='coerce').dt.date
        holders['shares'] = pd.to_numeric(holders['shares'], errors='coerce').astype('Int64')
        if 'pct_out' in holders.columns:
             holders['pct_out'] = pd.to_numeric(holders['pct_out'], errors='coerce')
        if 'value' in holders.columns:
             holders['value'] = pd.to_numeric(holders['value'], errors='coerce').astype('Int64')

        # Ensure required PK columns are present
        required_cols = ['ticker', 'holder', 'report_date']
        holders.dropna(subset=[col for col in required_cols if col in holders.columns], inplace=True)

        # Select and order columns for the database
        db_cols = ['ticker', 'holder', 'report_date', 'fetch_timestamp', 'shares', 'pct_out', 'value']
        final_df = holders[[col for col in db_cols if col in holders.columns]].copy()

        return final_df if not final_df.empty else None

    except Exception as e:
        logger.error(f"Failed to parse detailed holders for {ticker}: {e}", exc_info=True)
        return None

def parse_institutional_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses Institutional Holders."""
    return _parse_detailed_holders(holders_df, ticker, fetch_ts, logger)

def parse_mutual_fund_holders(holders_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses Mutual Fund Holders."""
    return _parse_detailed_holders(holders_df, ticker, fetch_ts, logger)

def parse_recommendations(recs_df: Optional[pd.DataFrame], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses the raw recommendations DataFrame."""
    if recs_df is None or recs_df.empty:
        return None
    logger.debug(f"Parsing recommendations data for {ticker}")
    try:
        recs = recs_df.copy()
        recs.index.name = 'recommendation_timestamp' # Use the index directly
        recs.reset_index(inplace=True)
        recs['ticker'] = ticker
        recs['fetch_timestamp'] = fetch_ts

        # Rename columns robustly (handle case variations)
        col_map = {
            'firm': 'firm',
            'to grade': 'to_grade',
            'from grade': 'from_grade',
            'action': 'action'
        }
        rename_dict = {}
        current_cols_lower = {str(c).lower(): c for c in recs.columns}

        for map_key_lower, db_col in col_map.items():
             if map_key_lower in current_cols_lower:
                 rename_dict[current_cols_lower[map_key_lower]] = db_col

        recs.rename(columns=rename_dict, inplace=True)

        # Convert timestamp to timezone-aware UTC
        if 'recommendation_timestamp' in recs.columns:
             recs['recommendation_timestamp'] = pd.to_datetime(recs['recommendation_timestamp'], errors='coerce', utc=True)
             # Ensure it has timezone info
             if recs['recommendation_timestamp'].dt.tz is None:
                  logger.warning(f"Recommendation timestamp for {ticker} was timezone naive, assuming UTC.")
                  recs['recommendation_timestamp'] = recs['recommendation_timestamp'].dt.tz_localize('UTC')
             else:
                  recs['recommendation_timestamp'] = recs['recommendation_timestamp'].dt.tz_convert('UTC')

        # Ensure required PK columns are not null
        pk_cols = ['ticker', 'recommendation_timestamp', 'firm']
        recs.dropna(subset=[col for col in pk_cols if col in recs.columns], inplace=True)

        if recs.empty: return None

        # Select and order columns
        db_cols = ['ticker', 'recommendation_timestamp', 'firm', 'fetch_timestamp', 'to_grade', 'from_grade', 'action']
        final_df = recs[[col for col in db_cols if col in recs.columns]].copy()

        return final_df
    except Exception as e:
        logger.error(f"Failed to parse recommendations for {ticker}: {e}", exc_info=True)
        return None

def parse_calendar(cal_data: Optional[Any], ticker: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses calendar data (dict or DataFrame) into a structured DataFrame."""
    if cal_data is None or (isinstance(cal_data, dict) and not cal_data) or (isinstance(cal_data, pd.DataFrame) and cal_data.empty):
        return None
    logger.debug(f"Parsing calendar data for {ticker} (type: {type(cal_data)})")
    parsed_events = []

    try:
        # Handle if yfinance returns a DataFrame directly (newer versions might)
        if isinstance(cal_data, pd.DataFrame):
            # Assuming columns might be 'Earnings Date', 'Earnings High', 'Earnings Low', 'Earnings Average',
            # 'Revenue High', 'Revenue Low', 'Revenue Average', 'Dividend Date', 'Ex-Dividend Date'
            cal_data.columns = [str(c).replace(' ','') for c in cal_data.columns] # Normalize column names

            # Process Earnings
            if 'EarningsDate' in cal_data.columns:
                # Might be a list/tuple of timestamps in the first row
                if isinstance(cal_data['EarningsDate'].iloc[0], (list, tuple)) and len(cal_data['EarningsDate'].iloc[0]) > 0:
                     start_ts = cal_data['EarningsDate'].iloc[0][0]
                     end_ts = cal_data['EarningsDate'].iloc[0][1] if len(cal_data['EarningsDate'].iloc[0]) > 1 else start_ts
                     start_date = pd.to_datetime(start_ts, errors='coerce', unit='s').date() if pd.notna(start_ts) else None
                     end_date = pd.to_datetime(end_ts, errors='coerce', unit='s').date() if pd.notna(end_ts) else start_date
                     if start_date:
                          parsed_events.append({
                              'event_type': 'Earnings', 'event_start_date': start_date, 'event_end_date': end_date,
                              'earnings_avg': cal_data['EarningsAverage'].iloc[0] if 'EarningsAverage' in cal_data.columns else None,
                              'earnings_low': cal_data['EarningsLow'].iloc[0] if 'EarningsLow' in cal_data.columns else None,
                              'earnings_high': cal_data['EarningsHigh'].iloc[0] if 'EarningsHigh' in cal_data.columns else None,
                              'revenue_avg': cal_data['RevenueAverage'].iloc[0] if 'RevenueAverage' in cal_data.columns else None,
                              'revenue_low': cal_data['RevenueLow'].iloc[0] if 'RevenueLow' in cal_data.columns else None,
                              'revenue_high': cal_data['RevenueHigh'].iloc[0] if 'RevenueHigh' in cal_data.columns else None,
                          })

            # Process Ex-Dividend Date
            if 'ExDividendDate' in cal_data.columns and pd.notna(cal_data['ExDividendDate'].iloc[0]):
                ex_div_date = pd.to_datetime(cal_data['ExDividendDate'].iloc[0], errors='coerce', unit='s').date()
                if ex_div_date:
                    parsed_events.append({'event_type': 'ExDividendDate', 'event_start_date': ex_div_date, 'event_end_date': ex_div_date})

        # Handle if yfinance returns a dictionary (older versions or different structure)
        elif isinstance(cal_data, dict):
            # Process Earnings dictionary
            earnings_info = cal_data.get('Earnings')
            if isinstance(earnings_info, dict) and 'Earnings Date' in earnings_info:
                earnings_dates = earnings_info['Earnings Date']
                if isinstance(earnings_dates, (list, tuple)) and len(earnings_dates) > 0:
                    start_ts = earnings_dates[0]; end_ts = earnings_dates[1] if len(earnings_dates) > 1 else start_ts
                    start_date = pd.to_datetime(start_ts, errors='coerce', unit='s').date() if pd.notna(start_ts) else None
                    end_date = pd.to_datetime(end_ts, errors='coerce', unit='s').date() if pd.notna(end_ts) else start_date
                    if start_date:
                         parsed_events.append({
                             'event_type': 'Earnings', 'event_start_date': start_date, 'event_end_date': end_date,
                             'earnings_avg': earnings_info.get('Earnings Average'),
                             'earnings_low': earnings_info.get('Earnings Low'),
                             'earnings_high': earnings_info.get('Earnings High'),
                             'revenue_avg': earnings_info.get('Revenue Average'),
                             'revenue_low': earnings_info.get('Revenue Low'),
                             'revenue_high': earnings_info.get('Revenue High'),
                         })

            # Process Ex-Dividend Date from dict
            ex_div_ts = cal_data.get('Ex-Dividend Date')
            if pd.notna(ex_div_ts):
                ex_div_date = pd.to_datetime(ex_div_ts, errors='coerce', unit='s').date()
                if ex_div_date:
                    parsed_events.append({'event_type': 'ExDividendDate', 'event_start_date': ex_div_date, 'event_end_date': ex_div_date})

        if not parsed_events:
             logger.info(f"No parsable calendar events found for {ticker}")
             return None

        df = pd.DataFrame(parsed_events)
        df['ticker'] = ticker
        df['fetch_timestamp'] = fetch_ts

        # Convert numeric types, preserving NaN
        num_cols = ['earnings_avg', 'earnings_low', 'earnings_high', 'revenue_avg', 'revenue_low', 'revenue_high']
        for col in num_cols:
             if col in df.columns:
                  df[col] = pd.to_numeric(df[col], errors='coerce')
                  # Check if column needs to be BIGINT
                  if 'revenue' in col: df[col] = df[col].astype('Int64')

        # Select and order columns
        db_cols = ['ticker', 'event_type', 'fetch_timestamp', 'event_start_date', 'event_end_date',
                   'earnings_avg', 'earnings_low', 'earnings_high', 'revenue_avg', 'revenue_low', 'revenue_high']
        final_df = df[[col for col in db_cols if col in df.columns]].copy()

        return final_df if not final_df.empty else None

    except Exception as e:
        logger.error(f"Failed to parse calendar data for {ticker}: {e}", exc_info=True)
        return None

def parse_financial_statement(stmt_df: Optional[pd.DataFrame], ticker: str, stmt_type: str, freq: str, fetch_ts: datetime, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Parses a raw financial statement DataFrame into a long-format DataFrame."""
    if stmt_df is None or stmt_df.empty:
        return None
    logger.debug(f"Parsing {freq} {stmt_type} for {ticker}")
    try:
        # Ensure index has a name
        if stmt_df.index.name is None: stmt_df.index.name = 'item_label'
        else: stmt_df.index.name = 'item_label' # Standardize

        # Reset index to turn item labels into a column
        long_df = stmt_df.reset_index()

        # Melt the DataFrame
        long_df = long_df.melt(
            id_vars=['item_label'],
            var_name='report_date', # Columns are the dates
            value_name='value'
        )

        # Clean and Convert Data Types
        # Convert report_date column headers (which might be Timestamps) to date objects
        long_df['report_date'] = pd.to_datetime(long_df['report_date'], errors='coerce').dt.date
        long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce') # Value should be numeric (DOUBLE)

        # Add Metadata Columns
        long_df['ticker'] = ticker
        long_df['frequency'] = freq
        long_df['statement_type'] = stmt_type
        long_df['fetch_timestamp'] = fetch_ts

        # Drop rows only if essential PK components are missing
        # Keep rows where 'value' is NaN as per requirement
        pk_cols = ['ticker', 'report_date', 'frequency', 'item_label']
        long_df.dropna(subset=[col for col in pk_cols if col in long_df.columns], inplace=True)

        if long_df.empty:
            logger.warning(f"No valid rows after melting/cleaning {freq} {stmt_type} for {ticker}")
            return None

        # Select and order columns for the database
        db_cols = ['ticker', 'report_date', 'frequency', 'statement_type', 'item_label', 'fetch_timestamp', 'value']
        final_df = long_df[[col for col in db_cols if col in long_df.columns]].copy()

        return final_df

    except Exception as e:
        logger.error(f"Failed to parse {freq} {stmt_type} for {ticker}: {e}", exc_info=True)
        return None


# --- Orchestration (Updated to include parsing) ---

def gather_ticker_info(tickers: List[str], logger: logging.Logger):
    """
    Iterates through tickers, fetches all domains, and parses the data.
    Prints summaries of parsed data for verification.
    """
    logger.info(f"Starting Full Info gathering & parsing for {len(tickers)} tickers.")
    # Store parsed results per ticker
    all_parsed_data: Dict[str, Dict[str, Optional[pd.DataFrame]]] = {}

    for ticker_symbol in tickers:
        logger.info(f"--- Processing ticker: {ticker_symbol} ---")
        raw_results = {} # Store raw fetched data for parsing
        parsed_dfs: Dict[str, Optional[pd.DataFrame]] = {} # Store parsed DataFrames
        fetch_ts = datetime.now(timezone.utc) # Consistent timestamp for this ticker's fetch cycle

        try:
            ticker_obj = yf.Ticker(ticker_symbol)

            # --- Fetch Phase ---
            raw_results['profile'] = fetch_profile_metrics(ticker_obj, logger); time.sleep(0.1)
            raw_results['actions'] = fetch_actions(ticker_obj, logger); time.sleep(YFINANCE_DELAY)
            raw_results['financials'] = fetch_financials(ticker_obj, logger); time.sleep(YFINANCE_DELAY)
            raw_results['holders'] = fetch_holders(ticker_obj, logger); time.sleep(YFINANCE_DELAY)
            raw_results['recommendations'] = fetch_recommendations(ticker_obj, logger); time.sleep(YFINANCE_DELAY)
            raw_results['calendar'] = fetch_calendar(ticker_obj, logger)

            # --- Parse Phase ---
            logger.info(f"--- Parsing data for {ticker_symbol} ---")

            # Parse Profile/Metrics
            parsed_dfs['profile'] = parse_profile_metrics(raw_results.get('profile'), ticker_symbol, fetch_ts, logger)
            if parsed_dfs['profile'] is not None: print(f"  Parsed Profile/Metrics: {parsed_dfs['profile'].shape}")
            else: print("  Parsing Profile/Metrics FAILED or No Data")

            # Parse Actions
            parsed_dfs['actions'] = parse_actions(raw_results.get('actions'), ticker_symbol, logger)
            if parsed_dfs['actions'] is not None: print(f"  Parsed Actions: {parsed_dfs['actions'].shape}")
            else: print("  Parsing Actions FAILED or No Data")

            # Parse Financials (combine all statements into one long DataFrame)
            all_fin_facts = []
            if raw_results.get('financials'):
                 fin_map = {'Income Statement': ['financials_annual', 'financials_quarterly'],
                            'Balance Sheet': ['balance_sheet_annual', 'balance_sheet_quarterly'],
                            'Cash Flow': ['cashflow_annual', 'cashflow_quarterly']}
                 for stmt_type, keys in fin_map.items():
                      for key in keys:
                           raw_df = raw_results['financials'].get(key)
                           freq = 'annual' if 'annual' in key else 'quarterly'
                           parsed_stmt_df = parse_financial_statement(raw_df, ticker_symbol, stmt_type, freq, fetch_ts, logger)
                           if parsed_stmt_df is not None:
                               all_fin_facts.append(parsed_stmt_df)
            if all_fin_facts:
                parsed_dfs['financials'] = pd.concat(all_fin_facts, ignore_index=True)
                print(f"  Parsed Financials (combined): {parsed_dfs['financials'].shape}")
            else:
                parsed_dfs['financials'] = None
                print("  Parsing Financials FAILED or No Data")

            # Parse Holders
            holders_data = raw_results.get('holders')
            if holders_data:
                 parsed_dfs['major_holders'] = parse_major_holders(holders_data.get('major'), ticker_symbol, fetch_ts, logger)
                 parsed_dfs['institutional_holders'] = parse_institutional_holders(holders_data.get('institutional'), ticker_symbol, fetch_ts, logger)
                 parsed_dfs['mutualfund_holders'] = parse_mutual_fund_holders(holders_data.get('mutualfund'), ticker_symbol, fetch_ts, logger)
                 print(f"  Parsed Major Holders: {parsed_dfs['major_holders'].shape if parsed_dfs['major_holders'] is not None else 'No Data'}")
                 print(f"  Parsed Institutional Holders: {parsed_dfs['institutional_holders'].shape if parsed_dfs['institutional_holders'] is not None else 'No Data'}")
                 print(f"  Parsed Mutual Fund Holders: {parsed_dfs['mutualfund_holders'].shape if parsed_dfs['mutualfund_holders'] is not None else 'No Data'}")
            else:
                 print("  Parsing Holders FAILED or No Data")
                 parsed_dfs['major_holders'] = parsed_dfs['institutional_holders'] = parsed_dfs['mutualfund_holders'] = None

            # Parse Recommendations
            parsed_dfs['recommendations'] = parse_recommendations(raw_results.get('recommendations'), ticker_symbol, fetch_ts, logger)
            if parsed_dfs['recommendations'] is not None: print(f"  Parsed Recommendations: {parsed_dfs['recommendations'].shape}")
            else: print("  Parsing Recommendations FAILED or No Data")

            # Parse Calendar
            parsed_dfs['calendar'] = parse_calendar(raw_results.get('calendar'), ticker_symbol, fetch_ts, logger)
            if parsed_dfs['calendar'] is not None: print(f"  Parsed Calendar: {parsed_dfs['calendar'].shape}")
            else: print("  Parsing Calendar FAILED or No Data")


            all_parsed_data[ticker_symbol] = parsed_dfs
            logger.info(f"Finished parsing for {ticker_symbol}.")

        except Exception as e:
            logger.error(f"Unexpected error during processing for {ticker_symbol}: {e}", exc_info=True)
            all_parsed_data[ticker_symbol] = {} # Mark as failed / no parsed data


        # Apply delay between different tickers
        logger.debug(f"Sleeping for {YFINANCE_DELAY} seconds before next ticker...")
        time.sleep(YFINANCE_DELAY)

    logger.info(f"Finished gathering and parsing for all sample tickers.")
    # return all_parsed_data # Return the dictionary of parsed DataFrames


# --- Main Execution ---
if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        logger.info(f"Config loaded. DB file (if needed later): {config.DB_FILE_STR}")
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error loading config: {e}", exc_info=True)
        sys.exit(1)

    # Define a small sample of tickers for this skeleton run
    sample_tickers = ['AAPL', 'MSFT', 'NVDA', 'NONEXISTENTTICKERXYZ', 'GSK']

    logger.info(f"--- Starting Stock Info Gatherer (v3 - Step 7: Fetch & Parse) ---")
    logger.info(f"Processing sample tickers: {sample_tickers}")

    # Call the function to gather and parse the data
    # parsed_results = gather_ticker_info(sample_tickers, logger) # Capture if needed
    gather_ticker_info(sample_tickers, logger)

    # TODO NEXT: Implement database setup and loading functions
    # using the parsed DataFrames stored in 'parsed_results' (or generated within the loop)

    logger.info("--- Stock Info Gatherer (v3 - Step 7) Finished ---")