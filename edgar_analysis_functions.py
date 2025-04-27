# -*- coding: utf-8 -*-
"""
EDGAR Analysis Functions (Refactored for Utilities)

Provides functions to analyze data stored in the EDGAR DuckDB database.
Uses logging_utils for standardized logging.
Connects read-only to the database.
"""

import logging  # Keep for level constants (e.g., logging.INFO)
from pathlib import Path
import duckdb
import pandas as pd
from typing import List, Optional, Dict, Any, Tuple, Union
from datetime import date, datetime, timezone

# --- Import Utilities ---
from logging_utils import setup_logging
from database_conn import get_db_connection

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO) # Or logging.DEBUG


# --- Database Connection (Using utility function for consistency) ---
def connect_db_readonly(db_path_str: str) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Connects read-only using the centralized get_db_connection utility.

    Args:
        db_path_str: Path to the database file as a string.

    Returns:
        A read-only DuckDB connection object or None if connection fails.
    """
    logger.info(f"Attempting read-only connection to: {db_path_str}")
    conn = get_db_connection(db_path_override=db_path_str, read_only=True)
    if conn:
        logger.info(f"Read-only connection successful: {db_path_str}")
    else:
        logger.error(f"Read-only connection failed: {db_path_str}")
    return conn


# --- Core Data Retrieval Functions ---

def get_company_cik(db_conn: duckdb.DuckDBPyConnection, identifier: str, identifier_type: str = 'ticker') -> Optional[str]:
    """
    Retrieves the CIK for a company based on its ticker symbol or primary name.

    Args:
        db_conn: Active DuckDB connection.
        identifier: The ticker symbol or company name.
        identifier_type: 'ticker' or 'name'.

    Returns:
        The CIK as a string (10 digits, padded), or None if not found.
    """
    identifier = identifier.strip()
    try:
        if identifier_type.lower() == 'ticker':
            query = """
                SELECT DISTINCT t.cik
                FROM tickers t
                WHERE t.ticker = ? COLLATE NOCASE -- Case-insensitive ticker match
                LIMIT 1;
            """
            result = db_conn.execute(query, [identifier.upper()]).fetchone()
        elif identifier_type.lower() == 'name':
            query = """
                SELECT c.cik
                FROM companies c
                WHERE c.primary_name ILIKE ? -- Case-insensitive search
                LIMIT 1;
            """
            result = db_conn.execute(query, [f'%{identifier}%']).fetchone()
        else:
            logger.error(f"Invalid identifier_type: {identifier_type}. Use 'ticker' or 'name'.")
            return None

        if result and result[0]:
            cik = str(result[0]).zfill(10) # Ensure padding
            logger.info(f"Found CIK {cik} for {identifier_type} '{identifier}'")
            return cik
        else:
            logger.warning(f"Could not find CIK for {identifier_type} '{identifier}'")
            return None
    except Exception as e:
        logger.error(f"Error querying CIK for {identifier}: {e}", exc_info=True)
        return None

def get_ticker_for_cik(db_conn: duckdb.DuckDBPyConnection, cik: str) -> Optional[str]:
    """
    Retrieves the most likely primary ticker for a given CIK from the tickers table.
    (Note: A CIK might have multiple tickers; this returns the first one found alphabetically).

    Args:
        db_conn: Active DuckDB connection.
        cik: The 10-digit CIK string.

    Returns:
        The ticker symbol as a string, or None if not found.
    """
    try:
        query = """
            SELECT ticker
            FROM tickers
            WHERE cik = ?
            ORDER BY ticker -- Get predictable result if multiple exist
            LIMIT 1;
        """
        result = db_conn.execute(query, [cik]).fetchone()
        if result and result[0]:
            ticker = result[0]
            logger.info(f"Found ticker {ticker} for CIK {cik}")
            return ticker
        else:
            logger.warning(f"Could not find ticker for CIK {cik}")
            return None
    except Exception as e:
        logger.error(f"Error querying ticker for CIK {cik}: {e}", exc_info=True)
        return None


def get_financial_data(
    db_conn: duckdb.DuckDBPyConnection,
    ciks: List[str],
    tags: List[str],
    forms: Optional[List[str]] = None, # e.g., ['10-K', '10-Q']
    fiscal_years: Optional[List[int]] = None,
    fiscal_periods: Optional[List[str]] = None, # e.g., ['FY', 'Q1', 'Q2']
    start_date: Optional[Union[str, date]] = None, # Filter by period_end_date
    end_date: Optional[Union[str, date]] = None,   # Filter by period_end_date
    most_recent_per_period: bool = True # Get only latest filing for each CIK/tag/period_end_date
) -> pd.DataFrame:
    """
    Retrieves specific financial data points (numeric) from the xbrl_facts table.
    Handles selecting the most recent fact per period end date if specified.

    Args:
        # ... (Args remain the same)

    Returns:
        # ... (Returns remain the same)
    """
    if not ciks or not tags:
        logger.error("CIKs list and Tags list cannot be empty.")
        return pd.DataFrame()

    # Build the base query components
    base_select = """
    f.cik, f.taxonomy, f.tag_name, f.unit, f.period_end_date, f.value_numeric,
    f.fy, f.fp, f.form, f.filed_date, f.accession_number
    """
    from_clause = "FROM xbrl_facts f"
    where_clauses = ["f.cik IN ({})".format(','.join('?' * len(ciks))),
                     "f.tag_name IN ({})".format(','.join('?' * len(tags))),
                     "f.value_numeric IS NOT NULL"]
    params = list(ciks) + list(tags) # Start with CIKs and Tags

    # Add optional filters
    if forms:
        where_clauses.append("f.form IN ({})".format(','.join('?' * len(forms))))
        params.extend(forms)
    if fiscal_years:
        where_clauses.append("f.fy IN ({})".format(','.join('?' * len(fiscal_years))))
        params.extend(fiscal_years)
    if fiscal_periods:
        where_clauses.append("f.fp IN ({})".format(','.join('?' * len(fiscal_periods))))
        params.extend(fiscal_periods)
    if start_date:
        where_clauses.append("f.period_end_date >= ?")
        params.append(start_date)
    if end_date:
        where_clauses.append("f.period_end_date <= ?")
        params.append(end_date)

    where_string = "\nWHERE " + "\n  AND ".join(where_clauses) if where_clauses else ""

    # Construct the final query based on most_recent_per_period
    if most_recent_per_period:
         # Use ROW_NUMBER within a CTE
        inner_query = f"""
        SELECT
            {base_select},
            ROW_NUMBER() OVER(PARTITION BY f.cik, f.tag_name, f.period_end_date ORDER BY f.filed_date DESC, f.accession_number DESC) as rn
        {from_clause}
        {where_string}
        """
        final_query = f"""
         WITH RankedFacts AS (
             {inner_query}
         )
         SELECT * EXCLUDE(rn)
         FROM RankedFacts
         WHERE rn = 1
         ORDER BY cik, tag_name, period_end_date DESC, filed_date DESC; -- Order the final result set
         """
         # Note: Ordering by columns from RankedFacts CTE implicitly
    else:
         # Simple query without ranking
        final_query = f"""
        SELECT
            {base_select}
        {from_clause}
        {where_string}
        ORDER BY f.cik, f.tag_name, f.period_end_date DESC, f.filed_date DESC; -- Order using alias f
        """

    try:
        logger.info(f"Querying financial data for {len(ciks)} CIKs, {len(tags)} Tags.")
        logger.debug(f"Executing query: {final_query} with params: {params}")
        df = db_conn.execute(final_query, params).fetchdf()
        logger.info(f"Retrieved {len(df)} financial fact records.")

        # Basic type conversion (same as before)
        if not df.empty:
            for date_col in ['period_end_date', 'filed_date']:
                 if date_col in df.columns:
                      df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
            df['value_numeric'] = pd.to_numeric(df['value_numeric'], errors='coerce')
            df['cik'] = df['cik'].astype(str)
        return df
    except Exception as e:
        logger.error(f"Error querying financial data: {e}", exc_info=True)
        return pd.DataFrame()
def get_stock_history(
    db_conn: duckdb.DuckDBPyConnection,
    tickers: List[str],
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None
) -> pd.DataFrame:
    """
    Retrieves historical stock price data from the stock_history table.

    Args:
        # ... (Args remain the same)

    Returns:
        # ... (Returns remain the same)
    """
    if not tickers:
        logger.error("Tickers list cannot be empty.")
        return pd.DataFrame()

    # Ensure tickers are uppercase
    tickers_upper = [t.upper() for t in tickers]

    # Base query with placeholders for tickers
    query = f"SELECT ticker, date, open, high, low, close, adj_close, volume FROM stock_history WHERE ticker IN ({','.join(['?'] * len(tickers_upper))})"
    params = list(tickers_upper) # Start params list with tickers

    # Append date filters and parameters IF they exist
    date_filters = []
    if start_date:
        date_filters.append("date >= ?")
        params.append(start_date) # Add start_date to params list
    if end_date:
        date_filters.append("date <= ?")
        params.append(end_date) # Add end_date to params list

    if date_filters:
        query += " AND " + " AND ".join(date_filters)

    query += " ORDER BY ticker, date;"

    try:
        # Log the *actual* number of tickers being queried
        logger.info(f"Querying stock history for {len(tickers_upper)} tickers.")
        logger.debug(f"Executing query: {query} with params: {params}") # Log query and params
        df = db_conn.execute(query, params).fetchdf()
        logger.info(f"Retrieved {len(df)} stock history records.")
        if not df.empty:
             df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        return df
    except Exception as e:
        logger.error(f"Error querying stock history: {e}", exc_info=True)
        return pd.DataFrame()

def get_latest_stock_price(db_conn: duckdb.DuckDBPyConnection, ticker: str) -> Optional[float]:
    """
    Retrieves the most recent closing price for a given ticker,
    preferring adjusted close but falling back to regular close if adjusted is unavailable.

    Args:
        db_conn: Active DuckDB connection.
        ticker: The ticker symbol.

    Returns:
        The latest closing price (adjusted if available, otherwise regular) as a float, or None.
    """
    # Fetch both adjusted close and regular close for the latest date
    query = """
        SELECT adj_close, close
        FROM stock_history
        WHERE ticker = ? COLLATE NOCASE
        ORDER BY date DESC
        LIMIT 1;
    """
    try:
        result = db_conn.execute(query, [ticker.upper()]).fetchone()
        if result:
            adj_close_val, close_val = result
            # Prefer adjusted close if it's a valid number
            if adj_close_val is not None and isinstance(adj_close_val, (float, int)) and adj_close_val > 0:
                 price = float(adj_close_val)
                 logger.info(f"Found latest adj_close {price} for ticker {ticker.upper()}")
                 return price
            # Fallback to regular close if it's a valid number
            elif close_val is not None and isinstance(close_val, (float, int)) and close_val > 0:
                 price = float(close_val)
                 logger.info(f"Found latest close {price} (adj_close was null/invalid) for ticker {ticker.upper()}")
                 return price
            else:
                 logger.warning(f"Latest adj_close ({adj_close_val}) and close ({close_val}) prices are null or invalid for ticker {ticker.upper()}")
                 return None
        else:
            logger.warning(f"Could not find any stock price data for ticker {ticker.upper()}")
            return None
    except Exception as e:
        logger.error(f"Error querying latest stock price for {ticker.upper()}: {e}", exc_info=True)
        return None
def get_shares_outstanding(
    db_conn: duckdb.DuckDBPyConnection,
    cik: str,
    ticker: Optional[str] = None, # Provide if known, helps lookup yf_profile
    date: Optional[Union[str, date]] = 'latest' # Find shares closest to this date
) -> Optional[float]:
    """
    Attempts to find shares outstanding for a CIK, prioritizing yf_profile_metrics
    then specific XBRL tags ('WeightedAverageNumberOfSharesOutstandingBasic', 'EntityCommonStockSharesOutstanding').

    Note: This is complex due to reporting variations (basic vs diluted, end-of-period vs avg).
          This function provides a best effort using common sources.

    Args:
        db_conn: Active DuckDB connection.
        cik: The CIK of the company.
        ticker: Optional ticker symbol to help lookup yfinance data.
        date: Date ('YYYY-MM-DD', date object, or 'latest') to find shares closest to.

    Returns:
        Shares outstanding as a float, or None.
    """
    shares = None
    cik_padded = str(cik).zfill(10) # Ensure correct padding

    # 1. Try yf_profile_metrics (usually most recent)
    if ticker:
        try:
            yf_query = """
                SELECT sharesOutstanding, floatShares, fetch_timestamp
                FROM yf_profile_metrics
                WHERE ticker = ? COLLATE NOCASE
                ORDER BY fetch_timestamp DESC
                LIMIT 1;
            """
            result = db_conn.execute(yf_query, [ticker.upper()]).fetchone()
            if result:
                # Prioritize sharesOutstanding, fallback to floatShares
                shares_val = result[0] if result[0] is not None else result[1]
                if shares_val is not None:
                    shares = float(shares_val)
                    logger.info(f"Found shares {shares} from yf_profile_metrics for ticker {ticker.upper()}")
                    return shares # Return immediately if found here
        except Exception as e:
            logger.warning(f"Error querying yf_profile_metrics for shares (Ticker: {ticker.upper()}): {e}")

    # 2. Try common XBRL tags if yfinance data failed or wasn't available
    logger.info(f"Trying XBRL tags for shares outstanding (CIK: {cik_padded})")
    shares_tags = [
        'WeightedAverageNumberOfSharesOutstandingBasic', # Often used for EPS
        'EntityCommonStockSharesOutstanding', # End of period shares
        # Add other potential tags here if needed
    ]

    # Determine date filtering for XBRL query
    end_date_filter = None
    if date != 'latest':
        try:
            end_date_filter = pd.to_datetime(date).date()
        except Exception as e:
            logger.warning(f"Invalid date format '{date}' for shares outstanding query, defaulting to latest. Error: {e}")
            date = 'latest'

    # Build XBRL query dynamically based on date requirement
    xbrl_query = """
    SELECT tag_name, value_numeric, period_end_date
    FROM xbrl_facts
    WHERE cik = ?
      AND tag_name IN ({})
      AND value_numeric IS NOT NULL
    """.format(','.join('?'*len(shares_tags)))
    params = [cik_padded] + shares_tags

    if date == 'latest':
        xbrl_query += " ORDER BY period_end_date DESC, filed_date DESC LIMIT 10;" # Fetch a few recent ones
    else:
        xbrl_query += " AND period_end_date <= ? ORDER BY period_end_date DESC, filed_date DESC LIMIT 10;"
        params.append(end_date_filter)

    try:
        results_df = db_conn.execute(xbrl_query, params).fetchdf()
        if not results_df.empty:
            # Prefer WeightedAverage if available and close to target date, else EntityCommonStock...
            results_df['period_end_date'] = pd.to_datetime(results_df['period_end_date']).dt.date
            target_dt = end_date_filter if end_date_filter else results_df['period_end_date'].max()

            # Find closest WeightedAverage...
            weighted_avg = results_df[results_df['tag_name'] == 'WeightedAverageNumberOfSharesOutstandingBasic'].copy()
            if not weighted_avg.empty:
                 weighted_avg['date_diff'] = (target_dt - weighted_avg['period_end_date']).abs()
                 best_weighted_avg = weighted_avg.sort_values('date_diff').iloc[0]
                 shares = float(best_weighted_avg['value_numeric'])
                 logger.info(f"Found shares {shares} from XBRL tag '{best_weighted_avg['tag_name']}' (Period end: {best_weighted_avg['period_end_date']})")
                 return shares

            # Fallback to EntityCommonStock...
            entity_common = results_df[results_df['tag_name'] == 'EntityCommonStockSharesOutstanding'].copy()
            if not entity_common.empty:
                 entity_common['date_diff'] = (target_dt - entity_common['period_end_date']).abs()
                 best_entity_common = entity_common.sort_values('date_diff').iloc[0]
                 shares = float(best_entity_common['value_numeric'])
                 logger.info(f"Found shares {shares} from XBRL tag '{best_entity_common['tag_name']}' (Period end: {best_entity_common['period_end_date']})")
                 return shares

    except Exception as e:
        logger.error(f"Error querying XBRL facts for shares (CIK: {cik_padded}): {e}", exc_info=True)

    logger.warning(f"Could not determine shares outstanding for CIK {cik_padded} / Ticker {ticker}")
    return None


# --- Metric/Ratio Calculation Functions ---

def calculate_eps(
    db_conn: duckdb.DuckDBPyConnection,
    cik: str,
    form: str = '10-K', # Typically calculate annual EPS
    fiscal_year: Optional[int] = None, # Specify year, otherwise latest
    period_end_date: Optional[Union[str, date]] = None # Alternative period specification
) -> Optional[float]:
    """
    Calculates Earnings Per Share (EPS) for a given CIK and period.
    Uses NetIncomeLoss and WeightedAverageNumberOfSharesOutstandingBasic.

    Args:
        db_conn: Active DuckDB connection.
        cik: The company CIK.
        form: Filing form to use (e.g., '10-K' for annual).
        fiscal_year: Fiscal year to calculate for (if None, finds latest matching 'form').
        period_end_date: Specific period end date (overrides fiscal_year if provided).

    Returns:
        Calculated EPS as a float, or None if data is missing.
    """
    cik_padded = str(cik).zfill(10)
    tags_needed = ['NetIncomeLoss', 'WeightedAverageNumberOfSharesOutstandingBasic']

    # Construct filters for get_financial_data
    filters = {'forms': [form]}
    if period_end_date:
         # Use a small range around the date to find the specific filing period end
         target_date = pd.to_datetime(period_end_date).date()
         filters['start_date'] = target_date - pd.Timedelta(days=5)
         filters['end_date'] = target_date + pd.Timedelta(days=5)
    elif fiscal_year:
         filters['fiscal_years'] = [fiscal_year]
    # If neither date nor year specified, get_financial_data will order by date desc

    try:
        # Fetch Net Income and Weighted Avg Shares
        financial_data = get_financial_data(
            db_conn,
            ciks=[cik_padded],
            tags=tags_needed,
            **filters # Pass dictionary of filters
        )

        if financial_data.empty or len(financial_data['tag_name'].unique()) < 2:
            logger.warning(f"Missing NetIncome or Shares data for CIK {cik_padded} (Filters: {filters})")
            return None

        # If multiple periods returned (no year/date specified), use the most recent one
        if period_end_date:
             # Select the exact period end date if specified
             period_data = financial_data[financial_data['period_end_date'] == target_date]
             if period_data.empty: # Fallback if exact date not found but nearby was
                 period_data = financial_data[financial_data['period_end_date'] == financial_data['period_end_date'].max()]
        else:
            # Use the period with the latest end date
            period_data = financial_data[financial_data['period_end_date'] == financial_data['period_end_date'].max()]


        if period_data.empty or len(period_data['tag_name'].unique()) < 2:
             logger.warning(f"Data retrieved but couldn't isolate required tags for a single period (CIK: {cik_padded}, Filters: {filters})")
             return None

        net_income = period_data.loc[period_data['tag_name'] == 'NetIncomeLoss', 'value_numeric'].iloc[0]
        shares = period_data.loc[period_data['tag_name'] == 'WeightedAverageNumberOfSharesOutstandingBasic', 'value_numeric'].iloc[0]

        if pd.isna(net_income) or pd.isna(shares) or shares == 0:
            logger.warning(f"Invalid NetIncome ({net_income}) or Shares ({shares}) for EPS calculation (CIK: {cik_padded})")
            return None

        eps = net_income / shares
        logger.info(f"Calculated EPS {eps:.4f} for CIK {cik_padded} (Period End: {period_data['period_end_date'].iloc[0]})")
        return float(eps)

    except Exception as e:
        logger.error(f"Error calculating EPS for CIK {cik_padded}: {e}", exc_info=True)
        return None

def calculate_roe(
    db_conn: duckdb.DuckDBPyConnection,
    cik: str,
    form: str = '10-K', # Typically calculate annual ROE
    fiscal_year: Optional[int] = None,
    period_end_date: Optional[Union[str, date]] = None
) -> Optional[float]:
    """
    Calculates Return on Equity (ROE). Uses NetIncomeLoss and StockholdersEquity.
    Needs average equity (requires data from current and prior period).

    Args:
        db_conn: Active DuckDB connection.
        cik: The company CIK.
        form: Filing form to use (e.g., '10-K').
        fiscal_year: Fiscal year to calculate for (requires this year and prior year's equity). If None, finds latest.
        period_end_date: Specific period end date (requires this period and prior period's equity).

    Returns:
        Calculated ROE as a float (percentage * 100), or None if data is missing.
    """
    cik_padded = str(cik).zfill(10)
    tags_needed = ['NetIncomeLoss', 'StockholdersEquity'] # Or 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'

    # Determine target period and prior period
    target_fy = None; target_ped = None; prior_fy = None; prior_ped = None

    try:
        if period_end_date:
            target_ped = pd.to_datetime(period_end_date).date()
            # Attempt to infer prior period date (assuming annual for simplicity)
            # More robust logic would check form/fy/fp
            prior_ped = target_ped - pd.DateOffset(years=1) # Approximate
            date_range_end = target_ped + pd.Timedelta(days=5)
            date_range_start = prior_ped - pd.Timedelta(days=370) # ~1 year + buffer
        elif fiscal_year:
            target_fy = fiscal_year
            prior_fy = fiscal_year - 1
            date_range_start = None; date_range_end = None # Filter by FY later
        else: # Get latest available
            # Find latest period end date for the form first
            latest_date_q = "SELECT MAX(period_end_date) FROM xbrl_facts WHERE cik = ? AND form = ? AND tag_name = 'StockholdersEquity'"
            latest_date_res = db_conn.execute(latest_date_q, [cik_padded, form]).fetchone()
            if not latest_date_res or not latest_date_res[0]:
                 logger.warning(f"Could not find latest period end date for ROE calc (CIK: {cik_padded}, Form: {form})"); return None
            target_ped = pd.to_datetime(latest_date_res[0]).date()
            prior_ped = target_ped - pd.DateOffset(years=1) # Approximate
            date_range_end = target_ped + pd.Timedelta(days=5)
            date_range_start = prior_ped - pd.Timedelta(days=370)

        # Fetch data covering both periods
        filters = {'forms': [form]}
        if target_fy and prior_fy: filters['fiscal_years'] = [target_fy, prior_fy]
        if date_range_start: filters['start_date'] = date_range_start
        if date_range_end: filters['end_date'] = date_range_end

        financial_data = get_financial_data(
            db_conn, ciks=[cik_padded], tags=tags_needed, **filters
        )

        if financial_data.empty:
            logger.warning(f"Missing Income or Equity data for ROE (CIK: {cik_padded}, Filters: {filters})"); return None

        # Separate Net Income (target period) and Equity (target and prior)
        net_income_data = financial_data[financial_data['tag_name'] == 'NetIncomeLoss']
        equity_data = financial_data[financial_data['tag_name'] == 'StockholdersEquity'] # Adjust tag if needed

        if net_income_data.empty or equity_data.empty:
             logger.warning(f"Could not find both NetIncome and Equity tags (CIK: {cik_padded})"); return None

        # Find target net income (closest to target_ped or matching target_fy)
        if target_ped:
             net_income_target = net_income_data.iloc[(net_income_data['period_end_date'] - target_ped).abs().argsort()[:1]]
        else: # target_fy must be set
             net_income_target = net_income_data[net_income_data['fy'] == target_fy]

        if net_income_target.empty: logger.warning(f"Could not find target Net Income (CIK: {cik_padded})"); return None
        net_income = net_income_target['value_numeric'].iloc[0]

        # Find target and prior equity
        if target_ped:
            equity_target = equity_data.iloc[(equity_data['period_end_date'] - target_ped).abs().argsort()[:1]]
            equity_prior = equity_data.iloc[(equity_data['period_end_date'] - prior_ped.date()).abs().argsort()[:1]]
        else: # fiscal years must be set
            equity_target = equity_data[equity_data['fy'] == target_fy]
            equity_prior = equity_data[equity_data['fy'] == prior_fy]

        if equity_target.empty or equity_prior.empty: logger.warning(f"Could not find target or prior Equity (CIK: {cik_padded})"); return None

        # Check if periods found are distinct enough (e.g., ~1 year apart)
        target_equity_date = equity_target['period_end_date'].iloc[0]
        prior_equity_date = equity_prior['period_end_date'].iloc[0]
        if abs((target_equity_date - prior_equity_date).days) < 300: # Heuristic check
             logger.warning(f"Target ({target_equity_date}) and Prior ({prior_equity_date}) equity periods are too close for ROE calc (CIK: {cik_padded})")
             # Could try fetching wider date range if needed
             return None

        equity_avg = (equity_target['value_numeric'].iloc[0] + equity_prior['value_numeric'].iloc[0]) / 2

        if pd.isna(net_income) or pd.isna(equity_avg) or equity_avg == 0:
            logger.warning(f"Invalid NetIncome ({net_income}) or Avg Equity ({equity_avg}) for ROE calculation (CIK: {cik_padded})")
            return None

        roe = (net_income / equity_avg) * 100 # As percentage
        logger.info(f"Calculated ROE {roe:.2f}% for CIK {cik_padded} (NetIncome Period: {net_income_target['period_end_date'].iloc[0]}, Avg Equity Periods: {prior_equity_date} to {target_equity_date})")
        return float(roe)

    except Exception as e:
        logger.error(f"Error calculating ROE for CIK {cik_padded}: {e}", exc_info=True)
        return None


def calculate_pe_ratio(
    db_conn: duckdb.DuckDBPyConnection,
    ticker: str,
    eps_form: str = '10-K', # Base P/E on annual EPS usually
    eps_fiscal_year: Optional[int] = None # Specify year, otherwise latest annual
) -> Optional[float]:
    """
    Calculates the Price-to-Earnings (P/E) ratio.

    Args:
        db_conn: Active DuckDB connection.
        ticker: The stock ticker symbol.
        eps_form: The form type used for EPS calculation ('10-K' or '10-Q').
        eps_fiscal_year: The fiscal year for the EPS calculation (defaults to latest).

    Returns:
        Calculated P/E ratio as float, or None.
    """
    ticker = ticker.upper()
    logger.info(f"Calculating P/E ratio for {ticker}...")

    # 1. Get CIK for the ticker
    cik = get_company_cik(db_conn, ticker, identifier_type='ticker')
    if not cik:
        logger.error(f"Cannot calculate P/E: Failed to get CIK for ticker {ticker}")
        return None

    # 2. Get the latest stock price
    latest_price = get_latest_stock_price(db_conn, ticker)
    if latest_price is None or latest_price <= 0: # Price must be positive
        logger.error(f"Cannot calculate P/E for {ticker}: Latest stock price unavailable or non-positive ({latest_price})")
        return None

    # 3. Calculate the relevant EPS
    # If fiscal year not specified, calculate_eps finds the latest for the form
    eps = calculate_eps(db_conn, cik, form=eps_form, fiscal_year=eps_fiscal_year)
    if eps is None or eps <= 0: # Denominator for P/E must be positive for standard interpretation
        logger.error(f"Cannot calculate P/E for {ticker}: EPS unavailable or non-positive ({eps}) for the specified period.")
        return None

    # 4. Calculate P/E
    pe_ratio = latest_price / eps
    logger.info(f"Calculated P/E {pe_ratio:.2f} for {ticker} (Price: {latest_price:.2f}, EPS: {eps:.4f})")
    return float(pe_ratio)


# --- Example Usage (If run directly) ---
# (Keep the existing example usage block, but update it to call new functions)
if __name__ == "__main__":
    logger.info("Running analysis functions script in example mode.")

    try:
        from config_utils import AppConfig
        config = AppConfig(calling_script_path=Path(__file__))
        DB_PATH_STR = config.DB_FILE_STR
    except Exception as e:
        logger.error(f"Could not load config for example usage: {e}")
        DB_PATH_STR = None # Set to None or a default path if config fails
        # Example: DB_PATH_STR = "/path/to/your/edgar_metadata.duckdb"

    if DB_PATH_STR:
        db_connection = connect_db_readonly(DB_PATH_STR)

        if db_connection:
            try:
                # --- Example: Use Apple (AAPL) ---
                example_ticker = "AAPL"
                logger.info(f"\n--- Running examples for ticker: {example_ticker} ---")

                # Get CIK
                apple_cik = get_company_cik(db_connection, example_ticker, identifier_type='ticker')
                logger.info(f"Example CIK for {example_ticker}: {apple_cik}")

                if apple_cik:
                    # Get Financial Data (e.g., Revenue and Assets for FY2023)
                    fin_tags = ['Revenues', 'Assets', 'NetIncomeLoss', 'StockholdersEquity', 'WeightedAverageNumberOfSharesOutstandingBasic']
                    fy2023_data = get_financial_data(db_connection, ciks=[apple_cik], tags=fin_tags, forms=['10-K'], fiscal_years=[2023])
                    logger.info(f"\nFY2023 Financial Data Sample (CIK: {apple_cik}):")
                    print(fy2023_data.to_string())

                    # Get Stock History (last 30 days)
                    end_hist_date = date.today()
                    start_hist_date = end_hist_date - pd.Timedelta(days=30)
                    stock_hist = get_stock_history(db_connection, tickers=[example_ticker], start_date=start_hist_date, end_date=end_hist_date)
                    logger.info(f"\nRecent Stock History Sample (Ticker: {example_ticker}):")
                    print(stock_hist.head().to_string())

                    # Get Latest Price
                    latest_price = get_latest_stock_price(db_connection, example_ticker)
                    logger.info(f"\nLatest Price for {example_ticker}: {latest_price}")

                    # Get Shares Outstanding
                    shares_out = get_shares_outstanding(db_connection, cik=apple_cik, ticker=example_ticker, date='latest')
                    logger.info(f"\nShares Outstanding for {example_ticker} (CIK: {apple_cik}): {shares_out}")

                    # Calculate latest Annual EPS
                    latest_annual_eps = calculate_eps(db_connection, cik=apple_cik, form='10-K', fiscal_year=None) # None gets latest
                    logger.info(f"\nLatest Annual EPS for {example_ticker} (CIK: {apple_cik}): {latest_annual_eps}")

                    # Calculate latest Annual ROE
                    latest_annual_roe = calculate_roe(db_connection, cik=apple_cik, form='10-K', fiscal_year=None) # None gets latest
                    logger.info(f"\nLatest Annual ROE for {example_ticker} (CIK: {apple_cik}): {latest_annual_roe}%")

                    # Calculate P/E Ratio (using latest price and latest annual EPS)
                    pe_ratio = calculate_pe_ratio(db_connection, ticker=example_ticker, eps_form='10-K', eps_fiscal_year=None)
                    logger.info(f"\nP/E Ratio for {example_ticker}: {pe_ratio}")

                else:
                    logger.warning(f"Could not find CIK for {example_ticker}, skipping dependent examples.")

            except Exception as ex:
                logger.error(f"An error occurred during example execution: {ex}", exc_info=True)
            finally:
                try:
                    db_connection.close()
                    logger.info("Example mode database connection closed.")
                except Exception as close_e:
                    logger.error(f"Error closing connection: {close_e}")
        else:
            logger.error("Could not connect to database for example usage.")
    else:
        logger.error("Database path not configured. Cannot run example usage.")