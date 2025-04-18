# edgar_analysis_functions.py
import duckdb
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

# Configure logging (optional but recommended)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')

def connect_db(db_path: Path) -> Optional[duckdb.DuckDBPyConnection]:
    """Connects to the DuckDB database file."""
    if not db_path.is_file():
        logging.error(f"Database file not found at: {db_path}")
        return None
    try:
        conn = duckdb.connect(database=str(db_path), read_only=True) # Connect read-only for analysis
        logging.info(f"Successfully connected to database: {db_path}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database {db_path}: {e}", exc_info=True)
        return None

def get_company_cik(db_conn: duckdb.DuckDBPyConnection, identifier: str, identifier_type: str = 'ticker') -> Optional[str]:
    """
    Retrieves the CIK for a company based on its ticker symbol or primary name.

    Args:
        db_conn: Active DuckDB connection.
        identifier: The ticker symbol or company name.
        identifier_type: 'ticker' or 'name'.

    Returns:
        The CIK as a string, or None if not found.
    """
    identifier = identifier.strip()
    try:
        if identifier_type.lower() == 'ticker':
            query = """
                SELECT DISTINCT t.cik
                FROM tickers t
                WHERE t.ticker = ?
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
            logging.error(f"Invalid identifier_type: {identifier_type}. Use 'ticker' or 'name'.")
            return None

        if result:
            cik = result[0]
            logging.info(f"Found CIK {cik} for {identifier_type} '{identifier}'")
            return cik
        else:
            logging.warning(f"Could not find CIK for {identifier_type} '{identifier}'")
            return None
    except Exception as e:
        logging.error(f"Error querying CIK for {identifier}: {e}", exc_info=True)
        return None


def get_cash_flow_data(db_conn: duckdb.DuckDBPyConnection, cik: str, tags: List[str], forms: List[str] = ['10-K', '10-Q']) -> pd.DataFrame:
    """
    Retrieves cash flow related facts for a specific CIK, set of tags, and form types.

    Args:
        db_conn: Active DuckDB connection.
        cik: The CIK of the company.
        tags: A list of US-GAAP tag names to retrieve (e.g., ['NetCashProvidedByUsedInOperatingActivities', ...]).
        forms: A list of form types to include (default: ['10-K', '10-Q']).

    Returns:
        A pandas DataFrame containing the requested facts, or an empty DataFrame if none found or error occurs.
    """
    if not isinstance(tags, list) or not tags:
        logging.error("Tags list cannot be empty.")
        return pd.DataFrame()
    if not isinstance(forms, list) or not forms:
        logging.error("Forms list cannot be empty.")
        return pd.DataFrame()

    query = """
    SELECT
        f.cik,
        f.form,             -- e.g., '10-K', '10-Q'
        f.filed_date,       -- Date the filing was submitted
        f.period_end_date,  -- The end date of the reporting period (crucial for time series)
        f.fp,               -- Fiscal period (e.g., 'FY', 'Q1', 'Q2')
        f.tag_name,         -- The specific XBRL tag
        f.value_numeric,    -- The numeric value of the fact
        f.unit              -- e.g., 'USD'
    FROM xbrl_facts f
    WHERE f.cik = ?
      AND f.tag_name IN ({}) -- Placeholder for tags
      AND f.form IN ({})     -- Placeholder for forms
      AND f.unit = 'USD'     -- Assuming we want USD values
    ORDER BY
        f.period_end_date ASC, f.filed_date ASC;
    """.format(','.join('?' * len(tags)), ','.join('?' * len(forms))) # Create placeholders

    params = [cik] + tags + forms

    try:
        logging.info(f"Querying cash flow data for CIK {cik}, Tags: {tags}, Forms: {forms}")
        df = db_conn.execute(query, params).fetchdf()
        logging.info(f"Retrieved {len(df)} cash flow fact records.")
        # Basic type conversion
        if not df.empty:
            df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')
            df['filed_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
        return df
    except Exception as e:
        logging.error(f"Error querying cash flow data for CIK {cik}: {e}", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on error