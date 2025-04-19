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
from typing import List, Optional, Dict, Any, Tuple

# --- Import Utilities ---
from logging_utils import setup_logging
# from database_conn import ManagedDatabaseConnection, get_db_connection # Keep if you refactor connect_db
from database_conn import get_db_connection # Use the utility function for consistency

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
# Define where logs should go (e.g., a 'logs' subdirectory relative to this script)
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
# Get the configured logger instance
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO) # Or logging.DEBUG


# --- Database Connection (Option 1: Keep original, just use logger) ---
# def connect_db(db_path: Path) -> Optional[duckdb.DuckDBPyConnection]:
#     """Connects to the DuckDB database file (read-only)."""
#     if not db_path.is_file():
#         logger.error(f"Database file not found at: {db_path}")
#         return None
#     try:
#         # Connect read-only for analysis
#         conn = duckdb.connect(database=str(db_path), read_only=True)
#         logger.info(f"Successfully connected read-only to database: {db_path}")
#         return conn
#     except Exception as e:
#         logger.error(f"Failed to connect read-only to database {db_path}: {e}", exc_info=True)
#         return None

# --- Database Connection (Option 2: Use utility function for consistency) ---
def connect_db_readonly(db_path_str: str) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Connects read-only using the centralized get_db_connection utility.

    Args:
        db_path_str: Path to the database file as a string.

    Returns:
        A read-only DuckDB connection object or None if connection fails.
    """
    logger.info(f"Attempting read-only connection to: {db_path_str}")
    # Use the utility function, ensuring read_only=True
    conn = get_db_connection(db_path_override=db_path_str, read_only=True)
    if conn:
        logger.info(f"Read-only connection successful: {db_path_str}")
    else:
        logger.error(f"Read-only connection failed: {db_path_str}")
    return conn


# --- Analysis Functions (Using 'logger' instance) ---

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
            logger.error(f"Invalid identifier_type: {identifier_type}. Use 'ticker' or 'name'.")
            return None

        if result:
            cik = result[0]
            logger.info(f"Found CIK {cik} for {identifier_type} '{identifier}'")
            return cik
        else:
            logger.warning(f"Could not find CIK for {identifier_type} '{identifier}'")
            return None
    except Exception as e:
        logger.error(f"Error querying CIK for {identifier}: {e}", exc_info=True)
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
        logger.error("Tags list cannot be empty.")
        return pd.DataFrame()
    if not isinstance(forms, list) or not forms:
        logger.error("Forms list cannot be empty.")
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
        logger.info(f"Querying cash flow data for CIK {cik}, Tags: {tags}, Forms: {forms}")
        df = db_conn.execute(query, params).fetchdf()
        logger.info(f"Retrieved {len(df)} cash flow fact records.")
        # Basic type conversion
        if not df.empty:
            df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')
            df['filed_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
        return df
    except Exception as e:
        logger.error(f"Error querying cash flow data for CIK {cik}: {e}", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on error


# --- Example Usage (If run directly) ---
# if __name__ == "__main__":
#     logger.info("Running analysis functions script in example mode.")
#
#     # --- Requires config_utils for DB path in example ---
#     try:
#         from config_utils import AppConfig
#         config = AppConfig(calling_script_path=Path(__file__))
#         DB_PATH_STR = config.DB_FILE_STR
#     except Exception as e:
#         logger.error(f"Could not load config for example usage: {e}")
#         DB_PATH_STR = None # Set to None or a default path if config fails
#         # Example: DB_PATH_STR = "/path/to/your/edgar_metadata.duckdb"
#
#     if DB_PATH_STR:
#         # Use the refactored connection function
#         db_connection = connect_db_readonly(DB_PATH_STR)
#
#         if db_connection:
#             try:
#                 # Example 1: Get CIK for Apple
#                 apple_cik = get_company_cik(db_connection, "AAPL", identifier_type='ticker')
#                 logger.info(f"Example CIK for AAPL: {apple_cik}")
#
#                 # Example 2: Get Cash Flow Data for Apple (if CIK found)
#                 if apple_cik:
#                     cf_tags = [
#                         'NetCashProvidedByUsedInOperatingActivities',
#                         'NetCashProvidedByUsedInInvestingActivities',
#                         'NetCashProvidedByUsedInFinancingActivities'
#                     ]
#                     apple_cf_data = get_cash_flow_data(db_connection, apple_cik, cf_tags)
#                     if not apple_cf_data.empty:
#                         logger.info(f"Retrieved {len(apple_cf_data)} cash flow records for AAPL.")
#                         print("\nSample AAPL Cash Flow Data:")
#                         print(apple_cf_data.head().to_string())
#                     else:
#                         logger.warning("No cash flow data retrieved for AAPL example.")
#
#             except Exception as ex:
#                 logger.error(f"An error occurred during example execution: {ex}", exc_info=True)
#             finally:
#                 # Close the connection
#                 try:
#                     db_connection.close()
#                     logger.info("Example mode database connection closed.")
#                 except Exception as close_e:
#                     logger.error(f"Error closing connection: {close_e}")
#         else:
#             logger.error("Could not connect to database for example usage.")
#     else:
#         logger.error("Database path not configured. Cannot run example usage.")