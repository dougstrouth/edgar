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
import sys

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.database_conn import get_db_connection

# --- Setup Logging ---
logger = logging.getLogger(__name__)

class AnalysisClient:
    """
    A client for analyzing data in the EDGAR DuckDB database.

    Manages a read-only connection and provides methods for common queries.
    """
    def __init__(self, db_path: Union[str, Path]):
        """
        Initializes the client and establishes a read-only database connection.

        Args:
            db_path: The path to the DuckDB database file.
        """
        self.db_path = str(db_path)
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._connect()

    def _connect(self):
        """Establishes the read-only database connection."""
        # Directly use the get_db_connection utility for a cleaner implementation.
        # The utility function handles logging for success or failure.
        self.conn = get_db_connection(db_path_override=self.db_path, read_only=True)

    def close(self):
        """Closes the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed by AnalysisClient.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}", exc_info=True)

    def get_company_cik(self, identifier: str, identifier_type: str = 'ticker') -> Optional[str]:
        """
        Retrieves the CIK for a company based on its ticker symbol or primary name.

        Args:
            identifier: The ticker symbol or company name.
            identifier_type: 'ticker' or 'name'.

        Returns:
            The CIK as a string, or None if not found.
        """
        if not self.conn:
            logger.error("No database connection available.")
            return None
        identifier = identifier.strip()
        try:
            if identifier_type.lower() == 'ticker':
                query = "SELECT DISTINCT t.cik FROM tickers t WHERE t.ticker = ? LIMIT 1;"
                result = self.conn.execute(query, [identifier.upper()]).fetchone()
            elif identifier_type.lower() == 'name':
                query = "SELECT c.cik FROM companies c WHERE c.primary_name ILIKE ? LIMIT 1;"
                result = self.conn.execute(query, [f'%{identifier}%']).fetchone()
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

    def get_financial_facts(self, cik: str, tags: List[str], forms: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Retrieves specific financial facts for a given CIK.

        Args:
            cik: The CIK of the company.
            tags: A list of US-GAAP tag names to retrieve.
            forms: An optional list of form types to include (e.g., ['10-K', '10-Q']).

        Returns:
            A pandas DataFrame containing the requested facts.
        """
        if not self.conn:
            logger.error("No database connection available."); return pd.DataFrame()
        if not isinstance(tags, list) or not tags:
            logger.error("Tags list cannot be empty."); return pd.DataFrame()

        params = [cik] + tags
        form_clause = ""
        if forms and isinstance(forms, list):
            form_clause = f"AND f.form IN ({','.join('?' * len(forms))})"
            params.extend(forms)

        query = f"""
            SELECT f.cik, f.form, f.filed_date, f.period_end_date, f.fp, f.tag_name, f.value_numeric, f.unit
            FROM xbrl_facts f
            WHERE f.cik = ? AND f.tag_name IN ({','.join('?' * len(tags))}) {form_clause}
            ORDER BY f.period_end_date ASC, f.filed_date ASC;
        """

        logger.info(f"Querying financial facts for CIK {cik}, Tags: {tags}")
        df = self.conn.execute(query, params).fetchdf()
        return df

    def get_cash_flow_data(self, cik: str, tags: List[str], forms: List[str] = ['10-K', '10-Q']) -> pd.DataFrame:
        """
        Retrieves cash flow related facts for a specific CIK, set of tags, and form types.

        Args:
            cik: The CIK of the company.
            tags: A list of US-GAAP tag names to retrieve.
            forms: A list of form types to include.

        Returns:
            A pandas DataFrame containing the requested facts.
        """
        if not self.conn:
            logger.error("No database connection available."); return pd.DataFrame()
        if not isinstance(tags, list) or not tags:
            logger.error("Tags list cannot be empty."); return pd.DataFrame()
        if not isinstance(forms, list) or not forms:
            logger.error("Forms list cannot be empty."); return pd.DataFrame()

        query = """
        SELECT f.cik, f.form, f.filed_date, f.period_end_date, f.fp, f.tag_name, f.value_numeric, f.unit
        FROM xbrl_facts f
        WHERE f.cik = ?
          AND f.tag_name IN ({})
          AND f.form IN ({})
          AND f.unit = 'USD'
        ORDER BY f.period_end_date ASC, f.filed_date ASC;
        """.format(','.join('?' * len(tags)), ','.join('?' * len(forms)))

        params = [cik] + tags + forms

        try:
            logger.info(f"Querying cash flow data for CIK {cik}, Tags: {tags}, Forms: {forms}")
            df = self.conn.execute(query, params).fetchdf()
            logger.info(f"Retrieved {len(df)} cash flow fact records.")
            if not df.empty:
                df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')
                df['filed_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
            return df
        except Exception as e:
            logger.error(f"Error querying cash flow data for CIK {cik}: {e}", exc_info=True)
            return pd.DataFrame()


# --- Example Usage (If run directly) ---
# if __name__ == "__main__":
#     logger.info("Running analysis functions script in example mode.")
#
#     # --- Requires config_utils for DB path in example ---
#     client = None
#     try:
#         from config_utils import AppConfig
#         config = AppConfig(calling_script_path=Path(__file__))
#         DB_PATH_STR = config.DB_FILE_STR
#
#         # Use the new AnalysisClient
#         client = AnalysisClient(DB_PATH_STR)
#         if not client.conn:
#             raise ConnectionError("Failed to connect to the database.")
#
#         # Example 1: Get CIK for Apple
#         apple_cik = client.get_company_cik("AAPL", identifier_type='ticker')
#         logger.info(f"Example CIK for AAPL: {apple_cik}")
#
#         # Example 2: Get Cash Flow Data for Apple (if CIK found)
#         if apple_cik:
#             cf_tags = [
#                 'NetCashProvidedByUsedInOperatingActivities',
#                 'NetCashProvidedByUsedInInvestingActivities',
#                 'NetCashProvidedByUsedInFinancingActivities'
#             ]
#             apple_cf_data = client.get_cash_flow_data(apple_cik, cf_tags)
#             if not apple_cf_data.empty:
#                 logger.info(f"Retrieved {len(apple_cf_data)} cash flow records for AAPL.")
#                 print("\nSample AAPL Cash Flow Data:")
#                 print(apple_cf_data.head().to_string())
#             else:
#                 logger.warning("No cash flow data retrieved for AAPL example.")
#
#     except Exception as ex:
#         logger.error(f"An error occurred during example execution: {ex}", exc_info=True)
#     finally:
#         if client:
#             client.close()