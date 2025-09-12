# -*- coding: utf-8 -*-
"""
EDGAR Analysis Functions (Refactored for Utilities)

Provides functions to analyze data stored in the EDGAR DuckDB database.
Uses logging_utils for standardized logging.
Connects read-only to the database.
"""

import time
import logging  # Keep for level constants (e.g., logging.INFO)
from pathlib import Path
import duckdb
import pandas as pd
from typing import List, Optional, Dict, Any, Tuple, Union

# --- Import Utilities ---
from logging_utils import setup_logging
from database_conn import get_db_connection

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

class AnalysisClient:
    """
    A client for analyzing data in the EDGAR DuckDB database.

    Manages a read-only connection and provides methods for common queries.
    """
    def __init__(self, db_path: Union[str, Path], interactive: bool = False):
        """
        Initializes the client and establishes a read-only database connection.

        Args:
            db_path: The path to the DuckDB database file.
            interactive: If True, enables features like progress bars for a better
                         interactive (e.g., Jupyter) experience.
        """
        self.db_path = str(db_path)
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.interactive = interactive
        self._connect()

    def _connect(self):
        """Establishes the read-only database connection."""
        pragmas = None
        if self.interactive:
            pragmas = {'enable_progress_bar': 'true'}

        # Directly use the get_db_connection utility for a cleaner implementation.
        # The utility function handles logging for success or failure.
        self.conn = get_db_connection(db_path_override=self.db_path, read_only=True, pragma_settings=pragmas)

    def close(self):
        """Closes the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed by AnalysisClient.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}", exc_info=True)

    def list_tables(self) -> Optional[pd.DataFrame]:
        """Lists all tables in the database with their row counts."""
        if not self.conn:
            logger.error("No database connection available.")
            return None
        try:
            return self.conn.sql("SUMMARIZE").df()
        except Exception as e:
            logger.error(f"Error listing tables: {e}", exc_info=True)
            return None

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
        query = ""
        params = []

        if identifier_type.lower() == 'ticker':
            query = "SELECT DISTINCT t.cik FROM tickers t WHERE t.ticker = ? LIMIT 1;"
            params = [identifier.upper()]
        elif identifier_type.lower() == 'name':
            query = "SELECT c.cik FROM companies c WHERE c.primary_name ILIKE ? LIMIT 1;"
            params = [f'%{identifier}%']
        else:
            logger.error(f"Invalid identifier_type: {identifier_type}. Use 'ticker' or 'name'.")
            return None

        try:
            result = self.conn.execute(query, params).fetchone()
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

    def get_financial_statements(self, ticker: str) -> pd.DataFrame:
        """
        Retrieves and combines the income statement, balance sheet, and cash flow
        statement for a given ticker, pivoted for time-series analysis.

        Args:
            ticker: The stock ticker symbol.

        Returns:
            A pandas DataFrame where each row is a report date and each column
            is a financial statement line item.
        """
        if not self.conn:
            logger.error("No database connection available."); return pd.DataFrame()

        query = """
            SELECT report_date, item_name, item_value FROM yf_income_statement WHERE ticker = ?
            UNION ALL
            SELECT report_date, item_name, item_value FROM yf_balance_sheet WHERE ticker = ?
            UNION ALL
            SELECT report_date, item_name, item_value FROM yf_cash_flow WHERE ticker = ?
        """
        try:
            logger.info(f"Querying full financial statements for {ticker}...")
            df = self.conn.execute(query, [ticker, ticker, ticker]).fetchdf()
            if df.empty:
                logger.warning(f"No financial statement data found for {ticker}.")
                return pd.DataFrame()

            # Pivot the table to get items as columns and dates as the index
            pivot_df = df.pivot(index='report_date', columns='item_name', values='item_value')
            pivot_df.sort_index(ascending=False, inplace=True) # Most recent first
            return pivot_df
        except Exception as e:
            logger.error(f"Error querying financial statements for {ticker}: {e}", exc_info=True)
            return pd.DataFrame()

    def profile_query(self, query: str, params: Optional[List[Any]] = None):
        """
        Runs EXPLAIN ANALYZE on a given query and prints the query plan.

        Args:
            query: The SQL query to profile.
            params: Optional list of parameters for the query.
        """
        if not self.conn:
            logger.error("No database connection available.")
            return

        logger.info(f"--- Profiling Query ---\n{query}\n" + ("-"*25))
        try:
            if params:
                # For parameterized queries, we must use PREPARE and EXECUTE
                # to allow EXPLAIN ANALYZE to work correctly. This is a best practice.
                stmt_name = f"profile_stmt_{int(time.time() * 1000)}"
                logger.debug(f"Preparing statement '{stmt_name}' for profiling.")
                
                # 1. PREPARE the statement with placeholders
                self.conn.execute(f"PREPARE {stmt_name} AS {query}")
                
                # 2. Build the EXECUTE call string with the parameters
                # This is necessary because EXPLAIN ANALYZE itself doesn't take bound params.
                param_str_parts = []
                for p in params:
                    if isinstance(p, str):
                        param_str_parts.append(f"'{p.replace(\"'\", \"''\")}'") # Escape single quotes
                    elif p is None:
                        param_str_parts.append("NULL")
                    else:
                        param_str_parts.append(str(p))
                param_str = f"({', '.join(param_str_parts)})"
                
                # 3. EXPLAIN ANALYZE the EXECUTE call
                plan = self.conn.execute(f"EXPLAIN ANALYZE EXECUTE {stmt_name}{param_str}").fetchall()
                self.conn.execute(f"DEALLOCATE {stmt_name}") # Clean up
            else:
                # For non-parameterized queries, a direct EXPLAIN ANALYZE is fine.
                plan = self.conn.execute(f"EXPLAIN ANALYZE {query}").fetchall()

            print("\n" + "\n".join([row[0] for row in plan]))
        except Exception as e:
            logger.error(f"Error profiling query: {e}", exc_info=True)

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