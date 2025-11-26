# -*- coding: utf-8 -*-
"""
EDGAR & YFinance Database Validation Script (Refactored)

Connects to the populated DuckDB database using utilities and runs
various checks to validate data integrity, types, relationships for
both EDGAR and supplementary Yahoo Finance data.

Uses:
- config_utils.AppConfig for DB path.
- logging_utils.setup_logging for logging.
- database_conn.ManagedDatabaseConnection for connection.
"""

import duckdb
import sys
import logging # Keep for level constants
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
try:
    from utils.config_utils import AppConfig
    from utils.logging_utils import setup_logging
    from utils.database_conn import ManagedDatabaseConnection
except ImportError as e:
    print(f"FATAL: Could not import utility modules: {e}", file=sys.stderr)
    sys.exit(1)

# --- Setup Logging ---
# Logger is instantiated in the main block after config is loaded

# --- List of Tables ---
# Tables loaded by edgar_data_loader.py
EDGAR_TABLES = ["companies", "tickers", "former_names", "filings", "xbrl_tags", "xbrl_facts", "xbrl_facts_orphaned"]
# Tables loaded by stock_data_gatherer.py
STOCK_TABLES = ["stock_history", "stock_fetch_errors"]
# Tables from macro/market sources
ECONOMIC_TABLES = ["macro_economic_data", "market_risk_factors"]

ALL_TABLES = EDGAR_TABLES + STOCK_TABLES + ECONOMIC_TABLES

# Additional supplementary tables used by gatherers/status tracking
SUPPLEMENTARY_OPTIONAL = [
    'yf_untrackable_tickers',
    'yf_fetch_status'
]


# --- Validation Helper Function ---
def run_validation_query(con: duckdb.DuckDBPyConnection, query: str, description: str, logger: logging.Logger, expect_zero: bool = False, warning_threshold: Optional[int] = None) -> Tuple[Optional[str], Optional[int], str]:
    """Runs a validation query, logs the result, and optionally flags non-zero counts."""
    logger.info(f"--- Running Check: {description} ---")
    try:
        result_df = con.sql(query).df()
        # Log full result only if it's small or specifically requested, otherwise summarize
        if len(result_df) < 10: logger.info(f"Result:\n{result_df.to_string(index=False)}")
        else: logger.info(f"Result: Query returned {len(result_df)} rows. Sample:\n{result_df.head().to_string(index=False)}")

        # Check if the result indicates an issue
        failure_type: Optional[str] = None
        count_val = None
        if "count" in result_df.columns and not result_df.empty:
            count_val = result_df['count'].iloc[0]
            if expect_zero and count_val != 0:
                failure_type = 'error'
                logger.warning(f"FAILED Check '{description}': Expected 0 count, got {count_val}.")
            elif warning_threshold is not None and count_val > warning_threshold:
                failure_type = 'warning'
                logger.warning(f"THRESHOLD Check '{description}': Count {count_val} exceeds threshold {warning_threshold}.")

        # Log simplified result for summary later if needed
        return failure_type, count_val, description

    except Exception as e:
        logger.error(f"FAILED Check '{description}' with error: {e}", exc_info=True)
        return 'error', None, description # Treat execution errors as issues

# --- Specific Validation Check Functions ---

def check_table_counts(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks row counts for all expected tables."""
    results = []
    logger.info("\n=== Checking Table Row Counts ===")
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    for table in ALL_TABLES:
         if table.lower() in db_tables:
             results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table};", f"Row count for {table}", logger))
         else:
              # Treat missing EDGAR tables as errors, but supplementary tables as warnings
              if table in EDGAR_TABLES:
                  logger.error(f"MISSING Table Check: Core EDGAR table '{table}' not found.")
                  results.append(('error', None, f"Missing Table Check: {table}"))
              else:
                  logger.warning(f"SKIPPED Table Check: Supplementary table '{table}' not found.")
                  # Do not append as an issue if it's a supplementary table
    return results

def check_edgar_fk_integrity(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks foreign key relationships within EDGAR tables."""
    results = []
    logger.info("\n=== Checking EDGAR Foreign Key Integrity (Expecting 0 violations) ===")
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM tickers t LEFT JOIN companies c ON t.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: tickers -> companies", logger, expect_zero=True))
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM former_names fn LEFT JOIN companies c ON fn.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: former_names -> companies", logger, expect_zero=True))
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM filings f LEFT JOIN companies c ON f.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: filings -> companies", logger, expect_zero=True))
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN companies c ON f.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: xbrl_facts -> companies", logger, expect_zero=True))
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN filings fi ON f.accession_number = fi.accession_number WHERE fi.accession_number IS NULL;", "FK Violation Check: xbrl_facts -> filings", logger, expect_zero=True))
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN xbrl_tags t ON f.taxonomy = t.taxonomy AND f.tag_name = t.tag_name WHERE t.taxonomy IS NULL;", "FK Violation Check: xbrl_facts -> xbrl_tags", logger, expect_zero=True))
    return results

def check_orphaned_facts(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks the orphaned facts table."""
    results = []
    logger.info("\n=== Checking Orphaned Facts Table ===")
    results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM xbrl_facts_orphaned;", "Orphan Check: Total orphaned facts", logger, warning_threshold=1000)) # Example threshold
    results.append(run_validation_query(con, "SELECT DISTINCT cik, accession_number FROM xbrl_facts_orphaned LIMIT 10;", "Orphan Check: Sample orphaned CIKs/Acc Nums", logger))
    # Check if any orphaned facts now have a matching CIK and filing (should be 0 after reprocessing logic)
    results.append(run_validation_query(con, "SELECT COUNT(o.*) as count FROM xbrl_facts_orphaned o JOIN filings f ON o.accession_number = f.accession_number AND o.cik = f.cik;", "Orphan Check: Orphaned facts with existing filings", logger, expect_zero=True))
    return results

def check_xbrl_facts_validations(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """
    Runs various checks on the xbrl_facts table for data integrity.
    """
    results = []
    logger.info("\n=== Checking XBRL Facts Data Integrity ===")
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    table_name = "xbrl_facts"

    if table_name in db_tables:
        # Check for NULLs in key columns
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE cik IS NULL OR accession_number IS NULL OR taxonomy IS NULL OR tag_name IS NULL OR form IS NULL;", f"{table_name}: NULL values in key identifier columns", logger, expect_zero=True))
        
        # Check for NULLs in financial value columns where unit is a monetary value
        monetary_units = ['USD', 'EUR', 'JPY', 'GBP', 'CAD', 'CHF', 'AUD', 'CNY', 'INR'] # Add more as needed
        monetary_units_str = "', '".join(monetary_units)
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE unit IN ('{monetary_units_str}') AND value_numeric IS NULL;", f"{table_name}: NULL values for monetary facts", logger, expect_zero=True))

        # Check for invalid date/time values
        # results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE period_end_date > filed_date;", f"{table_name}: period_end_date is after filed_date", logger, expect_zero=True))
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE filed_date IS NULL;", f"{table_name}: NULL filed_date date", logger, expect_zero=True))

        # Check for facts with no specified unit
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE unit IS NULL OR unit = '';", f"{table_name}: Facts with no unit specified", logger, warning_threshold=1000))

        # Informational check for outlier values in common financial tags (e.g., Assets, Revenues)
        # This is a simple example; more sophisticated outlier detection might be needed
        outlier_tags = ['Assets', 'Revenues', 'NetIncomeLoss']
        for tag in outlier_tags:
            query = f"""
                SELECT cik, accession_number, value_numeric
                FROM {table_name}
                WHERE tag_name = '{tag}' AND ABS(value_numeric) > 1e14 
                ORDER BY ABS(value_numeric) DESC
                LIMIT 5;
            """
            results.append(run_validation_query(con, query, f"{table_name}: Potential outliers for tag '{tag}'", logger, warning_threshold=0))

    return results

# Removed YF data validations per project scope change

def check_ticker_consistency(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks if tickers in stock tables exist in the main 'tickers' table."""
    results = []
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    logger.info("\n=== Checking Ticker Consistency Across Tables (Expecting 0 violations) ===")
    stock_tables_with_ticker = [
        "stock_history"
    ]

    # Build a unified ticker source that includes authoritative EDGAR tickers
    # and, if available, enriched tickers from the Massive/Polygon reference table.
    ticker_source_sql = "(SELECT DISTINCT ticker FROM tickers)"
    if 'massive_tickers' in db_tables:
        ticker_source_sql = (
            "(SELECT DISTINCT ticker FROM tickers "
            "UNION SELECT DISTINCT ticker FROM massive_tickers)"
        )
    for table in stock_tables_with_ticker:
        if table.lower() in db_tables:
            query = f"""
                SELECT COUNT(t1.ticker) as count
                FROM {table} t1
                LEFT JOIN {ticker_source_sql} t2 ON t1.ticker = t2.ticker
                WHERE t2.ticker IS NULL;
            """
            results.append(run_validation_query(con, query, f"Ticker Consistency Check: {table} -> tickers", logger, expect_zero=True))
    # Check the other way? Tickers in `tickers` without stock/yf data? Optional.
    # query_missing_yf = """
    #     SELECT COUNT(t.ticker) as count
    #     FROM tickers t
    #     LEFT JOIN yf_profile_metrics yf ON t.ticker = yf.ticker
    #     WHERE yf.ticker IS NULL;
    # """
    # results.append(run_validation_query(con, query_missing_yf, "Ticker Consistency Check: tickers missing yf_profile", logger, warning_threshold=100)) # Allow some missing
    return results

def check_market_risk_validations(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Runs various checks on the Fama-French market risk data."""
    results = []
    logger.info("\n=== Checking Market Risk Data Integrity ===")

    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    table_name = "market_risk_factors"

    if table_name in db_tables:
        # Check for NULLs in primary key columns
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE date IS NULL OR factor_model IS NULL;", f"{table_name}: NULL PK Check", logger, expect_zero=True))
        
        # Check for NULLs in the main market risk premium column
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE mkt_minus_rf IS NULL;", f"{table_name}: NULL Market Risk Premium Check", logger, expect_zero=True))

        # Informational check for distinct models
        results.append(run_validation_query(con, f"SELECT DISTINCT factor_model FROM {table_name};", f"{table_name}: Distinct Factor Models", logger))
    
    return results


def check_stock_history_validations(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Run validations specific to `stock_history` table."""
    results = []
    logger.info("\n=== Checking Stock History Data Integrity ===")
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    table_name = 'stock_history'

    if table_name in db_tables:
        # NULL PKs
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE ticker IS NULL OR date IS NULL;", f"{table_name}: NULL PK Check", logger, expect_zero=True))

        # Negative or impossible volumes
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE volume < 0;", f"{table_name}: Negative volume values", logger, expect_zero=True))

        # Check for missing price columns when volume exists
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE (open IS NULL AND close IS NULL AND adj_close IS NULL) AND volume IS NOT NULL;", f"{table_name}: Rows with volume but no price data", logger, warning_threshold=1))

        # Recent continuity check (optional): ensure last 7 calendar days have at most N missing tickers
        try:
            import datetime as _dt
            end_dt = _dt.date.today()
            start_dt = end_dt - _dt.timedelta(days=7)
            # Define the prioritized universe: tickers from the backlog prioritization
            # minus shared Polygon 404 list. If backlog table is absent, fall back to all tickers minus 404.
            prioritized_universe_cte = f"""
                WITH backlog AS (
                    SELECT DISTINCT ticker FROM prioritized_tickers_stock_backlog
                ),
                polygon_untrackable AS (
                    SELECT DISTINCT ticker FROM polygon_untrackable_tickers
                    WHERE last_failed_timestamp >= (CURRENT_DATE - INTERVAL 365 DAY)
                ),
                prioritized AS (
                    SELECT DISTINCT b.ticker
                    FROM backlog b
                    LEFT JOIN polygon_untrackable pu ON pu.ticker = b.ticker
                    WHERE pu.ticker IS NULL
                ),
                recent_stock AS (
                    SELECT DISTINCT ticker
                    FROM {table_name}
                    WHERE date BETWEEN DATE '{start_dt}' AND DATE '{end_dt}'
                )
            """
            # Count prioritized tickers missing recent stock history
            query = prioritized_universe_cte + "SELECT COUNT(*) AS count FROM prioritized p LEFT JOIN recent_stock rs ON p.ticker = rs.ticker WHERE rs.ticker IS NULL;"
            # Allow configuration of warning threshold via .env using AppConfig
            try:
                cfg = AppConfig(calling_script_path=Path(__file__))
                threshold = cfg.get_optional_int("STOCK_HISTORY_RECENT_MISSING_TICKERS_THRESHOLD", 100)
            except Exception:
                threshold = 100
            # If threshold is negative, treat as disabled (no warning)
            warn_threshold = None if (threshold is not None and threshold < 0) else threshold
            results.append(
                run_validation_query(
                    con,
                    query,
                    f"{table_name}: Prioritized tickers missing recent data (last 7 days, excluding 404, info<30d)",
                    logger,
                    warning_threshold=warn_threshold,
                )
            )
        except Exception:
            # Do not fail validation if date arithmetic isn't supported in the DB environment
            logger.debug("Skipping recent continuity check due to date arithmetic error.")

    return results


def check_error_tables(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks for the presence of errors in the fetch error tables."""
    results = []
    logger.info("\n=== Checking Fetch Error Tables ===")
    error_tables = ["stock_fetch_errors", "yf_info_fetch_errors"]
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}

    for table in error_tables:
        if table.lower() in db_tables:
            results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table};", f"Error Count Check: {table}", logger, warning_threshold=0))
    return results


def check_supplementary_presence(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Report presence/absence of optional supplementary tables used by various gatherers."""
    results = []
    logger.info("\n=== Checking Supplementary Table Presence ===")
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    for table in SUPPLEMENTARY_OPTIONAL:
        if table.lower() not in db_tables:
            logger.warning(f"Supplementary table '{table}' not found. This is optional but may indicate missing data pipelines.")
        else:
            results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table};", f"Supplementary presence: {table}", logger))
    return results


def check_macro_data_validations(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Runs various checks on the macroeconomic data."""
    results = []
    logger.info("\n=== Checking Macroeconomic Data Integrity ===")
    table_name = "macro_economic_data"
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}

    if table_name in db_tables:
        # Check for NULLs in primary key columns
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE date IS NULL OR series_id IS NULL;", f"{table_name}: NULL PK Check", logger, expect_zero=True))
        
        # Check for NULLs in the value column
        results.append(run_validation_query(con, f"SELECT COUNT(*) as count FROM {table_name} WHERE value IS NULL;", f"{table_name}: NULL Value Check", logger, expect_zero=True))

        # Informational check for distinct series
        results.append(run_validation_query(con, f"SELECT DISTINCT series_id FROM {table_name};", f"{table_name}: Distinct Series", logger))
        
        # Informational check for date range
        results.append(run_validation_query(con, f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM {table_name};", f"{table_name}: Date Range", logger))
    
    return results

# --- Main Orchestration Function ---
def run_all_checks(con: duckdb.DuckDBPyConnection, logger: logging.Logger):
    """Runs all defined validation checks."""
    all_results = []
    all_results.extend(check_table_counts(con, logger))
    all_results.extend(check_edgar_fk_integrity(con, logger))
    all_results.extend(check_orphaned_facts(con, logger))
    all_results.extend(check_xbrl_facts_validations(con, logger))
    # Removed YF checks per scope change
    all_results.extend(check_table_uniqueness(con, logger))
    all_results.extend(check_market_risk_validations(con, logger))
    all_results.extend(check_ticker_consistency(con, logger)) # Add new consistency check
    all_results.extend(check_error_tables(con, logger))
    all_results.extend(check_macro_data_validations(con, logger))
    # New checks for supplementary presence and stock history integrity
    all_results.extend(check_supplementary_presence(con, logger))
    all_results.extend(check_stock_history_validations(con, logger))

    # --- Summarize Results ---
    logger.info("\n=== Validation Summary ===")
    
    hard_failures = [res for res in all_results if res[0] == 'error']
    warnings_found = [res for res in all_results if res[0] == 'warning']
    
    # Report hard failures
    if hard_failures:
        logger.error(f"{len(hard_failures)} Validation Check(s) FAILED:")
        for _, _, desc in hard_failures:
            logger.error(f"  - FAILED: {desc}")
    
    # Report warnings
    if warnings_found:
        logger.warning(f"{len(warnings_found)} Validation Warning(s) triggered:")
        for _, _, desc in warnings_found:
            logger.warning(f"  - WARNING: {desc}")

    # Final summary message
    if not hard_failures:
        if warnings_found:
            logger.warning("Validation passed with warnings. Review logs above for details.")
        else:
            logger.info("All validation checks passed successfully!")
    else:
        logger.error("Validation finished with errors. Review logs above for details.")

def check_table_uniqueness(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """
    Checks the tables for uniqueness of primary keys and essential identifiers.
    """
    results = []
    logger.info("\n=== Checking Table Uniqueness ===")
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}

    uniqueness_checks = {
        "companies": ["cik"],
        "tickers": ["ticker", "exchange"],
        "filings": ["accession_number"],
        "xbrl_tags": ["taxonomy", "tag_name"],
        "stock_history": ["ticker", "date"],
        "yf_income_statement": ["ticker", "report_date", "item_name"],
        "yf_balance_sheet": ["ticker", "report_date", "item_name"],
        "yf_cash_flow": ["ticker", "report_date", "item_name"],
        "market_risk_factors": ["date", "factor_model"],
    }

    for table, pks in uniqueness_checks.items():
        if table in db_tables:
            pk_cols = ", ".join(pks)
            query = f"""
                SELECT COUNT(*) AS count FROM (
                    SELECT {pk_cols}, COUNT(*) FROM {table} GROUP BY {pk_cols} HAVING COUNT(*) > 1
                );
            """
            results.append(run_validation_query(con, query, f"{table}: Duplicate PKs ({pk_cols})", logger, expect_zero=True))

    return results


def main():
    """Main execution function for the validation script."""
    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY) # Setup logger

    try:
        config = AppConfig(calling_script_path=Path(__file__))
        logger.info(f"--- Starting Database Validation for: {config.DB_FILE_STR} ---")
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Error during initial setup: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Use ManagedDatabaseConnection for read-only connection
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=True) as db_conn:
            if db_conn:
                logger.info("Database connection successful (read-only).")
                run_all_checks(db_conn, logger) # Run all checks
            else:
                 logger.critical("Database connection failed. Cannot run validations.")
                 sys.exit(1)
    except Exception as e:
        logger.critical(f"An unhandled error occurred during validation: {e}", exc_info=True)
        sys.exit(1)

    logger.info(f"--- Database Validation Script Finished ---")

if __name__ == "__main__":
    main()
