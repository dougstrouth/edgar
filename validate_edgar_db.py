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

# --- Import Utilities ---
try:
    from config_utils import AppConfig
    from logging_utils import setup_logging
    from database_conn import ManagedDatabaseConnection
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
# Tables loaded by stock_info_gatherer.py
YF_INFO_TABLES = [
    "yf_profile_metrics", "yf_stock_actions", "yf_major_holders",
    "yf_recommendations", "yf_info_fetch_errors"
]
# Tables from macro/market sources
ECONOMIC_TABLES = ["macro_economic_data", "market_risk_factors"]

ALL_TABLES = EDGAR_TABLES + STOCK_TABLES + YF_INFO_TABLES + ECONOMIC_TABLES

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

def check_yf_data_validations(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Runs various checks on the yfinance supplementary tables."""
    results = []
    logger.info("\n=== Checking Yahoo Finance Data Integrity ===")

    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}

    # Null checks on PKs / Essential Columns
    if 'yf_profile_metrics' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_profile_metrics WHERE ticker IS NULL OR fetch_timestamp IS NULL;", "yf_profile_metrics: NULL PK/Timestamp Check", logger, expect_zero=True))
    if 'yf_stock_actions' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_stock_actions WHERE ticker IS NULL OR action_date IS NULL OR action_type IS NULL;", "yf_stock_actions: NULL PK Check", logger, expect_zero=True))
    if 'yf_major_holders' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_major_holders WHERE ticker IS NULL OR fetch_timestamp IS NULL;", "yf_major_holders: NULL PK/Timestamp Check", logger, expect_zero=True))
    if 'yf_recommendations' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_recommendations WHERE ticker IS NULL OR recommendation_timestamp IS NULL OR firm IS NULL;", "yf_recommendations: NULL PK Check", logger, expect_zero=True))

    # Data Range / Consistency Checks
    if 'yf_profile_metrics' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_profile_metrics WHERE cik IS NOT NULL AND LENGTH(cik) != 10;", "yf_profile_metrics: Invalid CIK format", logger, expect_zero=True))
    if 'yf_major_holders' in db_tables:
        results.append(run_validation_query(con, "SELECT COUNT(*) as count FROM yf_major_holders WHERE pct_insiders < 0 OR pct_insiders > 1 OR pct_institutions < 0 OR pct_institutions > 1;", "yf_major_holders: Invalid Percentage Range", logger, expect_zero=True))
    if 'yf_stock_actions' in db_tables:
        results.append(run_validation_query(con, "SELECT DISTINCT action_type FROM yf_stock_actions;", "yf_stock_actions: Distinct action_type", logger))

    return results

def check_ticker_consistency(con: duckdb.DuckDBPyConnection, logger: logging.Logger) -> List[Tuple[Optional[str], Optional[int], str]]:
    """Checks if tickers in stock/yf tables exist in the main 'tickers' table."""
    results = []
    db_tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    logger.info("\n=== Checking Ticker Consistency Across Tables (Expecting 0 violations) ===")
    yf_tables_with_ticker = [
        "stock_history", # From stock_data_gatherer
        "yf_profile_metrics", "yf_stock_actions", "yf_major_holders", "yf_recommendations"
    ]
    for table in yf_tables_with_ticker:
        if table.lower() in db_tables:
            query = f"""
                SELECT COUNT(t1.ticker) as count
                FROM {table} t1
                LEFT JOIN tickers t2 ON t1.ticker = t2.ticker
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

# --- Main Orchestration Function ---
def run_all_checks(con: duckdb.DuckDBPyConnection, logger: logging.Logger):
    """Runs all defined validation checks."""
    all_results = []
    all_results.extend(check_table_counts(con, logger))
    all_results.extend(check_edgar_fk_integrity(con, logger))
    all_results.extend(check_orphaned_facts(con, logger))
    all_results.extend(check_yf_data_validations(con, logger)) # Add new YF checks
    all_results.extend(check_table_uniqueness(con, logger))
    all_results.extend(check_market_risk_validations(con, logger))
    # all_results.extend(check_ticker_consistency(con, logger)) # Add new consistency check

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

    # Check 'companies' table for duplicate CIKs
    results.append(run_validation_query(con, """
        SELECT COUNT(*) AS count FROM (
            SELECT cik, COUNT(*) FROM companies GROUP BY cik HAVING COUNT(*) > 1
        );""", "Companies: Duplicate CIKs", logger, expect_zero=True))

    # Check 'tickers' table for duplicate ticker/exchange combinations (consider each unique)
    results.append(run_validation_query(con, """
        SELECT COUNT(*) AS count FROM (
            SELECT ticker, exchange, COUNT(*) FROM tickers GROUP BY ticker, exchange HAVING COUNT(*) > 1
        );""", "Tickers: Duplicate Ticker/Exchange", logger, expect_zero=True))

    # Check 'filings' table for duplicate accession numbers
    results.append(run_validation_query(con, """
        SELECT COUNT(*) AS count FROM (
            SELECT accession_number, COUNT(*) FROM filings GROUP BY accession_number HAVING COUNT(*) > 1
        );""", "Filings: Duplicate Accession Numbers", logger, expect_zero=True))

    # Check 'xbrl_tags' table for duplicate tag/taxonomy combinations (unlikely but good to validate)
    results.append(run_validation_query(con, """
        SELECT COUNT(*) AS count FROM (
            SELECT taxonomy, tag_name, COUNT(*) FROM xbrl_tags GROUP BY taxonomy, tag_name HAVING COUNT(*) > 1
        );""", "XBRL Tags: Duplicate Taxonomy/Tag", logger, expect_zero=True))

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