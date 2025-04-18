# -*- coding: utf-8 -*-
"""
EDGAR Database Validation Script (e.g., save as validate_edgar_db.py)

Connects to the populated DuckDB database and runs various checks
to validate data integrity, types, and relationships.
"""

import duckdb
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

# --- Configuration from .env ---
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'
if not dotenv_path.is_file(): sys.exit(f"ERROR: .env file not found at {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

try:
    DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError as e: sys.exit(f"ERROR: Missing required .env variable: {e}")

# --- Logging Setup ---
log_file_path = script_dir / "validate_edgar_db.log"
log_file_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def run_validation_query(con, query: str, description: str):
    """Runs a validation query and logs the result."""
    logging.info(f"--- Running Check: {description} ---")
    try:
        result = con.sql(query).df()
        logging.info(f"Result:\n{result.to_string(index=False)}")
        # Add specific checks based on expected results if needed
        if "count" in result.columns and result['count'].iloc[0] != 0 and "Orphan Check" not in description and "FK Violation Check" in description :
             logging.warning(f"Potential FK Violation detected for: {description}")

    except Exception as e:
        logging.error(f"FAILED Check '{description}' with error: {e}", exc_info=True)

# --- Main Validation Logic ---
if __name__ == "__main__":
    logging.info(f"--- Starting Database Validation for: {DB_FILE} ---")

    if not DB_FILE.exists():
        logging.error(f"Database file not found: {DB_FILE}")
        sys.exit(1)

    db_conn = None
    try:
        db_conn = duckdb.connect(database=str(DB_FILE), read_only=True)
        logging.info("Database connection successful.")

        # --- Basic Row Counts ---
        tables = ["companies", "tickers", "former_names", "filings", "xbrl_tags", "xbrl_facts", "xbrl_facts_orphaned"]
        for table in tables:
            run_validation_query(db_conn, f"SELECT COUNT(*) as count FROM {table};", f"Row count for {table}")

        # --- Distinct Value Spot Checks (VARCHAR columns) ---
        varchar_checks = {
            "companies": ["entity_type", "state_of_incorporation", "fiscal_year_end"],
            "tickers": ["exchange", "source"],
            "filings": ["form", "act"],
            "xbrl_tags": ["taxonomy"],
            "xbrl_facts": ["unit", "taxonomy", "form", "fp"],
            "xbrl_facts_orphaned": ["unit", "taxonomy", "form", "fp"]
        }
        for table, columns in varchar_checks.items():
            for col in columns:
                 run_validation_query(db_conn, f"SELECT DISTINCT \"{col}\" FROM {table} WHERE \"{col}\" IS NOT NULL LIMIT 10;", f"Distinct values for {table}.{col}")

        # --- Date Range Checks ---
        date_checks = {
             "former_names": ["date_from", "date_to"],
             "filings": ["filing_date", "report_date", "acceptance_datetime"],
             "xbrl_facts": ["period_start_date", "period_end_date", "filed_date"],
             "xbrl_facts_orphaned": ["period_start_date", "period_end_date", "filed_date"]
        }
        for table, columns in date_checks.items():
             for col in columns:
                  run_validation_query(db_conn, f"SELECT MIN(\"{col}\") as min_date, MAX(\"{col}\") as max_date FROM {table};", f"Date range for {table}.{col}")

        # --- Foreign Key Integrity Checks ---
        logging.info("\n--- Checking Foreign Key Integrity (Expecting 0 violations for non-orphan tables) ---")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM tickers t LEFT JOIN companies c ON t.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: tickers -> companies")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM former_names fn LEFT JOIN companies c ON fn.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: former_names -> companies")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM filings f LEFT JOIN companies c ON f.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: filings -> companies")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN companies c ON f.cik = c.cik WHERE c.cik IS NULL;", "FK Violation Check: xbrl_facts -> companies")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN filings fi ON f.accession_number = fi.accession_number WHERE fi.accession_number IS NULL;", "FK Violation Check: xbrl_facts -> filings")
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM xbrl_facts f LEFT JOIN xbrl_tags t ON f.taxonomy = t.taxonomy AND f.tag_name = t.tag_name WHERE t.taxonomy IS NULL;", "FK Violation Check: xbrl_facts -> xbrl_tags")

        # --- Orphan Table Check ---
        run_validation_query(db_conn, "SELECT COUNT(*) as count FROM xbrl_facts_orphaned;", "Orphan Check: Total orphaned facts")
        run_validation_query(db_conn, "SELECT DISTINCT cik, accession_number FROM xbrl_facts_orphaned LIMIT 10;", "Orphan Check: Sample orphaned CIKs/Accession Nums")

        # --- Check Specific Problem Columns Again ---
        run_validation_query(db_conn, "SELECT DISTINCT unit FROM xbrl_facts LIMIT 10;", "Data Type Check: xbrl_facts.unit")
        run_validation_query(db_conn, "SELECT DISTINCT taxonomy FROM xbrl_facts LIMIT 10;", "Data Type Check: xbrl_facts.taxonomy")
        run_validation_query(db_conn, "SELECT DISTINCT unit FROM xbrl_facts_orphaned LIMIT 10;", "Data Type Check: xbrl_facts_orphaned.unit")
        run_validation_query(db_conn, "SELECT DISTINCT taxonomy FROM xbrl_facts_orphaned LIMIT 10;", "Data Type Check: xbrl_facts_orphaned.taxonomy")


        logging.info("\n--- Validation Checks Complete ---")

    except Exception as e:
        logging.error(f"Failed to connect to or query database: {e}", exc_info=True)
    finally:
        if db_conn:
            try:
                db_conn.close()
                logging.debug("Validation DB connection closed.")
            except Exception as e_close:
                logging.error(f"Error closing validation DB connection: {e_close}")