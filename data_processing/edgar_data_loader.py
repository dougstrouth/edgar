# -*- coding: utf-8 -*-
"""
EDGAR Parquet to Database Loader

Loads structured data from the intermediate Parquet format into a DuckDB
database. This is the second, high-speed stage of the loading process.

Uses:
- config_utils.AppConfig for loading configuration from .env.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.
"""

import os
import sys
import logging # Keep import for level constants (e.g., logging.INFO)
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, Tuple
from datetime import datetime, date, timezone

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import duckdb # Still needed for duckdb.Error and potentially type hints
from tqdm import tqdm

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig                 # Import configuration loader
from utils.logging_utils import setup_logging            # Import logging setup function
from utils.database_conn import ManagedDatabaseConnection # Import DB context manager

# --- Database Schema (Remains the same) ---
# --- Database Schema (Updated with more robust PKs for efficient loading) ---
# ... (SCHEMA dictionary remains unchanged)
SCHEMA = {
    "companies": """
        CREATE TABLE IF NOT EXISTS companies (
            cik                         VARCHAR(10) PRIMARY KEY, primary_name VARCHAR, entity_name_cf VARCHAR,
            entity_type                 VARCHAR, sic VARCHAR, sic_description VARCHAR, ein VARCHAR, description VARCHAR,
            category                    VARCHAR, fiscal_year_end VARCHAR(4), state_of_incorporation VARCHAR,
            phone                       VARCHAR, flags VARCHAR,
            mailing_street1             VARCHAR, mailing_street2 VARCHAR, mailing_city VARCHAR, mailing_state_or_country VARCHAR, mailing_zip_code VARCHAR,
            business_street1            VARCHAR, business_street2 VARCHAR, business_city VARCHAR, business_state_or_country VARCHAR, business_zip_code VARCHAR,
            first_added_timestamp       TIMESTAMPTZ DEFAULT now(), last_parsed_timestamp TIMESTAMPTZ
        );""",
    "tickers": """
        CREATE TABLE IF NOT EXISTS tickers (
            cik VARCHAR(10) NOT NULL, ticker VARCHAR NOT NULL COLLATE NOCASE, exchange VARCHAR NOT NULL COLLATE NOCASE, source VARCHAR
            -- PK handled by INSERT OR REPLACE logic, FKs slow down bulk loads
        );""",
    "former_names": """
        CREATE TABLE IF NOT EXISTS former_names (
            cik VARCHAR(10) NOT NULL, former_name VARCHAR NOT NULL, date_from TIMESTAMPTZ NOT NULL, date_to TIMESTAMPTZ,
            PRIMARY KEY (cik, former_name, date_from)
        );""",
    "filings": """
        CREATE TABLE IF NOT EXISTS filings (
            accession_number            VARCHAR PRIMARY KEY, cik VARCHAR(10) NOT NULL, filing_date DATE, report_date DATE,
            acceptance_datetime         TIMESTAMPTZ, act VARCHAR, form VARCHAR NOT NULL, file_number VARCHAR, film_number VARCHAR,
            items                       VARCHAR, size BIGINT, is_xbrl BOOLEAN, is_inline_xbrl BOOLEAN, primary_document VARCHAR,
            primary_doc_description VARCHAR
        );""",
    "xbrl_tags": """
        CREATE TABLE IF NOT EXISTS xbrl_tags (
            taxonomy                    VARCHAR NOT NULL COLLATE NOCASE,
            tag_name                    VARCHAR NOT NULL,
            label                       VARCHAR,
            description                 VARCHAR,
            PRIMARY KEY (taxonomy, tag_name) -- Composite Primary Key
        );""",
    "xbrl_facts": """
        CREATE TABLE IF NOT EXISTS xbrl_facts (
            cik VARCHAR(10) NOT NULL, accession_number VARCHAR NOT NULL, taxonomy VARCHAR NOT NULL COLLATE NOCASE,
            tag_name VARCHAR NOT NULL, unit VARCHAR NOT NULL COLLATE NOCASE, period_end_date DATE,
            value_numeric DOUBLE, value_text VARCHAR, fy INTEGER, fp VARCHAR, form VARCHAR NOT NULL, filed_date DATE, frame VARCHAR,
            -- Define a composite primary key for accurate replacement
            PRIMARY KEY (cik, accession_number, taxonomy, tag_name, unit, period_end_date, frame)
        );""",
    "xbrl_facts_orphaned": """
        CREATE TABLE IF NOT EXISTS xbrl_facts_orphaned (
            cik VARCHAR(10) NOT NULL, accession_number VARCHAR NOT NULL, taxonomy VARCHAR NOT NULL COLLATE NOCASE,
            tag_name VARCHAR NOT NULL, unit VARCHAR NOT NULL COLLATE NOCASE, period_end_date DATE,
            value_numeric DOUBLE, value_text VARCHAR, fy INTEGER, fp VARCHAR, form VARCHAR NOT NULL, filed_date DATE, frame VARCHAR,
            -- Note: No FK to filings here
            PRIMARY KEY (cik, accession_number, taxonomy, tag_name, unit, period_end_date, frame)
        );""",
    "indexes": [
        "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies (primary_name);",
        "CREATE INDEX IF NOT EXISTS idx_tickers_ticker ON tickers (ticker);",
        "CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings (cik);",
        "CREATE INDEX IF NOT EXISTS idx_filings_form ON filings (form);",
        "CREATE INDEX IF NOT EXISTS idx_filings_date ON filings (filing_date);"
    ]
}

def load_parquet_to_db(config: AppConfig, logger: logging.Logger):
    """Loads data from Parquet files into the DuckDB database."""
    # Define PRAGMA settings for write-heavy operations
    write_pragmas = {
        'threads': os.cpu_count(),
        'memory_limit': config.DUCKDB_MEMORY_LIMIT
    }
    if config.DUCKDB_TEMP_DIR:
        config.DUCKDB_TEMP_DIR.mkdir(exist_ok=True)
        write_pragmas['temp_directory'] = str(config.DUCKDB_TEMP_DIR)
        logger.info(f"Using temporary directory for DuckDB: {config.DUCKDB_TEMP_DIR}")

    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False, pragma_settings=write_pragmas) as db_conn:
            if db_conn is None:
                raise ConnectionError(f"Failed to establish database connection to {config.DB_FILE_STR}")

            # --- Create Base Tables (if they don't exist at all) ---
            logger.info("Creating tables if they don't exist...")
            for table_name, create_sql in SCHEMA.items():
                if table_name != "indexes":
                    db_conn.execute(create_sql)
            logger.info("Base tables created or already exist.")

            # --- Blue-Green Deployment: Create NEW tables from Parquet ---
            logger.info("--- Starting Blue-Green Load: Creating new tables from Parquet ---")

            # Load main tables
            main_tables = ["companies", "tickers", "former_names", "filings", "xbrl_tags"]
            for table in main_tables:
                table_new = f"{table}_new"
                parquet_path = config.PARQUET_DIR / table
                if parquet_path.exists() and any(parquet_path.iterdir()):
                    logger.info(f"Creating new table '{table_new}' from Parquet files...")
                    if table == "filings":
                        # Special handling for filings to ensure accession_number is unique
                        db_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER(PARTITION BY accession_number ORDER BY filing_date DESC) as rn FROM read_parquet('{parquet_path}/*.parquet')
                            ) WHERE rn = 1;""")
                    elif table == "companies":
                        # Special handling for companies to ensure cik is unique
                        db_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER(PARTITION BY cik) as rn FROM read_parquet('{parquet_path}/*.parquet')
                            ) WHERE rn = 1;""")
                    elif table == "tickers":
                        # Special handling for tickers to ensure ticker, exchange is unique
                        db_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker, exchange) as rn FROM read_parquet('{parquet_path}/*.parquet')
                            ) WHERE rn = 1;""")
                    elif table == "xbrl_tags":
                        # Special handling for xbrl_tags to ensure taxonomy, tag_name is unique
                        db_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER(PARTITION BY taxonomy, tag_name) as rn FROM read_parquet('{parquet_path}/*.parquet')
                            ) WHERE rn = 1;""")
                    else:
                        db_conn.execute(f"CREATE OR REPLACE TABLE {table_new} AS SELECT * FROM read_parquet('{parquet_path}/*.parquet');")
                else:
                    logger.warning(f"Parquet directory for '{table}' not found or empty. Creating empty new table.")
                    db_conn.execute(SCHEMA[table].replace(f"CREATE TABLE IF NOT EXISTS {table}", f"CREATE OR REPLACE TABLE {table_new}"))

            # --- Handle XBRL Facts (Valid vs Orphaned) ---
            facts_parquet_path = config.PARQUET_DIR / "xbrl_facts"
            if facts_parquet_path.exists() and any(facts_parquet_path.iterdir()):
                logger.info("Processing and loading xbrl_facts...")
                # Create the new valid facts table by joining with the new filings table
                logger.info("Creating new table 'xbrl_facts_new'...")
                db_conn.execute("""
                    CREATE OR REPLACE TABLE xbrl_facts_new AS
                    SELECT p.* FROM read_parquet('{0}/*.parquet') p
                    JOIN filings_new f ON p.accession_number = f.accession_number
                    JOIN companies_new c ON p.cik = c.cik
                    ORDER BY p.filed_date;
                """.format(facts_parquet_path))

                # Create the new orphaned facts table
                logger.info("Creating new table 'xbrl_facts_orphaned_new'...")
                db_conn.execute("""
                    CREATE OR REPLACE TABLE xbrl_facts_orphaned_new AS
                    SELECT p.* FROM read_parquet('{0}/*.parquet') p LEFT JOIN (
                        SELECT f.accession_number, c.cik FROM filings_new f JOIN companies_new c ON f.cik = c.cik
                    ) AS valid_filings ON p.accession_number = valid_filings.accession_number AND p.cik = valid_filings.cik
                    WHERE valid_filings.accession_number IS NULL
                    ORDER BY p.filed_date;
                """.format(facts_parquet_path))
            else:
                logger.warning("Parquet directory for 'xbrl_facts' not found. Creating empty new fact tables.")
                db_conn.execute(SCHEMA["xbrl_facts"].replace("CREATE TABLE IF NOT EXISTS xbrl_facts", "CREATE OR REPLACE TABLE xbrl_facts_new"))
                db_conn.execute(SCHEMA["xbrl_facts_orphaned"].replace("CREATE TABLE IF NOT EXISTS xbrl_facts_orphaned", "CREATE OR REPLACE TABLE xbrl_facts_orphaned_new"))

            # --- Atomic Swap ---
            logger.info("--- Starting Atomic Table Swap ---")
            db_conn.begin()
            try:
                all_load_tables = main_tables + ["xbrl_facts", "xbrl_facts_orphaned"]
                for table in all_load_tables:
                    logger.info(f"Swapping table: {table}")
                    db_conn.execute(f"DROP TABLE IF EXISTS {table};")
                    db_conn.execute(f"ALTER TABLE {table}_new RENAME TO {table};")
                db_conn.commit()
                logger.info("--- Atomic Table Swap Complete ---")
            except Exception as swap_e:
                logger.error("--- ATOMIC SWAP FAILED! Rolling back. Database is in its previous state. ---", exc_info=True)
                db_conn.rollback()
                raise swap_e

            # --- Create Indexes on the new tables (one transaction per index) ---
            logger.info("Creating indexes (may take time, one transaction per index)...")
            for index_sql in SCHEMA["indexes"]:
                try:
                    logger.info(f"Beginning transaction for index: {index_sql}")
                    db_conn.begin()
                    db_conn.execute(index_sql)
                    db_conn.commit()
                    logger.info(f"Successfully created index: {index_sql}")
                except Exception as index_e:
                    logger.error(f"Failed to create index: {index_sql}. Rolling back. Error: {index_e}")
                    try:
                        db_conn.rollback()
                    except Exception as rb_e:
                        logger.error(f"Failed to rollback transaction for index creation: {rb_e}")
            logger.info("Index creation process finished.")

    except ConnectionError as e:
        logger.critical(f"Database Connection Error: {e}. Cannot continue load.")
        raise # Re-raise to stop processing
    except Exception as e:
        logger.error(f"Critical error during Parquet load process: {e}", exc_info=True)
        raise # Re-raise to stop processing

if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    logger.info(f"--- Starting EDGAR Data Loader Script ---")
    logger.info(f"Using DB_FILE: {config.DB_FILE}")
    logger.info(f"Parquet source directory: {config.PARQUET_DIR}")

    load_parquet_to_db(config, logger)

    # --- Final Script Message ---
    logger.info(f"--- EDGAR Data Loader Script Finished ---")