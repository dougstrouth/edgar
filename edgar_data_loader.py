# -*- coding: utf-8 -*-
"""
EDGAR Data Loader Script (Refactored for Utilities)

Orchestrates the parsing of extracted SEC EDGAR JSON files and loads
the structured data into a DuckDB database using batch processing.

Uses:
- config_utils.AppConfig for loading configuration from .env.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.
- json_parse for parsing logic.

Handles orphaned facts, reprocessing deletions, and data type consistency.
Control toggles are now primarily read from environment variables via AppConfig.
"""

import json
import os
import sys
import logging # Keep import for level constants (e.g., logging.INFO)
import math
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, Tuple
from datetime import datetime, date, timezone

import pandas as pd
import duckdb # Still needed for duckdb.Error and potentially type hints
# from dotenv import load_dotenv # No longer needed here
from tqdm import tqdm

# --- Import Utilities ---
from config_utils import AppConfig                 # Import configuration loader
from logging_utils import setup_logging            # Import logging setup function
from database_conn import ManagedDatabaseConnection # Import DB context manager
try:
    import json_parse
    # Assume functions needed are accessed via json_parse.*
except ImportError:
    print("ERROR: Could not import 'json_parse.py'. Make sure it's in the same directory.", file=sys.stderr)
    sys.exit(1)
except AttributeError as e:
    print(f"ERROR: Missing required function in 'json_parse.py': {e}", file=sys.stderr)
    sys.exit(1)

# --- Constants (can be moved to config or kept here if truly static) ---
# Example: SCRIPT_NAME = Path(__file__).stem (defined in __main__)

# --- Database Schema (Remains the same) ---
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
            cik VARCHAR(10) NOT NULL, ticker VARCHAR NOT NULL COLLATE NOCASE, exchange VARCHAR NOT NULL COLLATE NOCASE, source VARCHAR,
            PRIMARY KEY (cik, ticker, exchange), FOREIGN KEY (cik) REFERENCES companies(cik)
        );""",
    "former_names": """
        CREATE TABLE IF NOT EXISTS former_names (
            cik VARCHAR(10) NOT NULL, former_name VARCHAR NOT NULL, date_from TIMESTAMPTZ, date_to TIMESTAMPTZ,
            PRIMARY KEY (cik, former_name, date_from), FOREIGN KEY (cik) REFERENCES companies(cik)
        );""",
    "filings": """
        CREATE TABLE IF NOT EXISTS filings (
            accession_number            VARCHAR PRIMARY KEY, cik VARCHAR(10) NOT NULL, filing_date DATE, report_date DATE,
            acceptance_datetime         TIMESTAMPTZ, act VARCHAR, form VARCHAR NOT NULL, file_number VARCHAR, film_number VARCHAR,
            items                       VARCHAR, size BIGINT, is_xbrl BOOLEAN, is_inline_xbrl BOOLEAN,
            primary_document            VARCHAR, primary_doc_description VARCHAR, FOREIGN KEY (cik) REFERENCES companies(cik)
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
            tag_name VARCHAR NOT NULL, unit VARCHAR NOT NULL COLLATE NOCASE, period_start_date DATE, period_end_date DATE,
            value_numeric DOUBLE, value_text VARCHAR, fy INTEGER, fp VARCHAR, form VARCHAR NOT NULL, filed_date DATE, frame VARCHAR,
            FOREIGN KEY (cik) REFERENCES companies(cik),
            FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
            FOREIGN KEY (taxonomy, tag_name) REFERENCES xbrl_tags(taxonomy, tag_name)
        );""",
    "xbrl_facts_orphaned": """
        CREATE TABLE IF NOT EXISTS xbrl_facts_orphaned (
            cik VARCHAR(10) NOT NULL, accession_number VARCHAR NOT NULL, taxonomy VARCHAR NOT NULL COLLATE NOCASE,
            tag_name VARCHAR NOT NULL, unit VARCHAR NOT NULL COLLATE NOCASE, period_start_date DATE, period_end_date DATE,
            value_numeric DOUBLE, value_text VARCHAR, fy INTEGER, fp VARCHAR, form VARCHAR NOT NULL, filed_date DATE, frame VARCHAR
            -- Note: No FK to filings here
        );""",
    "indexes": [
        "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies (primary_name);",
        "CREATE INDEX IF NOT EXISTS idx_tickers_ticker ON tickers (ticker);",
        "CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings (cik);",
        "CREATE INDEX IF NOT EXISTS idx_filings_form ON filings (form);",
        "CREATE INDEX IF NOT EXISTS idx_filings_date ON filings (filing_date);",
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_cik_tag ON xbrl_facts (cik, taxonomy, tag_name);",
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_tag_date ON xbrl_facts (taxonomy, tag_name, period_end_date);",
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_accn ON xbrl_facts (accession_number);",
        "CREATE INDEX IF NOT EXISTS idx_orphan_facts_cik_accn ON xbrl_facts_orphaned (cik, accession_number);",
        "CREATE INDEX IF NOT EXISTS idx_orphan_facts_tag ON xbrl_facts_orphaned (taxonomy, tag_name);"
    ]
}

# --- Helper Function for DataFrame Preparation (Remains the same) ---
def _prepare_df_for_load(df: pd.DataFrame, str_cols: List[str], date_cols: List[str] = None, numeric_cols: List[str] = None, int_cols: List[str] = None) -> pd.DataFrame:
    """Ensure specified columns are strings, dates, numeric, or integers, handling errors."""
    df_out = df.copy()
    if str_cols:
        for col in str_cols:
            if col in df_out.columns:
                df_out[col] = df_out[col].apply(lambda x: str(x) if pd.notna(x) else None)
    if date_cols:
        for col in date_cols:
            if col in df_out.columns:
                df_out[col] = pd.to_datetime(df_out[col], errors='coerce').dt.date
    if numeric_cols:
        for col in numeric_cols:
             if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
    if int_cols:
         for col in int_cols:
             if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce').astype('Int64')
    return df_out


# --- Database Loading Function (Updated to use logger instance and AppConfig object) ---
def load_batch_to_db(
    batch_data: Dict[str, List[Dict]],
    config: AppConfig,           # Pass the AppConfig object
    logger: logging.Logger,      # Pass the logger instance
    is_first_batch: bool,
    is_last_batch: bool
):
    """
    Uses ManagedDatabaseConnection to load the provided batch of aggregated data.
    Handles table creation, pre-deletions, data loading, and index creation within transactions.
    Uses provided AppConfig for paths/toggles and logger for logging.
    """
    logger.info(f"Starting DB load for batch. First batch: {is_first_batch}, Last batch: {is_last_batch}")

    try:
        # Use the context manager with DB path from config
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as db_conn:
            if db_conn is None:
                raise ConnectionError(f"Failed to establish database connection to {config.DB_FILE_STR}")

            # Get skip toggles from config
            skip_table_creation = config.get_optional_bool("SKIP_TABLE_CREATION", False)
            skip_data_loading = config.get_optional_bool("SKIP_DATA_LOADING", False)
            skip_index_creation = config.get_optional_bool("SKIP_INDEX_CREATION", False)

            # --- Create Tables ---
            if is_first_batch and not skip_table_creation:
                logger.info("Creating tables if they don't exist...")
                try:
                    for table_name, create_sql in SCHEMA.items():
                        if table_name != "indexes":
                            logger.debug(f"Executing CREATE for table: {table_name}")
                            db_conn.execute(create_sql)
                    logger.info("Base tables created or already exist.")
                except duckdb.Error as e:
                    logger.error(f"DuckDB Error during table creation: {e}", exc_info=True)
                    raise # Context manager handles rollback
                except Exception as e:
                    logger.error(f"Unexpected error during table creation: {e}", exc_info=True)
                    raise
            elif is_first_batch and skip_table_creation:
                logger.warning("Skipping Table Creation based on config.")

            # --- Load Data ---
            if not skip_data_loading:
                logger.info("Loading batch data into tables...")
                try:
                    # Pre-delete logic (unchanged conceptually)
                    ciks_in_batch_set: Set[str] = set()
                    if batch_data.get("companies"):
                        ciks_in_batch_set = {str(comp.get("cik")) for comp in batch_data["companies"] if comp.get("cik")}

                    if ciks_in_batch_set:
                        ciks_list = list(ciks_in_batch_set)
                        placeholder = ','.join('?' * len(ciks_list))
                        logger.info(f"Pre-deleting dependent records for {len(ciks_list)} CIKs in batch...")
                        try:
                            db_conn.execute(f"DELETE FROM xbrl_facts WHERE cik IN ({placeholder})", ciks_list)
                            db_conn.execute(f"DELETE FROM xbrl_facts_orphaned WHERE cik IN ({placeholder})", ciks_list)
                            db_conn.execute(f"DELETE FROM filings WHERE cik IN ({placeholder})", ciks_list)
                            db_conn.execute(f"DELETE FROM tickers WHERE cik IN ({placeholder})", ciks_list)
                            db_conn.execute(f"DELETE FROM former_names WHERE cik IN ({placeholder})", ciks_list)
                            logger.info(f"Pre-deletion complete for CIKs: {', '.join(ciks_list[:5])}...")
                        except duckdb.Error as e_del:
                            logger.error(f"DuckDB Error during pre-emptive deletion: {e_del}", exc_info=True)
                            raise e_del

                    # Load Companies (unchanged conceptually, uses _prepare_df_for_load)
                    if batch_data.get("companies"):
                        df_companies = pd.DataFrame(batch_data["companies"])
                        if not df_companies.empty:
                            logger.info(f"Preparing {len(df_companies)} company records...")
                            str_cols_comp = ['cik', 'primary_name', 'entity_name_cf', 'entity_type', 'sic', 'sic_description', 'ein', 'description', 'category', 'fiscal_year_end', 'state_of_incorporation', 'phone', 'flags', 'mailing_street1', 'mailing_street2', 'mailing_city', 'mailing_state_or_country', 'mailing_zip_code', 'business_street1', 'business_street2', 'business_city', 'business_state_or_country', 'business_zip_code']
                            date_cols_comp = ['last_parsed_timestamp']
                            df_companies = _prepare_df_for_load(df_companies, str_cols=str_cols_comp, date_cols=date_cols_comp)
                            logger.info(f"Loading {len(df_companies)} company records...")
                            db_conn.register('df_companies_reg', df_companies)
                            df_cols_to_load = [f'"{col}"' for col in df_companies.columns]
                            col_str = ", ".join(df_cols_to_load)
                            update_cols = [col for col in df_cols_to_load if col not in ('"cik"', '"first_added_timestamp"')]
                            update_setters = ", ".join([f'{col} = excluded.{col}' for col in update_cols])
                            if update_setters: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO UPDATE SET {update_setters}"""
                            else: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO NOTHING"""
                            db_conn.execute(sql)
                            db_conn.unregister('df_companies_reg')
                            logger.debug("Company batch loaded/updated.")

                    # Load Tickers (unchanged conceptually)
                    if batch_data.get("tickers"):
                        unique_tickers = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['tickers'] if d.get('cik') and d.get('ticker') and d.get('exchange')}]
                        if unique_tickers:
                            logger.info(f"Preparing {len(unique_tickers)} unique ticker records...")
                            df_tickers = pd.DataFrame(unique_tickers)
                            str_cols_tick = ['cik', 'ticker', 'exchange', 'source']
                            df_tickers = _prepare_df_for_load(df_tickers, str_cols=str_cols_tick)
                            ticker_data_tuples = [tuple(x) for x in df_tickers[['cik', 'ticker', 'exchange', 'source']].to_numpy()]
                            logger.info(f"Loading {len(ticker_data_tuples)} ticker records...")
                            db_conn.executemany("INSERT INTO tickers (cik, ticker, exchange, source) VALUES (?, ?, ?, ?) ON CONFLICT (cik, ticker, exchange) DO NOTHING", ticker_data_tuples)
                            logger.debug("Ticker batch loaded.")

                    # Load Former Names (unchanged conceptually)
                    if batch_data.get("former_names"):
                        unique_fns = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['former_names'] if d.get('cik') and d.get('former_name') and d.get('date_from')}]
                        if unique_fns:
                            logger.info(f"Preparing {len(unique_fns)} unique former name records...")
                            df_fns = pd.DataFrame(unique_fns)
                            str_cols_fn = ['cik', 'former_name']
                            date_cols_fn = ['date_from', 'date_to']
                            df_fns = _prepare_df_for_load(df_fns, str_cols=str_cols_fn, date_cols=date_cols_fn)
                            fn_data_tuples = [tuple(x) for x in df_fns[['cik', 'former_name', 'date_from', 'date_to']].to_numpy()]
                            logger.info(f"Loading {len(fn_data_tuples)} former name records...")
                            db_conn.executemany("INSERT INTO former_names (cik, former_name, date_from, date_to) VALUES (?, ?, ?, ?) ON CONFLICT (cik, former_name, date_from) DO NOTHING", fn_data_tuples)
                            logger.debug("Former name batch loaded.")

                    # Load Filings (unchanged conceptually)
                    valid_accession_numbers_for_batch = set()
                    if batch_data.get("filings"):
                        unique_filings = {d['accession_number']: d for d in batch_data['filings'] if d.get('accession_number')}.values()
                        if unique_filings:
                            logger.info(f"Preparing {len(unique_filings)} unique filing records...")
                            df_filings = pd.DataFrame(list(unique_filings))
                            str_cols_fil = ['cik', 'accession_number', 'act', 'form', 'file_number', 'film_number', 'items', 'primary_document', 'primary_doc_description']
                            date_cols_fil = ['filing_date', 'report_date']
                            dt_cols_fil = ['acceptance_datetime']
                            int_cols_fil = ['size']
                            df_filings = _prepare_df_for_load(df_filings, str_cols=str_cols_fil, date_cols=date_cols_fil+dt_cols_fil, int_cols=int_cols_fil)
                            filing_cols = ['cik', 'accession_number', 'filing_date', 'report_date', 'acceptance_datetime', 'act', 'form', 'file_number', 'film_number', 'items', 'size', 'is_xbrl', 'is_inline_xbrl', 'primary_document', 'primary_doc_description']
                            for col in filing_cols:
                                if col not in df_filings.columns: df_filings[col] = pd.NA
                            filing_data_tuples = [tuple(x) for x in df_filings[filing_cols].to_numpy()]
                            logger.info(f"Loading {len(filing_data_tuples)} filing records...")
                            placeholders = ", ".join(["?"] * len(filing_cols))
                            db_conn.executemany(f"INSERT INTO filings ({', '.join(filing_cols)}) VALUES ({placeholders}) ON CONFLICT (accession_number) DO NOTHING", filing_data_tuples)
                            valid_accession_numbers_for_batch = {str(tpl[1]) for tpl in filing_data_tuples if tpl[1]}
                            logger.debug("Filing batch loaded.")

                    # Load XBRL Tags (unchanged conceptually)
                    if batch_data.get("xbrl_tags"):
                        unique_tag_data = {}
                        for tag_dict in batch_data.get("xbrl_tags", []):
                            key = (tag_dict.get("taxonomy"), tag_dict.get("tag_name"))
                            if all(key):
                                if key not in unique_tag_data: unique_tag_data[key] = tag_dict
                        unique_tags_list = list(unique_tag_data.values())
                        if unique_tags_list:
                            logger.info(f"Preparing {len(unique_tags_list)} unique tag definitions...")
                            df_tags = pd.DataFrame(unique_tags_list)
                            str_cols_tags = ['taxonomy', 'tag_name', 'label', 'description']
                            df_tags = _prepare_df_for_load(df_tags, str_cols=str_cols_tags)
                            logger.info(f"Loading {len(df_tags)} tag records...")
                            db_conn.register('df_tags_reg', df_tags)
                            tag_cols = ['taxonomy', 'tag_name', 'label', 'description']
                            col_str = ", ".join([f'"{c}"' for c in tag_cols])
                            sql = f"""INSERT INTO xbrl_tags ({col_str}) SELECT {col_str} FROM df_tags_reg ON CONFLICT (taxonomy, tag_name) DO NOTHING"""
                            db_conn.execute(sql)
                            db_conn.unregister('df_tags_reg')
                            logger.debug("XBRL Tag batch loaded.")

                    # Load XBRL Facts (unchanged conceptually)
                    if batch_data.get("xbrl_facts"):
                        logger.info(f"Preparing {len(batch_data['xbrl_facts'])} potential fact records...")
                        df_facts_all = pd.DataFrame(batch_data["xbrl_facts"])
                        if not df_facts_all.empty:
                            if 'accession_number' in df_facts_all.columns:
                                df_facts_all['accession_number'] = df_facts_all['accession_number'].astype(str)
                            if not valid_accession_numbers_for_batch:
                                logger.warning("No valid filings found/loaded in this batch. All facts will be treated as orphaned.")
                                df_facts_valid = pd.DataFrame()
                                df_facts_orphaned = df_facts_all.copy()
                            else:
                                is_valid = df_facts_all['accession_number'].isin(valid_accession_numbers_for_batch)
                                df_facts_valid = df_facts_all[is_valid].copy()
                                df_facts_orphaned = df_facts_all[~is_valid].copy()

                            facts_str_cols = ['cik', 'accession_number', 'taxonomy', 'tag_name', 'unit', 'value_text', 'fp', 'form', 'frame']
                            facts_date_cols = ['period_start_date', 'period_end_date', 'filed_date']
                            facts_numeric_cols = ['value_numeric']
                            facts_int_cols = ['fy']

                            if not df_facts_valid.empty:
                                logger.info(f"Processing {len(df_facts_valid)} valid facts for xbrl_facts table...")
                                df_facts_valid = _prepare_df_for_load(df_facts_valid, str_cols=facts_str_cols, date_cols=facts_date_cols, numeric_cols=facts_numeric_cols, int_cols=facts_int_cols)
                                db_conn.register('df_facts_valid_reg', df_facts_valid)
                                df_cols_to_load = [f'"{col}"' for col in df_facts_valid.columns]
                                cols_str = ", ".join(df_cols_to_load)
                                if cols_str:
                                    db_conn.execute(f"INSERT INTO xbrl_facts ({cols_str}) SELECT {cols_str} FROM df_facts_valid_reg")
                                db_conn.unregister('df_facts_valid_reg')
                                logger.debug("Valid XBRL Fact batch loaded into xbrl_facts.")
                            else: logger.info("No valid facts found to load into xbrl_facts table for this batch.")

                            if not df_facts_orphaned.empty:
                                logger.warning(f"Processing {len(df_facts_orphaned)} orphaned facts for xbrl_facts_orphaned table...")
                                df_facts_orphaned = _prepare_df_for_load(df_facts_orphaned, str_cols=facts_str_cols, date_cols=facts_date_cols, numeric_cols=facts_numeric_cols, int_cols=facts_int_cols)
                                db_conn.register('df_facts_orphaned_reg', df_facts_orphaned)
                                df_cols_to_load_orphan = [f'"{col}"' for col in df_facts_orphaned.columns]
                                cols_str_orphan = ", ".join(df_cols_to_load_orphan)
                                if cols_str_orphan:
                                    db_conn.execute(f"INSERT INTO xbrl_facts_orphaned ({cols_str_orphan}) SELECT {cols_str_orphan} FROM df_facts_orphaned_reg")
                                db_conn.unregister('df_facts_orphaned_reg')
                                logger.debug("Orphaned XBRL Fact batch loaded into xbrl_facts_orphaned.")
                            else: logger.info("No orphaned facts found in this batch.")
                        else: logger.info("Fact DataFrame was initially empty for this batch.")

                    logger.info("Batch load operations successful within transaction.")

                except duckdb.Error as e:
                    logger.error(f"DuckDB Error during data loading: {e}", exc_info=True)
                    raise e
                except Exception as e:
                    logger.error(f"Unexpected error during data loading: {e}", exc_info=True)
                    raise e

            else:
                logger.warning("--- Skipping Data Loading based on config setting ---")

            # --- Create Indexes ---
            if is_last_batch and not skip_index_creation:
                logger.info("Creating indexes (may take time)...")
                try:
                    for index_sql in SCHEMA["indexes"]:
                        logger.debug(f"Executing: {index_sql}")
                        db_conn.execute(index_sql)
                    logger.info("Indexes created successfully.")
                except duckdb.Error as e:
                    logger.error(f"DuckDB Error during index creation: {e}", exc_info=True)
                    raise e
                except Exception as e:
                    logger.error(f"Unexpected error during index creation: {e}", exc_info=True)
                    raise e
            elif is_last_batch and skip_index_creation:
                logger.warning("--- Skipping Index Creation based on config setting ---")

    except ConnectionError as e:
        logger.critical(f"Database Connection Error: {e}. Cannot continue batch.")
        raise # Re-raise to stop processing
    except Exception as e:
        logger.error(f"Critical error processing batch: {e}", exc_info=True)
        raise # Re-raise to stop processing

# --- Main Loading Logic ---
if __name__ == "__main__":

    # --- Initialize Config and Logging ---
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        # Config loading failed and exited, log using basic config if possible
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.getLogger().critical(f"Unexpected error loading config: {e}", exc_info=True)
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO) # Use INFO by default

    # Log effective settings
    logger.info(f"--- Starting EDGAR Data Loader Script ---")
    logger.info(f"Using DB_FILE: {config.DB_FILE}")
    logger.info(f"Using EXTRACT_BASE_DIR: {config.EXTRACT_BASE_DIR}")
    logger.info(f"Using Ticker File: {config.TICKER_FILE_PATH}")

    # Load control toggles from config
    process_limit = config.get_optional_int("PROCESS_LIMIT", default=None)
    process_specific_cik = config.get_optional_var("PROCESS_SPECIFIC_CIK", default=None)
    skip_cik_prep = config.get_optional_bool("SKIP_CIK_PREP", default=False)
    skip_parsing = config.get_optional_bool("SKIP_PARSING", default=False)
    skip_db_load = config.get_optional_bool("SKIP_DB_LOAD", default=False)
    # Note: skip_table_creation, skip_data_loading, skip_index_creation are handled in load_batch_to_db
    cik_batch_size = config.get_optional_int("CIK_BATCH_SIZE", default=25)

    logger.info(f"Control Settings: PROCESS_LIMIT={process_limit}, PROCESS_SPECIFIC_CIK={process_specific_cik}, "
                 f"SKIP_CIK_PREP={skip_cik_prep}, SKIP_PARSING={skip_parsing}, SKIP_DB_LOAD={skip_db_load}, "
                 f"CIK_BATCH_SIZE={cik_batch_size}") # Add others if needed

    # --- 1. Get CIK List ---
    all_ciks = []
    if not skip_cik_prep:
        logger.info("--- Running Task: Prepare CIKs ---")
        ticker_data = json_parse.load_ticker_data(config.TICKER_FILE_PATH)
        if not ticker_data:
            logger.critical("Failed to load ticker data. Exiting.")
            sys.exit("Failed to load ticker data.")
        all_ciks = json_parse.extract_formatted_ciks(ticker_data)
        if not all_ciks:
            logger.critical("No CIKs extracted. Exiting.")
            sys.exit("No CIKs extracted.")
        logger.info(f"Found {len(all_ciks)} unique CIKs from ticker file.")
    elif not process_specific_cik:
         msg = "SKIP_CIK_PREP is True, but PROCESS_SPECIFIC_CIK is not set. No CIKs to process. Exiting."
         logger.critical(msg)
         sys.exit(msg)

    # --- Determine CIKs to process ---
    ciks_to_process = []
    if process_specific_cik:
        try:
             formatted_specific_cik = process_specific_cik.zfill(10)
             ciks_to_process = [formatted_specific_cik]
             logger.warning(f"--- Processing SPECIFIC CIK: {formatted_specific_cik} ---")
        except AttributeError:
             msg = f"Invalid PROCESS_SPECIFIC_CIK format: '{process_specific_cik}'. Must be a string."
             logger.critical(msg)
             sys.exit(msg)
    elif not skip_cik_prep: # Only use all_ciks or limit if prep wasn't skipped
        if process_limit is not None and isinstance(process_limit, int):
            ciks_to_process = all_ciks[:process_limit]
            logger.warning(f"--- Processing LIMITED set: First {process_limit} CIKs ---")
        else:
            ciks_to_process = all_ciks
            logger.info(f"--- Processing ALL {len(all_ciks)} CIKs from ticker file ---")

    if not ciks_to_process:
        logger.error("No CIKs selected for processing based on current settings. Exiting.")
        sys.exit(1)

    # --- 2. Initialize Aggregators ---
    aggregated_data: Dict[str, List[Dict]] = { "companies": [], "tickers": [], "former_names": [], "filings": [], "xbrl_tags": [], "xbrl_facts": [] }
    processed_ciks_in_batch = 0
    total_processed_ciks = 0
    skipped_ciks_count = 0
    run_unique_tags: Set[Tuple[Optional[str], Optional[str]]] = set()
    is_first_batch = True

    # --- 3. Iterate, Parse, Load Batches ---
    if not skip_parsing:
        logger.info("--- Running Task: Parse JSON Files and Load Batches ---")
        num_ciks_total = len(ciks_to_process)
        effective_batch_size = cik_batch_size if isinstance(cik_batch_size, int) and cik_batch_size > 0 else num_ciks_total

        for cik_index, cik in enumerate(tqdm(ciks_to_process, desc="Processing CIKs", unit="cik")):
            # Parsing Logic (using config for paths)
            submission_json_path = config.SUBMISSIONS_DIR / f"CIK{cik}.json"
            companyfacts_json_path = config.COMPANYFACTS_DIR / f"CIK{cik}.json"
            found_any_file = False
            company_entity_name = None

            if submission_json_path.is_file():
                found_any_file = True
                parsed_submission = json_parse.parse_submission_json_for_db(submission_json_path)
                if parsed_submission:
                    if parsed_submission.get("companies"): aggregated_data["companies"].append(parsed_submission["companies"])
                    aggregated_data["tickers"].extend(parsed_submission.get("tickers", []))
                    aggregated_data["former_names"].extend(parsed_submission.get("former_names", []))
                    aggregated_data["filings"].extend(parsed_submission.get("filings", []))

            if companyfacts_json_path.is_file():
                found_any_file = True
                parsed_facts = json_parse.parse_company_facts_json_for_db(companyfacts_json_path)
                if parsed_facts:
                    company_entity_name = parsed_facts.get("company_entity_name")
                    for tag_dict in parsed_facts.get("xbrl_tags", []):
                        tag_key = (tag_dict.get("taxonomy"), tag_dict.get("tag_name"))
                        if all(tag_key) and tag_key not in run_unique_tags:
                            aggregated_data["xbrl_tags"].append(tag_dict)
                            run_unique_tags.add(tag_key)
                    aggregated_data["xbrl_facts"].extend(parsed_facts.get("xbrl_facts", []))

            if company_entity_name:
                 for comp_rec in reversed(aggregated_data["companies"]):
                      if comp_rec.get("cik") == cik:
                           comp_rec["entity_name_cf"] = company_entity_name
                           break

            if found_any_file:
                processed_ciks_in_batch += 1
                total_processed_ciks += 1
            else:
                skipped_ciks_count += 1
                logger.debug(f"No submission or companyfacts JSON found for CIK {cik}")

            # Check if Batch Load is Needed
            is_last_cik_in_list = (cik_index == num_ciks_total - 1)
            if processed_ciks_in_batch > 0 and \
               (processed_ciks_in_batch >= effective_batch_size or is_last_cik_in_list):
                logger.info(f"Processed CIK batch (size {processed_ciks_in_batch}), performing DB load...")
                if not skip_db_load:
                    try:
                        # Pass config and logger to the loading function
                        load_batch_to_db(aggregated_data, config, logger, is_first_batch, is_last_cik_in_list)
                        logger.info(f"Batch load complete. Total CIKs processed so far: {total_processed_ciks}")
                    except Exception as e:
                        logger.critical(f"Failed to load batch ending at CIK {cik}: {e}. Stopping.", exc_info=True)
                        sys.exit(1)
                else:
                     logger.warning("--- Skipping DB Load phase based on config setting ---")

                # Reset aggregators for the next batch (even if DB load skipped)
                aggregated_data = { "companies": [], "tickers": [], "former_names": [], "filings": [], "xbrl_tags": [], "xbrl_facts": [] }
                run_unique_tags = set()
                processed_ciks_in_batch = 0
                is_first_batch = False # Mark first batch as done

        logger.info(f"Finished parsing and loading phase. Processed {total_processed_ciks} CIKs with data. Skipped {skipped_ciks_count} CIKs without JSON files.")

    # --- Final Check for Schema Operations if Parsing Skipped ---
    else:
        logger.warning("--- Skipping Parsing and Batch Loading Phase based on config setting ---")
        # Check toggles loaded from config earlier
        skip_table_creation = config.get_optional_bool("SKIP_TABLE_CREATION", False)
        skip_index_creation = config.get_optional_bool("SKIP_INDEX_CREATION", False)

        if not skip_db_load and (not skip_table_creation or not skip_index_creation):
             logger.info("Connecting to DB for schema operations only...")
             try:
                 # Use context manager for schema-only operations
                 with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as db_conn:
                      if db_conn is None:
                           raise ConnectionError(f"Failed connection to {config.DB_FILE_STR} for schema ops.")

                      if not skip_table_creation:
                          logger.info("Creating tables...")
                          try:
                               for table_name, create_sql in SCHEMA.items():
                                    if table_name != "indexes":
                                         logger.debug(f"Executing CREATE for table: {table_name}")
                                         db_conn.execute(create_sql)
                               logger.info("Base tables created or already exist.")
                          except Exception as e_inner:
                               logger.error(f"Schema creation failed: {e_inner}", exc_info=True); raise e_inner

                      if not skip_index_creation:
                          logger.info("Creating indexes...")
                          try:
                               for index_sql in SCHEMA["indexes"]:
                                    logger.debug(f"Executing: {index_sql}")
                                    db_conn.execute(index_sql)
                               logger.info("Indexes created.")
                          except Exception as e_inner:
                               logger.error(f"Index creation failed: {e_inner}", exc_info=True); raise e_inner

                      logger.info("Schema operations complete.")
                      # Commit/close handled by context manager
             except ConnectionError as e:
                  logger.critical(f"DB Connection Error during schema ops: {e}")
             except Exception as e:
                  logger.error(f"Error during schema-only operations: {e}", exc_info=True)
        else:
             logger.info("Skipping schema operations as well based on config.")

    # --- Final Script Message ---
    logger.info(f"--- EDGAR Data Loader Script Finished ---")