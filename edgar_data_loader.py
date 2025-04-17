# -*- coding: utf-8 -*-
"""
EDGAR Data Loader Script (e.g., save as edgar_data_loader.py)

Orchestrates the parsing of extracted SEC EDGAR JSON files and loads
the structured data into a DuckDB database using batch processing.
Uses a COMPOSITE PRIMARY KEY for xbrl_tags table.

Handles facts referencing missing filings by storing them in a separate
'xbrl_facts_orphaned' table (Option 2).

Handles reprocessing by pre-deleting dependent records (including filings)
before company updates.

Ensures key VARCHAR columns are explicitly cast to string before DB insertion.

Relies on parsing functions defined in 'edgar_data_parser.py' (or equivalent).
Includes toggles for controlling execution flow (limits, skipping phases, batch size).
"""

import json
import os
import sys
import logging
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date, timezone
import math

# --- Import Parsers from the second script ---
try:
    import json_parse
    load_ticker_data = json_parse.load_ticker_data
    extract_formatted_ciks = json_parse.extract_formatted_ciks
    parse_submission_json_for_db = json_parse.parse_submission_json_for_db
    parse_company_facts_json_for_db = json_parse.parse_company_facts_json_for_db
except ImportError:
    print("ERROR: Could not import 'edgar_data_parser.py'. Make sure it's in the same directory or accessible.", file=sys.stderr)
    sys.exit(1)
except AttributeError as e:
    print(f"ERROR: Missing required function in 'edgar_data_parser.py': {e}", file=sys.stderr)
    sys.exit(1)

# --- Configuration from .env ---
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'
if not dotenv_path.is_file(): sys.exit(f"ERROR: .env file not found at {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

try:
    DOWNLOAD_DIR = Path(os.environ['DOWNLOAD_DIR']).resolve()
    EXTRACT_BASE_DIR = DOWNLOAD_DIR / "extracted_json"
    SUBMISSIONS_DIR = EXTRACT_BASE_DIR / "submissions"
    COMPANYFACTS_DIR = EXTRACT_BASE_DIR / "companyfacts"
    TICKER_FILE_PATH = DOWNLOAD_DIR / "company_tickers.json"
    DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError as e: sys.exit(f"ERROR: Missing required .env variable: {e}")
except Exception as e: sys.exit(f"Error configuring paths: {e}")

# --- Logging Setup ---
log_file_path = script_dir / "edgar_data_loader.log"
log_file_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

# --- Control Settings / Toggles ---
PROCESS_LIMIT = None
PROCESS_SPECIFIC_CIK = None
SKIP_CIK_PREP = False
SKIP_PARSING = False
SKIP_DB_LOAD = False
SKIP_TABLE_CREATION = False
SKIP_DATA_LOADING = False # Set to True to only create schema/indexes
SKIP_INDEX_CREATION = False
CIK_BATCH_SIZE = 25

# --- Log Initial Settings ---
logging.info(f"--- Data Loader Script Start ---")
logging.info(f"Control Settings: PROCESS_LIMIT={PROCESS_LIMIT}, PROCESS_SPECIFIC_CIK={PROCESS_SPECIFIC_CIK}, "
             f"SKIP_PARSING={SKIP_PARSING}, SKIP_DB_LOAD={SKIP_DB_LOAD}, SKIP_TABLE_CREATION={SKIP_TABLE_CREATION}, "
             f"SKIP_DATA_LOADING={SKIP_DATA_LOADING}, SKIP_INDEX_CREATION={SKIP_INDEX_CREATION}, CIK_BATCH_SIZE={CIK_BATCH_SIZE}")
logging.info(f"Using DB_FILE: {DB_FILE}")
logging.info(f"Using EXTRACT_BASE_DIR: {EXTRACT_BASE_DIR}")
logging.info(f"Using Ticker File: {TICKER_FILE_PATH}")

# --- Database Schema ---
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
        # Optional indexes for the orphan table
        "CREATE INDEX IF NOT EXISTS idx_orphan_facts_cik_accn ON xbrl_facts_orphaned (cik, accession_number);",
        "CREATE INDEX IF NOT EXISTS idx_orphan_facts_tag ON xbrl_facts_orphaned (taxonomy, tag_name);"
    ]
}

# --- Helper Function for Database Loading ---
def load_batch_to_db(batch_data: Dict[str, List[Dict]], db_path: Path, is_first_batch: bool, is_last_batch: bool):
    """Connects to DuckDB and loads the provided batch of aggregated data using explicit transactions."""
    logging.info(f"Starting DB load for batch. First batch: {is_first_batch}, Last batch: {is_last_batch}")
    db_conn = None
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_conn = duckdb.connect(database=str(db_path), read_only=False, config={'default_null_order': 'nulls_last'})
        logging.debug(f"DB connection established: {db_path}")

        # --- Create Tables ---
        if is_first_batch and not SKIP_TABLE_CREATION:
            logging.info("Creating tables if they don't exist...")
            # ... (table creation logic remains the same) ...
            try:
                db_conn.begin()
                for table_name, create_sql in SCHEMA.items():
                    if table_name != "indexes":
                        logging.debug(f"Executing CREATE for table: {table_name}")
                        db_conn.execute(create_sql)
                db_conn.commit()
                logging.info("Base tables created or already exist.")
            except duckdb.Error as e:
                logging.error(f"DuckDB Error during table creation: {e}", exc_info=True); db_conn.rollback(); raise
            except Exception as e:
                logging.error(f"Unexpected error table creation: {e}", exc_info=True); db_conn.rollback(); raise
        elif is_first_batch and SKIP_TABLE_CREATION:
            logging.warning("Skipping Table Creation.")

        # --- Load Data ---
        if not SKIP_DATA_LOADING:
            logging.info("Loading batch data into tables transaction...")
            try:
                db_conn.begin() # Start transaction for all data loading

                # --- Pre-delete Dependent Records for CIKs in Batch ---
                ciks_in_batch_set = set()
                if batch_data.get("companies"):
                    ciks_in_batch_set = {comp.get("cik") for comp in batch_data["companies"] if comp.get("cik")}

                if ciks_in_batch_set:
                    ciks_list = list(ciks_in_batch_set)
                    placeholder = ','.join('?' * len(ciks_list))
                    logging.info(f"Pre-deleting dependent records for {len(ciks_list)} CIKs in batch...")
                    try:
                        db_conn.execute(f"DELETE FROM xbrl_facts WHERE cik IN ({placeholder})", ciks_list)
                        db_conn.execute(f"DELETE FROM xbrl_facts_orphaned WHERE cik IN ({placeholder})", ciks_list)
                        db_conn.execute(f"DELETE FROM filings WHERE cik IN ({placeholder})", ciks_list)
                        db_conn.execute(f"DELETE FROM tickers WHERE cik IN ({placeholder})", ciks_list)
                        db_conn.execute(f"DELETE FROM former_names WHERE cik IN ({placeholder})", ciks_list)
                        logging.info(f"Pre-deletion complete for CIKs: {', '.join(ciks_list[:5])}...")
                    except duckdb.Error as e_del:
                        logging.error(f"DuckDB Error during pre-emptive deletion: {e_del}", exc_info=True)
                        db_conn.rollback(); raise

                # --- Load Companies ---
                if batch_data.get("companies"):
                    df_companies = pd.DataFrame(batch_data["companies"])
                    if not df_companies.empty:
                         # Ensure key string columns are strings
                         str_cols = ['cik', 'primary_name', 'entity_name_cf', 'entity_type', 'sic', 'sic_description', 'ein', 'description', 'category', 'fiscal_year_end', 'state_of_incorporation', 'phone', 'flags', 'mailing_street1', 'mailing_street2', 'mailing_city', 'mailing_state_or_country', 'mailing_zip_code', 'business_street1', 'business_street2', 'business_city', 'business_state_or_country', 'business_zip_code']
                         for col in str_cols:
                              if col in df_companies.columns: df_companies[col] = df_companies[col].astype(str)

                         logging.info(f"Loading {len(df_companies)} company records...")
                         db_conn.register('df_companies_reg', df_companies)
                         df_cols_to_load = [f'"{col}"' for col in df_companies.columns]
                         col_str = ", ".join(df_cols_to_load)
                         update_cols = [col for col in df_cols_to_load if col not in ('"cik"', '"first_added_timestamp"')]
                         update_setters = ", ".join([f'{col} = excluded.{col}' for col in update_cols])
                         if update_setters: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO UPDATE SET {update_setters}"""
                         else: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO NOTHING"""
                         db_conn.execute(sql)
                         db_conn.unregister('df_companies_reg')
                         logging.debug("Company batch loaded/updated.")

                # --- Load Tickers ---
                if batch_data.get("tickers"):
                    unique_tickers = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['tickers']}]
                    if unique_tickers:
                         logging.info(f"Loading {len(unique_tickers)} unique ticker records...")
                         # Ensure string types before creating tuples
                         df_tickers = pd.DataFrame(unique_tickers)
                         for col in ['cik', 'ticker', 'exchange', 'source']:
                              if col in df_tickers.columns: df_tickers[col] = df_tickers[col].astype(str)
                         ticker_data_tuples = [tuple(x) for x in df_tickers[['cik', 'ticker', 'exchange', 'source']].to_numpy()]

                         db_conn.executemany("INSERT INTO tickers (cik, ticker, exchange, source) VALUES (?, ?, ?, ?) ON CONFLICT (cik, ticker, exchange) DO NOTHING", ticker_data_tuples)
                         logging.debug("Ticker batch loaded.")

                # --- Load Former Names ---
                if batch_data.get("former_names"):
                     unique_fns = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['former_names']}]
                     if unique_fns:
                         logging.info(f"Loading {len(unique_fns)} unique former name records...")
                         fn_data_tuples = []
                         for d in unique_fns:
                             dt_from = d.get('date_from'); dt_to = d.get('date_to')
                             if isinstance(dt_from, pd.Timestamp): dt_from = dt_from.to_pydatetime()
                             if isinstance(dt_to, pd.Timestamp): dt_to = dt_to.to_pydatetime()
                             fn_data_tuples.append(tuple([str(d['cik']), str(d['former_name']), dt_from, dt_to])) # Ensure str

                         db_conn.executemany("INSERT INTO former_names (cik, former_name, date_from, date_to) VALUES (?, ?, ?, ?) ON CONFLICT (cik, former_name, date_from) DO NOTHING", fn_data_tuples)
                         logging.debug("Former name batch loaded.")

                # --- Load Filings ---
                valid_accession_numbers_for_batch = set()
                if batch_data.get("filings"):
                    unique_filings = {d['accession_number']: d for d in batch_data['filings'] if d.get('accession_number')}.values()
                    if unique_filings:
                         logging.info(f"Loading {len(unique_filings)} unique filing records...")
                         filing_cols = ['cik', 'accession_number', 'filing_date', 'report_date', 'acceptance_datetime', 'act', 'form', 'file_number', 'film_number', 'items', 'size', 'is_xbrl', 'is_inline_xbrl', 'primary_document', 'primary_doc_description']
                         filing_data_tuples = []
                         for d in unique_filings:
                              filing_date = d.get('filing_date'); report_date = d.get('report_date'); acceptance_dt = d.get('acceptance_datetime')
                              if isinstance(filing_date, pd.Timestamp): filing_date = filing_date.date()
                              if isinstance(report_date, pd.Timestamp): report_date = report_date.date()
                              if isinstance(acceptance_dt, pd.Timestamp): acceptance_dt = acceptance_dt.to_pydatetime()
                              # Ensure string types for relevant columns
                              filing_data_tuples.append(tuple([
                                   str(d.get('cik')), str(d.get('accession_number')), filing_date, report_date,
                                   acceptance_dt, str(d.get('act')) if d.get('act') else None,
                                   str(d.get('form')), str(d.get('file_number')) if d.get('file_number') else None,
                                   str(d.get('film_number')) if d.get('film_number') else None,
                                   str(d.get('items')) if d.get('items') else None, d.get('size'), d.get('is_xbrl'),
                                   d.get('is_inline_xbrl'), str(d.get('primary_document')) if d.get('primary_document') else None,
                                   str(d.get('primary_doc_description')) if d.get('primary_doc_description') else None
                              ]))
                         placeholders = ", ".join(["?"] * len(filing_cols))
                         db_conn.executemany(f"INSERT INTO filings ({', '.join(filing_cols)}) VALUES ({placeholders}) ON CONFLICT (accession_number) DO NOTHING", filing_data_tuples)
                         valid_accession_numbers_for_batch = {tpl[1] for tpl in filing_data_tuples if tpl[1]}
                         logging.debug("Filing batch loaded.")

                # --- Load XBRL Tags ---
                if batch_data.get("xbrl_tags"):
                    unique_tag_data = {}
                    for tag_dict in batch_data.get("xbrl_tags", []):
                        key = (tag_dict.get("taxonomy"), tag_dict.get("tag_name"))
                        if all(key):
                            if key not in unique_tag_data: unique_tag_data[key] = tag_dict
                    unique_tags_list = list(unique_tag_data.values())
                    if unique_tags_list:
                        logging.info(f"Loading {len(unique_tags_list)} unique tag definitions...")
                        df_tags = pd.DataFrame(unique_tags_list)
                        if not df_tags.empty:
                             # Ensure string types
                             for col in ['taxonomy', 'tag_name', 'label', 'description']:
                                 if col in df_tags.columns: df_tags[col] = df_tags[col].astype(str)
                             db_conn.register('df_tags_reg', df_tags)
                             tag_cols = ['taxonomy', 'tag_name', 'label', 'description']
                             col_str = ", ".join([f'"{c}"' for c in tag_cols])
                             sql = f"""INSERT INTO xbrl_tags ({col_str}) SELECT {col_str} FROM df_tags_reg ON CONFLICT (taxonomy, tag_name) DO NOTHING"""
                             db_conn.execute(sql)
                             db_conn.unregister('df_tags_reg')
                             logging.debug("XBRL Tag batch loaded.")


                # --- Load XBRL Facts (Split into Valid and Orphaned) ---
                if batch_data.get("xbrl_facts"):
                    logging.info(f"Preparing {len(batch_data['xbrl_facts'])} potential fact records...")
                    df_facts_all = pd.DataFrame(batch_data["xbrl_facts"])

                    if not df_facts_all.empty:
                        # Define columns expected to be strings
                        facts_str_cols = ['cik', 'accession_number', 'taxonomy', 'tag_name', 'unit', 'value_text', 'fp', 'form', 'frame']

                        # Get valid accession numbers from filings loaded in this batch
                        if not valid_accession_numbers_for_batch:
                            logging.warning("No valid filings found in this batch. All facts will be treated as orphaned.")
                            df_facts_valid = pd.DataFrame()
                            df_facts_orphaned = df_facts_all.copy()
                        else:
                            is_valid = df_facts_all['accession_number'].isin(valid_accession_numbers_for_batch)
                            df_facts_valid = df_facts_all[is_valid].copy()
                            df_facts_orphaned = df_facts_all[~is_valid].copy()

                        # Process VALID Facts
                        if not df_facts_valid.empty:
                            logging.info(f"Processing {len(df_facts_valid)} valid facts for xbrl_facts table...")
                            # Type conversions
                            for col in ['period_start_date', 'period_end_date', 'filed_date']:
                                 if col in df_facts_valid.columns: df_facts_valid[col] = pd.to_datetime(df_facts_valid[col], errors='coerce').dt.date
                            if 'value_numeric' in df_facts_valid.columns: df_facts_valid['value_numeric'] = pd.to_numeric(df_facts_valid['value_numeric'], errors='coerce')
                            if 'fy' in df_facts_valid.columns: df_facts_valid['fy'] = pd.to_numeric(df_facts_valid['fy'], errors='coerce', downcast='integer')
                            # Explicitly cast string columns
                            for col in facts_str_cols:
                                if col in df_facts_valid.columns: df_facts_valid[col] = df_facts_valid[col].astype(str)

                            # Insert valid facts
                            db_conn.register('df_facts_valid_reg', df_facts_valid)
                            df_cols_to_load = [f'"{col}"' for col in df_facts_valid.columns]
                            cols_str = ", ".join(df_cols_to_load)
                            if cols_str:
                                 db_conn.execute(f"INSERT INTO xbrl_facts ({cols_str}) SELECT {cols_str} FROM df_facts_valid_reg")
                            db_conn.unregister('df_facts_valid_reg')
                            logging.debug("Valid XBRL Fact batch loaded into xbrl_facts.")
                        else:
                            logging.info("No valid facts found to load into xbrl_facts table for this batch.")

                        # Process ORPHANED Facts
                        if not df_facts_orphaned.empty:
                            logging.warning(f"Processing {len(df_facts_orphaned)} orphaned facts for xbrl_facts_orphaned table...")
                            # Type conversions
                            for col in ['period_start_date', 'period_end_date', 'filed_date']:
                                 if col in df_facts_orphaned.columns: df_facts_orphaned[col] = pd.to_datetime(df_facts_orphaned[col], errors='coerce').dt.date
                            if 'value_numeric' in df_facts_orphaned.columns: df_facts_orphaned['value_numeric'] = pd.to_numeric(df_facts_orphaned['value_numeric'], errors='coerce')
                            if 'fy' in df_facts_orphaned.columns: df_facts_orphaned['fy'] = pd.to_numeric(df_facts_orphaned['fy'], errors='coerce', downcast='integer')
                            # Explicitly cast string columns
                            for col in facts_str_cols:
                                if col in df_facts_orphaned.columns: df_facts_orphaned[col] = df_facts_orphaned[col].astype(str)

                            # Insert orphaned facts
                            db_conn.register('df_facts_orphaned_reg', df_facts_orphaned)
                            df_cols_to_load_orphan = [f'"{col}"' for col in df_facts_orphaned.columns]
                            cols_str_orphan = ", ".join(df_cols_to_load_orphan)
                            if cols_str_orphan:
                                 db_conn.execute(f"INSERT INTO xbrl_facts_orphaned ({cols_str_orphan}) SELECT {cols_str_orphan} FROM df_facts_orphaned_reg")
                            db_conn.unregister('df_facts_orphaned_reg')
                            logging.debug("Orphaned XBRL Fact batch loaded into xbrl_facts_orphaned.")
                        else:
                            logging.info("No orphaned facts found in this batch.")
                    else:
                         logging.info("Fact DataFrame was initially empty for this batch.")


                # --- Commit Transaction ---
                db_conn.commit()
                logging.info("Batch load transaction committed.")

            except duckdb.Error as e:
                logging.error(f"DuckDB Error during data loading transaction: {e}", exc_info=True)
                if db_conn: db_conn.rollback();
                raise e
            except Exception as e:
                 logging.error(f"Unexpected error data loading transaction: {e}", exc_info=True)
                 if db_conn: db_conn.rollback();
                 raise e
        else:
            logging.warning("--- Skipping Data Loading based on toggle settings ---")

        # --- Create Indexes ---
        if is_last_batch and not SKIP_INDEX_CREATION:
             logging.info("Creating indexes (may take time)...")
             try:
                  if db_conn is None: # Reconnect if connection wasn't established
                      db_conn = duckdb.connect(database=str(db_path), read_only=False)

                  db_conn.begin()
                  for index_sql in SCHEMA["indexes"]:
                       logging.debug(f"Executing: {index_sql}")
                       db_conn.execute(index_sql)
                  db_conn.commit()
                  logging.info("Indexes created.")
             except duckdb.Error as e:
                  logging.error(f"DuckDB Error during index creation: {e}", exc_info=True)
                  if db_conn:
                       try: db_conn.rollback()
                       except Exception as e_rb_idx: logging.error(f"Error during index rollback: {e_rb_idx}")
             except Exception as e:
                  logging.error(f"Unexpected error index creation: {e}", exc_info=True)
                  if db_conn:
                       try: db_conn.rollback()
                       except Exception as e_rb_idx: logging.error(f"Error during index rollback: {e_rb_idx}")
        elif is_last_batch and SKIP_INDEX_CREATION:
             logging.warning("--- Skipping Index Creation based on toggle settings ---")

    except Exception as e:
        logging.error(f"Error in DB operation batch (outside data transaction): {e}", exc_info=True)
        raise
    finally:
        if db_conn:
            try:
                db_conn.close()
                logging.debug("DB connection closed for batch.")
            except Exception as e:
                logging.error(f"Error closing DB connection: {e}")


# --- Main Loading Logic ---
if __name__ == "__main__":

    # --- 1. Get CIK List ---
    all_ciks = []
    if not SKIP_CIK_PREP:
        logging.info("--- Running Task: Prepare CIKs ---")
        ticker_data = load_ticker_data(TICKER_FILE_PATH)
        if not ticker_data: sys.exit("Failed to load ticker data.")
        all_ciks = extract_formatted_ciks(ticker_data)
        if not all_ciks: sys.exit("No CIKs extracted.")
        logging.info(f"Found {len(all_ciks)} unique CIKs from ticker file.")
    elif not PROCESS_SPECIFIC_CIK:
         sys.exit("SKIP_CIK_PREP is True, but PROCESS_SPECIFIC_CIK is not set. No CIKs to process. Exiting.")

    # --- Determine CIKs to process ---
    ciks_to_process = []
    if PROCESS_SPECIFIC_CIK:
        try:
             formatted_specific_cik = PROCESS_SPECIFIC_CIK.zfill(10)
             ciks_to_process = [formatted_specific_cik]
             logging.warning(f"--- Processing SPECIFIC CIK: {formatted_specific_cik} ---")
        except AttributeError:
             sys.exit(f"Invalid PROCESS_SPECIFIC_CIK format: '{PROCESS_SPECIFIC_CIK}'. Must be a string.")
    elif not SKIP_CIK_PREP:
        if PROCESS_LIMIT is not None and isinstance(PROCESS_LIMIT, int):
            ciks_to_process = all_ciks[:PROCESS_LIMIT]
            logging.warning(f"--- Processing LIMITED set: First {PROCESS_LIMIT} CIKs ---")
        else:
            ciks_to_process = all_ciks
            logging.info(f"--- Processing ALL {len(all_ciks)} CIKs from ticker file ---")

    if not ciks_to_process:
        logging.error("No CIKs selected for processing based on current settings. Exiting.")
        sys.exit(1)

    # --- 2. Initialize Aggregators ---
    aggregated_data = { "companies": [], "tickers": [], "former_names": [], "filings": [], "xbrl_tags": [], "xbrl_facts": [] }
    processed_ciks_in_batch = 0
    total_processed_ciks = 0
    skipped_ciks_count = 0
    run_unique_tags = set()
    is_first_batch = True


    # --- 3. Iterate, Parse, Load Batches ---
    if not SKIP_PARSING:
        logging.info("--- Running Task: Parse JSON Files and Load Batches ---")
        num_ciks_total = len(ciks_to_process)
        effective_batch_size = CIK_BATCH_SIZE if isinstance(CIK_BATCH_SIZE, int) and CIK_BATCH_SIZE > 0 else num_ciks_total

        for cik_index, cik in enumerate(tqdm(ciks_to_process, desc="Processing CIKs")):
            # --- Parsing Logic ---
            submission_json_path = SUBMISSIONS_DIR / f"CIK{cik}.json"
            companyfacts_json_path = COMPANYFACTS_DIR / f"CIK{cik}.json"
            found_any_file = False
            company_entity_name = None

            # Parse Submission JSON
            if submission_json_path.is_file():
                found_any_file = True
                parsed_submission = parse_submission_json_for_db(submission_json_path)
                if parsed_submission:
                    if parsed_submission.get("companies"): aggregated_data["companies"].append(parsed_submission["companies"])
                    aggregated_data["tickers"].extend(parsed_submission.get("tickers", []))
                    aggregated_data["former_names"].extend(parsed_submission.get("former_names", []))
                    aggregated_data["filings"].extend(parsed_submission.get("filings", []))

            # Parse CompanyFacts JSON
            if companyfacts_json_path.is_file():
                found_any_file = True
                parsed_facts = parse_company_facts_json_for_db(companyfacts_json_path)
                if parsed_facts:
                    company_entity_name = parsed_facts.get("company_entity_name")
                    for tag_dict in parsed_facts.get("xbrl_tags", []):
                        tag_key = (tag_dict.get("taxonomy"), tag_dict.get("tag_name"))
                        if all(tag_key) and tag_key not in run_unique_tags:
                            aggregated_data["xbrl_tags"].append(tag_dict)
                            run_unique_tags.add(tag_key)
                    aggregated_data["xbrl_facts"].extend(parsed_facts.get("xbrl_facts", []))

            # Update company record with entity name from companyfacts
            if company_entity_name:
                 for comp_rec in reversed(aggregated_data["companies"]):
                      if comp_rec.get("cik") == cik:
                           comp_rec["entity_name_cf"] = company_entity_name
                           break

            # Update counts
            if found_any_file:
                processed_ciks_in_batch += 1
                total_processed_ciks += 1
            else:
                skipped_ciks_count += 1
                logging.debug(f"No submission or companyfacts JSON found for CIK {cik}")


            # --- Check if Batch Load is Needed ---
            is_last_cik_in_list = (cik_index == num_ciks_total - 1)
            if processed_ciks_in_batch > 0 and \
               (processed_ciks_in_batch >= effective_batch_size or is_last_cik_in_list):
                logging.info(f"Processed CIK batch (size {processed_ciks_in_batch}), performing DB load...")
                try:
                    load_batch_to_db(aggregated_data, DB_FILE, is_first_batch, is_last_cik_in_list)
                    logging.info(f"Batch load complete. Total CIKs processed so far: {total_processed_ciks}")
                    # Reset aggregators for the next batch
                    aggregated_data = { "companies": [], "tickers": [], "former_names": [], "filings": [], "xbrl_tags": [], "xbrl_facts": [] }
                    run_unique_tags = set()
                    processed_ciks_in_batch = 0
                    is_first_batch = False
                except Exception as e:
                    logging.error(f"Failed to load batch ending at CIK {cik}: {e}. Stopping.", exc_info=True)
                    sys.exit(1)

        logging.info(f"Finished parsing and loading phase. Processed {total_processed_ciks} CIKs with data. Skipped {skipped_ciks_count} CIKs without JSON files.")

    # --- Final Check for Schema Operations if Parsing Skipped ---
    else:
        logging.warning("--- Skipping Parsing and Batch Loading Phase based on toggle settings ---")
        if not SKIP_DB_LOAD and (not SKIP_TABLE_CREATION or not SKIP_INDEX_CREATION):
             logging.info("Connecting to DB for schema operations only...")
             db_conn = None
             try:
                  DB_FILE.parent.mkdir(parents=True, exist_ok=True)
                  db_conn = duckdb.connect(database=str(DB_FILE), read_only=False)
                  if not SKIP_TABLE_CREATION:
                      logging.info("Creating tables...")
                      db_conn.begin()
                      try:
                           for table_name, create_sql in SCHEMA.items():
                                if table_name != "indexes":
                                     logging.debug(f"Executing CREATE for table: {table_name}")
                                     db_conn.execute(create_sql)
                           db_conn.commit()
                           logging.info("Base tables created or already exist.")
                      except Exception as e_inner:
                           db_conn.rollback(); logging.error(f"Schema creation failed: {e_inner}", exc_info=True); raise e_inner
                  if not SKIP_INDEX_CREATION:
                      logging.info("Creating indexes...")
                      db_conn.begin()
                      try:
                           for index_sql in SCHEMA["indexes"]:
                                logging.debug(f"Executing: {index_sql}")
                                db_conn.execute(index_sql)
                           db_conn.commit()
                           logging.info("Indexes created.")
                      except Exception as e_inner:
                           db_conn.rollback(); logging.error(f"Index creation failed: {e_inner}", exc_info=True); raise e_inner
                  logging.info("Schema operations complete.")
             except Exception as e: logging.error(f"Error during schema operations: {e}", exc_info=True)
             finally:
                  if db_conn:
                      try: db_conn.close()
                      except Exception as e_close: logging.error(f"Error closing DB connection: {e_close}")
        else:
             logging.info("Skipping schema operations as well.")

    # --- Final Script Message ---
    logging.info("--- Data Loader Script Finished ---")