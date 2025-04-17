# -*- coding: utf-8 -*-
"""
EDGAR Data Loader Script (e.g., save as edgar_data_loader.py)

Orchestrates the parsing of extracted SEC EDGAR JSON files and loads
the structured data into a DuckDB database using batch processing.
Uses a COMPOSITE PRIMARY KEY for xbrl_tags table.

Relies on parsing functions defined in 'json_parse.py'.
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
    print("ERROR: Could not import 'json_parse.py'. Make sure it's in the same directory or accessible.", file=sys.stderr)
    sys.exit(1)
except AttributeError as e:
    print(f"ERROR: Missing required function in 'json_parse.py': {e}", file=sys.stderr)
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
SKIP_DATA_LOADING = False
SKIP_INDEX_CREATION = False
CIK_BATCH_SIZE = 100 # Set to None to disable batching

# --- Log Initial Settings ---
logging.info(f"--- Data Loader Script Start ---")
logging.info(f"Control Settings: PROCESS_LIMIT={PROCESS_LIMIT}, PROCESS_SPECIFIC_CIK={PROCESS_SPECIFIC_CIK}, "
             f"SKIP_PARSING={SKIP_PARSING}, SKIP_DB_LOAD={SKIP_DB_LOAD}, SKIP_TABLE_CREATION={SKIP_TABLE_CREATION}, "
             f"SKIP_DATA_LOADING={SKIP_DATA_LOADING}, SKIP_INDEX_CREATION={SKIP_INDEX_CREATION}, CIK_BATCH_SIZE={CIK_BATCH_SIZE}")
logging.info(f"Using DB_FILE: {DB_FILE}")
logging.info(f"Using EXTRACT_BASE_DIR: {EXTRACT_BASE_DIR}")
logging.info(f"Using Ticker File: {TICKER_FILE_PATH}")


# --- Database Schema (Revised: Composite PK for xbrl_tags) ---
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
        );""", # <<< CORRECTED SCHEMA (Removed tag_id)
    "xbrl_facts": """
        CREATE TABLE IF NOT EXISTS xbrl_facts (
            cik VARCHAR(10) NOT NULL, accession_number VARCHAR NOT NULL, taxonomy VARCHAR NOT NULL COLLATE NOCASE,
            tag_name VARCHAR NOT NULL, unit VARCHAR NOT NULL COLLATE NOCASE, period_start_date DATE, period_end_date DATE,
            value_numeric DOUBLE, value_text VARCHAR, fy INTEGER, fp VARCHAR, form VARCHAR NOT NULL, filed_date DATE, frame VARCHAR,
            FOREIGN KEY (cik) REFERENCES companies(cik),
            FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
            FOREIGN KEY (taxonomy, tag_name) REFERENCES xbrl_tags(taxonomy, tag_name) -- Composite Foreign Key
        );""", # <<< CORRECTED SCHEMA
    "indexes": [
        "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies (primary_name);",
        "CREATE INDEX IF NOT EXISTS idx_tickers_ticker ON tickers (ticker);",
        "CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings (cik);",
        "CREATE INDEX IF NOT EXISTS idx_filings_form ON filings (form);",
        "CREATE INDEX IF NOT EXISTS idx_filings_date ON filings (filing_date);",
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_cik_tag ON xbrl_facts (cik, taxonomy, tag_name);", # Index still useful
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_tag_date ON xbrl_facts (taxonomy, tag_name, period_end_date);", # Index still useful
        "CREATE INDEX IF NOT EXISTS idx_xbrl_facts_accn ON xbrl_facts (accession_number);"
        # Removed index on tag_id as it no longer exists
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
            # (Table creation logic remains the same, uses corrected schema)
            logging.info("Creating tables if they don't exist...")
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
                db_conn.begin()

                # --- Load Companies ---
                if batch_data.get("companies"):
                    # (Logic remains the same)
                    df_companies = pd.DataFrame(batch_data["companies"])
                    if not df_companies.empty:
                         logging.info(f"Loading {len(df_companies)} company records...")
                         db_conn.register('df_companies_reg', df_companies)
                         df_cols_to_load = [f'"{col}"' for col in df_companies.columns]
                         col_str = ", ".join(df_cols_to_load)
                         update_setters = ", ".join([f"{col} = excluded.{col}" for col in df_cols_to_load if col != '"cik"' and col != '"first_added_timestamp"'])
                         if update_setters: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO UPDATE SET {update_setters}"""
                         else: sql = f"""INSERT INTO companies ({col_str}) SELECT {col_str} FROM df_companies_reg ON CONFLICT (cik) DO NOTHING"""
                         db_conn.execute(sql)
                         db_conn.unregister('df_companies_reg')
                         logging.debug("Company batch loaded/updated.")

                # --- Load Tickers ---
                if batch_data.get("tickers"):
                    # (Logic remains the same)
                    unique_tickers = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['tickers']}]
                    if unique_tickers:
                         logging.info(f"Loading {len(unique_tickers)} unique ticker records...")
                         ticker_data_tuples = [tuple(d.get(col) for col in ['cik', 'ticker', 'exchange', 'source']) for d in unique_tickers]
                         db_conn.executemany("INSERT INTO tickers (cik, ticker, exchange, source) VALUES (?, ?, ?, ?) ON CONFLICT (cik, ticker, exchange) DO NOTHING", ticker_data_tuples)
                         logging.debug("Ticker batch loaded.")

                # --- Load Former Names ---
                if batch_data.get("former_names"):
                    # (Logic remains the same)
                     unique_fns = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['former_names']}]
                     if unique_fns:
                         logging.info(f"Loading {len(unique_fns)} unique former name records...")
                         fn_data_tuples = [tuple(d.get(col) for col in ['cik', 'former_name', 'date_from', 'date_to']) for d in unique_fns]
                         db_conn.executemany("INSERT INTO former_names (cik, former_name, date_from, date_to) VALUES (?, ?, ?, ?) ON CONFLICT (cik, former_name, date_from) DO NOTHING", fn_data_tuples)
                         logging.debug("Former name batch loaded.")

                # --- Load Filings ---
                if batch_data.get("filings"):
                    # (Logic remains the same)
                    unique_filings = {d['accession_number']: d for d in batch_data['filings'] if d.get('accession_number')}.values()
                    if unique_filings:
                         logging.info(f"Loading {len(unique_filings)} unique filing records...")
                         filing_cols = ['cik', 'accession_number', 'filing_date', 'report_date', 'acceptance_datetime', 'act', 'form', 'file_number', 'film_number', 'items', 'size', 'is_xbrl', 'is_inline_xbrl', 'primary_document', 'primary_doc_description']
                         filing_data_tuples = [tuple(d.get(col) for col in filing_cols) for d in unique_filings]
                         placeholders = ", ".join(["?"] * len(filing_cols))
                         db_conn.executemany(f"INSERT INTO filings ({', '.join(filing_cols)}) VALUES ({placeholders}) ON CONFLICT (accession_number) DO NOTHING", filing_data_tuples)
                         logging.debug("Filing batch loaded.")


                # --- Load XBRL Tags --- (Uses composite PK)
                if batch_data.get("xbrl_tags"):
                    if batch_data["xbrl_tags"]:
                        logging.info(f"Loading {len(batch_data['xbrl_tags'])} unique tag definitions...")
                        df_tags = pd.DataFrame(batch_data["xbrl_tags"])
                        if not df_tags.empty:
                             db_conn.register('df_tags_reg', df_tags)
                             # Columns match the table definition (no tag_id)
                             tag_cols = ['taxonomy', 'tag_name', 'label', 'description']
                             col_str = ", ".join([f'"{c}"' for c in tag_cols])
                             # ON CONFLICT targets the composite primary key
                             sql = f"""
                                 INSERT INTO xbrl_tags ({col_str})
                                 SELECT {col_str} FROM df_tags_reg
                                 ON CONFLICT (taxonomy, tag_name) DO NOTHING
                             """
                             db_conn.execute(sql)
                             db_conn.unregister('df_tags_reg')
                             logging.debug("XBRL Tag batch loaded.")

                # --- Load XBRL Facts --- (remains same)
                if batch_data.get("xbrl_facts"):
                     # (Logic remains the same)
                     logging.info(f"Loading {len(batch_data['xbrl_facts'])} fact records...")
                     df_facts = pd.DataFrame(batch_data["xbrl_facts"])
                     if not df_facts.empty:
                         # (Type conversion logic remains same)
                         for col in ['period_start_date', 'period_end_date', 'filed_date']:
                              if col in df_facts.columns: df_facts[col] = pd.to_datetime(df_facts[col], errors='coerce').dt.date
                         if 'value_numeric' in df_facts.columns: df_facts['value_numeric'] = pd.to_numeric(df_facts['value_numeric'], errors='coerce')
                         if 'fy' in df_facts.columns: df_facts['fy'] = pd.to_numeric(df_facts['fy'], errors='coerce', downcast='integer')

                         # (Delete logic remains same)
                         ciks_in_batch = list(df_facts['cik'].unique())
                         if ciks_in_batch:
                              placeholder = ','.join('?' * len(ciks_in_batch))
                              logging.info(f"Deleting existing facts for {len(ciks_in_batch)} CIKs in batch before insert...")
                              db_conn.execute(f"DELETE FROM xbrl_facts WHERE cik IN ({placeholder})", ciks_in_batch)

                         # (Insert logic remains same)
                         db_conn.register('df_facts_reg', df_facts)
                         df_cols_to_load = [f'"{col}"' for col in df_facts.columns]
                         cols_str = ", ".join(df_cols_to_load)
                         if cols_str: db_conn.execute(f"INSERT INTO xbrl_facts ({cols_str}) SELECT {cols_str} FROM df_facts_reg")
                         else: logging.warning("No columns derived from DataFrame for xbrl_facts insert.")
                         db_conn.unregister('df_facts_reg')
                         logging.debug("XBRL Fact batch loaded.")


                db_conn.commit()
                logging.info("Batch load transaction committed.")

            except duckdb.Error as e:
                logging.error(f"DuckDB Error during data loading transaction: {e}", exc_info=True)
                if db_conn: db_conn.rollback(); raise
            except Exception as e:
                 logging.error(f"Unexpected error data loading transaction: {e}", exc_info=True)
                 if db_conn: db_conn.rollback(); raise
        else:
            logging.warning("--- Skipping Data Loading based on toggle settings ---")

        # --- Create Indexes ---
        if is_last_batch and not SKIP_INDEX_CREATION:
            # (Logic remains the same)
            logging.info("Creating indexes (may take time)...")
            try:
                db_conn.begin()
                for index_sql in SCHEMA["indexes"]:
                    logging.debug(f"Executing: {index_sql}")
                    db_conn.execute(index_sql)
                db_conn.commit()
                logging.info("Indexes created.")
            except duckdb.Error as e:
                logging.error(f"DuckDB Error during index creation: {e}", exc_info=True)
                if db_conn: db_conn.rollback()
            except Exception as e:
                logging.error(f"Unexpected error index creation: {e}", exc_info=True)
                if db_conn: db_conn.rollback()
        elif is_last_batch and SKIP_INDEX_CREATION:
            logging.warning("--- Skipping Index Creation based on toggle settings ---")

    except Exception as e:
        logging.error(f"Error in DB operation batch: {e}", exc_info=True)
        raise
    finally:
        if db_conn:
            try: db_conn.close(); logging.debug("DB connection closed for batch.")
            except Exception as e: logging.error(f"Error closing DB connection: {e}")


# --- Main Loading Logic ---
if __name__ == "__main__":

    # --- 1. Get CIK List ---
    all_ciks = [] # Initialize outside
    if not SKIP_CIK_PREP:
        logging.info("--- Running Task: Prepare CIKs ---")
        ticker_data = load_ticker_data(TICKER_FILE_PATH)
        if not ticker_data: sys.exit("Failed to load ticker data.")
        all_ciks = extract_formatted_ciks(ticker_data) # Assigned here
        if not all_ciks: sys.exit("No CIKs extracted.")
        logging.info(f"Found {len(all_ciks)} unique CIKs from ticker file.")
    elif not PROCESS_SPECIFIC_CIK:
         # Exit if we skip CIK prep AND don't have a specific CIK to run
         sys.exit("SKIP_CIK_PREP is True, but PROCESS_SPECIFIC_CIK is not set. No CIKs to process. Exiting.")
    # else: all_ciks remains empty if SKIP_CIK_PREP=True and PROCESS_SPECIFIC_CIK is set

    # --- Determine CIKs to process ---
    ciks_to_process = [] # Initialize before conditional assignment
    if PROCESS_SPECIFIC_CIK:
        ciks_to_process = [PROCESS_SPECIFIC_CIK]
        logging.warning(f"--- Processing SPECIFIC CIK: {PROCESS_SPECIFIC_CIK} ---")
    elif not SKIP_CIK_PREP: # Only use all_ciks or limit if prep wasn't skipped
        if PROCESS_LIMIT is not None:
            ciks_to_process = all_ciks[:PROCESS_LIMIT]
            logging.warning(f"--- Processing LIMITED set: First {PROCESS_LIMIT} CIKs ---")
        else:
            ciks_to_process = all_ciks
            logging.info(f"--- Processing ALL {len(all_ciks)} CIKs from ticker file ---")

    # Final check if we have anything to process
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
        # (Main loop structure remains the same)
        logging.info("--- Running Task: Parse JSON Files and Load Batches ---")
        num_ciks_total = len(ciks_to_process)
        for cik_index, cik in enumerate(tqdm(ciks_to_process, desc="Processing CIKs")):
            # --- Parsing Logic (remains the same) ---
            # ... find files, call parsers, append to aggregated_data ...
            submission_json_path = SUBMISSIONS_DIR / f"CIK{cik}.json"
            companyfacts_json_path = COMPANYFACTS_DIR / f"CIK{cik}.json"
            found_any_file = False
            company_entity_name = None

            if submission_json_path.is_file():
                found_any_file = True
                parsed_submission = parse_submission_json_for_db(submission_json_path)
                if parsed_submission:
                    if parsed_submission.get("companies"): aggregated_data["companies"].append(parsed_submission["companies"])
                    aggregated_data["tickers"].extend(parsed_submission.get("tickers", []))
                    aggregated_data["former_names"].extend(parsed_submission.get("former_names", []))
                    aggregated_data["filings"].extend(parsed_submission.get("filings", []))

            if companyfacts_json_path.is_file():
                found_any_file = True
                parsed_facts = parse_company_facts_json_for_db(companyfacts_json_path)
                if parsed_facts:
                    company_entity_name = parsed_facts.get("company_entity_name")
                    for tag_dict in parsed_facts.get("xbrl_tags", []):
                        tag_key = (tag_dict["taxonomy"], tag_dict["tag_name"])
                        if tag_key not in run_unique_tags:
                            aggregated_data["xbrl_tags"].append(tag_dict)
                            run_unique_tags.add(tag_key)
                    aggregated_data["xbrl_facts"].extend(parsed_facts.get("xbrl_facts", []))

            if company_entity_name:
                 for comp_rec in aggregated_data["companies"]:
                      if comp_rec["cik"] == cik:
                           comp_rec["entity_name_cf"] = company_entity_name
                           break

            if found_any_file: processed_ciks_in_batch += 1; total_processed_ciks += 1
            else: skipped_ciks_count += 1; logging.debug(f"No files for CIK {cik}")

            # --- Check if Batch Load is Needed ---
            # (Logic remains the same)
            is_last_cik_in_list = (cik_index == num_ciks_total - 1)
            if CIK_BATCH_SIZE and processed_ciks_in_batch > 0 and \
               (processed_ciks_in_batch >= CIK_BATCH_SIZE or is_last_cik_in_list):
                logging.info(f"Processed CIK batch (size {processed_ciks_in_batch}), performing DB load...")
                try:
                    load_batch_to_db(aggregated_data, DB_FILE, is_first_batch, is_last_cik_in_list)
                    logging.info(f"Batch load complete. Total CIKs processed so far: {total_processed_ciks}")
                    aggregated_data = {k: [] for k in aggregated_data}
                    run_unique_tags = set()
                    processed_ciks_in_batch = 0
                    is_first_batch = False
                except Exception as e:
                    logging.error(f"Failed to load batch ending at CIK {cik}: {e}. Stopping.")
                    sys.exit(1)

        # --- Load Final Batch (Only if batching disabled) ---
        # (Logic remains the same)
        if not CIK_BATCH_SIZE:
             if total_processed_ciks > 0 and any(len(v) > 0 for v in aggregated_data.values()):
                 logging.info(f"Batching disabled. Processing final load for {total_processed_ciks} CIKs...")
                 try:
                      load_batch_to_db(aggregated_data, DB_FILE, is_first_batch, True)
                      logging.info(f"Final load complete. Total CIKs processed: {total_processed_ciks}")
                 except Exception as e:
                      logging.error(f"Failed to load final batch: {e}. Stopping.")
                      sys.exit(1)
             elif not SKIP_PARSING:
                  logging.info("No data aggregated in single pass to load.")

        logging.info(f"Finished parsing and loading phase. Processed {total_processed_ciks} CIKs with data. Skipped {skipped_ciks_count} CIKs.")

    # --- Final Check for Schema Operations if Parsing Skipped ---
    # (Logic remains the same)
    else:
        logging.warning("--- Skipping Parsing and Batch Loading Phase based on toggle settings ---")
        if not SKIP_DB_LOAD and (not SKIP_TABLE_CREATION or not SKIP_INDEX_CREATION):
             # ... (Schema/Index creation only logic) ...
             logging.info("Connecting to DB for schema operations only...")
             db_conn = None
             try:
                  DB_FILE.parent.mkdir(parents=True, exist_ok=True)
                  db_conn = duckdb.connect(database=str(DB_FILE), read_only=False)
                  if not SKIP_TABLE_CREATION:
                      logging.info("Creating tables...")
                      db_conn.begin()
                      try:
                           # No sequence to create now
                           for table_name, create_sql in SCHEMA.items():
                                if table_name != "indexes": db_conn.execute(create_sql)
                           db_conn.commit()
                      except Exception as e_inner: db_conn.rollback(); logging.error(f"Schema creation failed: {e_inner}"); raise e_inner
                  if not SKIP_INDEX_CREATION:
                      logging.info("Creating indexes...")
                      db_conn.begin()
                      try:
                           for index_sql in SCHEMA["indexes"]: db_conn.execute(index_sql)
                           db_conn.commit()
                      except Exception as e_inner: db_conn.rollback(); logging.error(f"Index creation failed: {e_inner}"); raise e_inner
                  logging.info("Schema operations complete.")
             except Exception as e: logging.error(f"Error during schema operations: {e}")
             finally:
                  if db_conn: db_conn.close()

    # --- Final Script Message ---
    logging.info("--- Data Loader Script Finished ---")