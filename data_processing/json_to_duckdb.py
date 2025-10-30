# -*- coding: utf-8 -*-
"""
EDGAR JSON to DuckDB Parser and Loader

This script orchestrates the parsing of extracted SEC EDGAR JSON files and
loads the structured data directly into a DuckDB database. It creates temporary
tables for each data type (companies, tickers, filings, etc.) to be used in a
subsequent, atomic update of the main database tables.

This replaces the intermediate Parquet conversion step, aiming to reduce
memory usage and I/O by leveraging DuckDB's direct JSON loading and data
manipulation capabilities.
"""

import sys
import logging
from pathlib import Path
from tqdm import tqdm

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection as DuckDBConnection


from data_processing.edgar_data_loader import SCHEMA

# Define TABLE_NAMES from the SCHEMA keys
TABLE_NAMES = [name for name in SCHEMA.keys() if name != "indexes"]





import argparse

# --- Main Loading Logic ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse SEC EDGAR JSON files and load into DuckDB.")
    parser.add_argument(
        "--cik",
        type=str,
        default=None,
        help="Process only a specific CIK (Central Index Key).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of CIKs to process.",
    )
    args = parser.parse_args()

    process_specific_cik = args.cik
    process_limit = args.limit

    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"

    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)




    # --- Establish DuckDB Connection and Create Temp Tables ---
    try:
        with DuckDBConnection(db_path_override=config.DB_FILE_STR) as db_conn:
            if db_conn is None:
                raise ConnectionError(f"Failed to establish database connection to {config.DB_FILE_STR}")
            logger.info("Successfully connected to DuckDB for bulk loading.")

            # Create temporary tables based on SCHEMA for direct insertion
            for table_name in TABLE_NAMES:
                # Extract the CREATE TABLE statement, modify it for a temp table
                create_sql = SCHEMA[table_name].replace(f"CREATE TABLE IF NOT EXISTS {table_name}", f"CREATE TEMPORARY TABLE IF NOT EXISTS temp_{table_name}")

                db_conn.execute(create_sql)
                logger.info(f"Created temporary table: temp_{table_name}")

            # --- 1. Get CIK List ---


            # --- Get CIK List ---
            if process_specific_cik:
                ciks_to_process = [str(process_specific_cik)]
                logger.info(f"Processing only specified CIK: {ciks_to_process}")
            else:
                logger.info(f"Scanning for CIKs in {config.SUBMISSIONS_DIR}...")
                all_ciks = sorted([p.stem.replace('CIK', '') for p in config.SUBMISSIONS_DIR.glob("CIK*.json")])
                if process_limit:
                    ciks_to_process = all_ciks[:process_limit]
                    logger.warning(f"PROCESS_LIMIT set to {process_limit}. Processing only the first {len(ciks_to_process)} of {len(all_ciks)} total CIKs.")
                else:
                    ciks_to_process = all_ciks
                    logger.info(f"Found {len(ciks_to_process)} CIKs to process.")

            if not ciks_to_process:
                logger.warning("No CIKs found to process. Exiting.")
                sys.exit(0)

            # --- 2. Process CIKs and Load to Temp Tables using read_json ---
            logger.info(f"Processing {len(ciks_to_process)} CIKs...")

            submission_json_files = [str(config.SUBMISSIONS_DIR / f"CIK{cik}.json") for cik in ciks_to_process]
            companyfacts_json_files = [str(config.COMPANYFACTS_DIR / f"CIK{cik}.json") for cik in ciks_to_process]

            # Filter out files that don't exist
            submission_json_files = [f for f in submission_json_files if Path(f).is_file()]
            companyfacts_json_files = [f for f in companyfacts_json_files if Path(f).is_file()]

            if not submission_json_files:
                logger.warning("No submission JSON files found to process.")
            else:
                # --- Load Companies ---
                logger.info("Loading data into temp_companies...")
                db_conn.execute(f'''
                    INSERT INTO temp_companies (cik, entity_type, sic, sic_description, ein, description, state_of_incorporation, fiscal_year_end, phone, flags, primary_name)
                    SELECT
                        regexp_extract(filename, 'CIK(\d+)\.json', 1) AS cik,
                        entityType,
                        sic,
                        sicDescription,
                        ein,
                        description,
                        stateOfincorporation,
                        fiscalYearEnd,
                        phone,
                        flags,
                        name AS primary_name
                    FROM read_json({submission_json_files},
                        auto_detect=true,
                        columns={{
                            'entityType': 'VARCHAR', 'sic': 'VARCHAR', 'sicDescription': 'VARCHAR',
                            'ein': 'VARCHAR', 'description': 'VARCHAR', 'stateOfIncorporation': 'VARCHAR',
                            'fiscalYearEnd': 'VARCHAR', 'phone': 'VARCHAR', 'flags': 'VARCHAR', 'name': 'VARCHAR',
                            'filename': 'VARCHAR'
                        }}
                    )
                    WHERE regexp_extract(filename, 'CIK(\d+)\.json', 1) IS NOT NULL;
                ''')
                logger.info("Finished loading data into temp_companies.")

                # --- Load Tickers ---
                logger.info("Loading data into temp_tickers...")
                db_conn.execute(f'''
                    INSERT INTO temp_tickers (cik, ticker, exchange, source)
                    SELECT
                        cik,
                        t.ticker,
                        t.exchange,
                        'submission' as source
                    FROM read_json({submission_json_files},
                        auto_detect=true,
                        columns={{'cik': 'VARCHAR', 'tickers': 'STRUCT(ticker VARCHAR, exchange VARCHAR)[]'}}
                    ), UNNEST(tickers) AS t(ticker, exchange);
                ''')
                logger.info("Finished loading data into temp_tickers.")

                # --- Load Former Names ---
                logger.info("Loading data into temp_former_names...")
                db_conn.execute(f'''
                    INSERT INTO temp_former_names (cik, former_name, date_from, date_to)
                    SELECT
                        cik,
                        name AS former_name,
                        "from" AS date_from,
                        "to" AS date_to
                    FROM read_json({submission_json_files},
                        auto_detect=true,
                        columns={{'cik': 'VARCHAR', 'formerNames': 'STRUCT(name VARCHAR, "from" VARCHAR, "to" VARCHAR)[]'}}
                    ), UNNEST(formerNames) AS fn(name, "from", "to");
                ''')
                logger.info("Finished loading data into temp_former_names.")

                # --- Load Filings ---
                logger.info("Loading data into temp_filings...")
                db_conn.execute(f'''
                    INSERT INTO temp_filings (accession_number, cik, filing_date, report_date, acceptance_datetime, form, file_number, film_number, items, size, is_xbrl, is_inline_xbrl, primary_document, primary_doc_description)
                    SELECT
                        accessionNumber,
                        cik,
                        filingDate,
                        reportDate,
                        acceptanceDatetime,
                        form,
                        fileNumber,
                        filmNumber,
                        items,
                        size,
                        isXBRL,
                        isInlineXBRL,
                        primaryDocument,
                        primaryDocDescription
                    FROM read_json({submission_json_files},
                        json_format='auto',
                        columns={{
                            'cik': 'VARCHAR',
                            'filings': 'STRUCT(recent STRUCT(accessionNumber VARCHAR[], filingDate DATE[], reportDate DATE[], acceptanceDatetime TIMESTAMPTZ[], act VARCHAR[], form VARCHAR[], fileNumber VARCHAR[], filmNumber VARCHAR[], items VARCHAR[], size BIGINT[], isXBRL BOOLEAN[], isInlineXBRL BOOLEAN[], primaryDocument VARCHAR[], primaryDocDescription VARCHAR[]))'
                        }}
                    ),
                    UNNEST(filings.recent.accessionNumber, filings.recent.filingDate, filings.recent.reportDate, filings.recent.acceptanceDatetime, filings.recent.act, filings.recent.form, filings.recent.fileNumber, filings.recent.filmNumber, filings.recent.items, filings.recent.size, filings.recent.isXBRL, filings.recent.isInlineXBRL, filings.recent.primaryDocument, filings.recent.primaryDocDescription)
                    AS t(accessionNumber, filingDate, reportDate, acceptanceDatetime, act, form, fileNumber, filmNumber, items, size, isXBRL, isInlineXBRL, primaryDocument, primaryDocDescription);
                ''')
                logger.info("Finished loading data into temp_filings.")

            if not companyfacts_json_files:
                logger.warning("No companyfacts JSON files found to process.")
            else:
                # --- Load XBRL Tags ---
                logger.info("Loading data into temp_xbrl_tags...")
                db_conn.execute(f'''
                    INSERT INTO temp_xbrl_tags (taxonomy, tag_name, label, description)
                    SELECT DISTINCT
                        taxonomy_entry.key AS taxonomy,
                        tag_entry.key AS tag_name,
                        tag_entry.value.label AS label,
                        tag_entry.value.description AS description
                    FROM read_json({companyfacts_json_files},
                        auto_detect=true,
                        columns={{'facts': 'JSON'}}
                    ),
                    UNNEST(JSON_EACH(facts)) AS taxonomy_entry,
                    UNNEST(JSON_EACH(taxonomy_entry.value)) AS tag_entry;
                ''');
                logger.info("Finished loading data into temp_xbrl_tags.")

                # --- Load XBRL Facts ---
                logger.info("Loading data into temp_xbrl_facts...")
                db_conn.execute(f'''
                    INSERT INTO temp_xbrl_facts (cik, accession_number, taxonomy, tag_name, unit, period_end_date, value_numeric, value_text, fy, fp, form, filed_date, frame)
                    SELECT
                        cik,
                        fact_item.accn AS accession_number,
                        taxonomy_entry.key AS taxonomy,
                        tag_entry.key AS tag_name,
                        unit_key AS unit,
                        fact_item.end AS period_end_date,
                        fact_item.val AS value_numeric,
                        fact_item.val::VARCHAR AS value_text,
                        fact_item.fy AS fy,
                        fact_item.fp AS fp,
                        fact_item.form AS form,
                        fact_item.filed AS filed_date,
                        fact_item.frame AS frame
                    FROM read_json({companyfacts_json_files},
                        auto_detect=true,
                        columns={{'cik': 'VARCHAR', 'facts': 'JSON'}}
                    ),
                    UNNEST(JSON_EACH(facts)) AS taxonomy_entry,
                    UNNEST(JSON_EACH(taxonomy_entry.value)) AS tag_entry,
                    UNNEST(JSON_KEYS(tag_entry.value.units)) AS unit_key,
                    UNNEST(JSON_EXTRACT(tag_entry.value.units, CONCAT('$.', unit_key))) AS fact_item;
                ''');
                logger.info("Finished loading data into temp_xbrl_facts.")

                # Update entity_name_cf in temp_companies from companyfacts
                logger.info("Updating entity_name_cf in temp_companies from companyfacts...")
                db_conn.execute(f'''
                    UPDATE temp_companies
                    SET entity_name_cf = cf.entity_name_cf
                    FROM (
                        SELECT
                            cik,
                            entityName AS entity_name_cf
                        FROM read_json({companyfacts_json_files},
                            auto_detect=true,
                            columns={{'cik': 'VARCHAR', 'entityName': 'VARCHAR'}}
                        )
                    ) AS cf
                    WHERE temp_companies.cik = cf.cik;
                ''')
                logger.info("Finished updating entity_name_cf in temp_companies.")

            logger.info(f"Finished processing CIKs.")

    except Exception as e:
        logger.critical(f"An error occurred during JSON to DuckDB loading: {e}", exc_info=True)
        sys.exit(1)

    logger.info("--- EDGAR JSON to DuckDB Direct Loading Finished ---")
