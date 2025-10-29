# -*- coding: utf-8 -*-
"""
EDGAR JSON to Parquet Parser

Orchestrates the parsing of extracted SEC EDGAR JSON files and saves
the structured data into an intermediate Parquet format for fast, subsequent
loading into a database.

This script is the first part of the two-stage loading process.
"""

import sys
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from tqdm import tqdm

# --- BEGIN: Add project root to sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from data_processing import json_parse
from data_processing import parquet_converter

# --- Constants ---
DEFAULT_MAX_CPU_IO_WORKERS = 8

# --- Worker function for parallel parsing ---
def parse_cik_data_worker(cik: str, submissions_dir: Path, companyfacts_dir: Path) -> Optional[Dict[str, Any]]:
    """
    Worker function to parse JSON files for a single CIK.
    Returns a dictionary of parsed data or None if no files found/parsed.
    """
    submission_json_path = submissions_dir / f"CIK{cik}.json"
    companyfacts_json_path = companyfacts_dir / f"CIK{cik}.json"
    
    parsed_data_for_cik = {
        "cik": cik, "companies": None, "tickers": [], "former_names": [], "filings": [],
        "xbrl_tags": [], "xbrl_facts": [], "company_entity_name": None, "found_any_file": False
    }

    if submission_json_path.is_file():
        parsed_data_for_cik["found_any_file"] = True
        parsed_submission = json_parse.parse_submission_json_for_db(submission_json_path)
        if parsed_submission:
            parsed_data_for_cik.update(parsed_submission)
            # Create a set of accession numbers from the successfully parsed filings
            relevant_accession_numbers = {f['accession_number'] for f in parsed_submission.get("filings", [])}
        else:
            relevant_accession_numbers = set()

    if companyfacts_json_path.is_file():
        parsed_data_for_cik["found_any_file"] = True
        # Pass the set of relevant accession numbers to the facts parser
        parsed_facts = json_parse.parse_company_facts_json_for_db(companyfacts_json_path, relevant_accession_numbers=relevant_accession_numbers)
        if parsed_facts:
            parsed_data_for_cik["company_entity_name"] = parsed_facts.get("company_entity_name")
            parsed_data_for_cik["xbrl_tags"].extend(parsed_facts.get("xbrl_tags", []))
            parsed_data_for_cik["xbrl_facts"].extend(parsed_facts.get("xbrl_facts", []))
    
    return parsed_data_for_cik if parsed_data_for_cik["found_any_file"] else None

# --- Main Loading Logic ---
if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)
    parquet_converter.logger = logger # Share logger with converter
    # Configure the json_parse logger to be less verbose
    logging.getLogger(json_parse.__name__).setLevel(logging.WARNING)

    max_parsing_workers = config.get_optional_int("MAX_CPU_IO_WORKERS", DEFAULT_MAX_CPU_IO_WORKERS)

    logger.info(f"--- Starting EDGAR JSON to Parquet Conversion ---")
    config.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Parquet output directory: {config.PARQUET_DIR}")

    # Load control toggles
    process_limit = config.get_optional_int("PROCESS_LIMIT", default=None)
    process_specific_cik = config.get_optional_var("PROCESS_SPECIFIC_CIK", default=None)
    cik_batch_size = config.get_optional_int("CIK_BATCH_SIZE", default=100)

    # --- 1. Get CIK List ---
    logger.info("--- Running Task: Prepare CIKs ---")
    ticker_data = json_parse.load_ticker_data(config.TICKER_FILE_PATH)
    if not ticker_data:
        logger.critical("Failed to load ticker data. Exiting.")
        sys.exit(1)
    all_ciks = json_parse.extract_formatted_ciks(ticker_data)
    if not all_ciks:
        logger.critical("No CIKs extracted. Exiting.")
        sys.exit(1)

    # --- Determine CIKs to process ---
    ciks_to_process = []
    if process_specific_cik:
        ciks_to_process = [process_specific_cik.zfill(10)]
        logger.warning(f"--- Processing SPECIFIC CIK: {ciks_to_process[0]} ---")
    elif process_limit:
        ciks_to_process = all_ciks[:process_limit]
        logger.warning(f"--- Processing LIMITED set: First {process_limit} CIKs ---")
    else:
        ciks_to_process = all_ciks
        logger.info(f"--- Processing ALL {len(all_ciks)} CIKs ---")

    if not ciks_to_process:
        logger.error("No CIKs selected for processing. Exiting.")
        sys.exit(1)

    # --- 2. Parse and Convert in Batches ---
    total_processed_ciks = 0
    skipped_ciks_count = 0
    run_unique_tags: Set[Tuple[Optional[str], Optional[str]]] = set()

    # Use a ProcessPoolExecutor for the single, heavy I/O-bound writing task
    # and a ThreadPoolExecutor for the many, light I/O-bound parsing tasks.
    with ProcessPoolExecutor(max_workers=1) as writer_executor, ThreadPoolExecutor(max_workers=max_parsing_workers) as parsing_executor:
        parsing_jobs = [(cik, config.SUBMISSIONS_DIR, config.COMPANYFACTS_DIR) for cik in ciks_to_process]
        parsed_results_queue = []
        writer_future = None

        future_to_cik = { parsing_executor.submit(parse_cik_data_worker, *job): job[0] for job in parsing_jobs }

        for future in tqdm(as_completed(future_to_cik), total=len(ciks_to_process), desc="Parsing CIK JSONs"):
            cik = future_to_cik[future]
            try:
                parsed_data = future.result()
                if parsed_data:
                    parsed_results_queue.append(parsed_data)
                    total_processed_ciks += 1
                else:
                    skipped_ciks_count += 1
            except Exception as exc:
                logger.error(f"Error parsing CIK {cik}: {exc}", exc_info=True)
                skipped_ciks_count += 1

            # Check if a batch is ready for conversion
            is_last_item = total_processed_ciks + skipped_ciks_count == len(ciks_to_process)
            if parsed_results_queue and (len(parsed_results_queue) >= cik_batch_size or (is_last_item and parsed_results_queue)):
                
                # If a writer process is still running, wait for it to finish before submitting the next batch
                if writer_future and not writer_future.done():
                    logger.info("Waiting for previous Parquet writer process to finish...")
                    writer_future.result() # This blocks and will raise exceptions from the writer process

                logger.info(f"Batch of {len(parsed_results_queue)} parsed CIKs ready for Parquet conversion...")

                # Aggregate data from the queue
                batch_aggregated_data: Dict[str, List[Dict]] = { "companies": [], "tickers": [], "former_names": [], "filings": [], "xbrl_tags": [], "xbrl_facts": [] }
                for item in parsed_results_queue:
                    if item.get("companies"): batch_aggregated_data["companies"].append(item["companies"])
                    batch_aggregated_data["tickers"].extend(item.get("tickers", []))
                    batch_aggregated_data["former_names"].extend(item.get("former_names", []))
                    batch_aggregated_data["filings"].extend(item.get("filings", []))
                    batch_aggregated_data["xbrl_facts"].extend(item.get("xbrl_facts", []))
                    for tag_dict in item.get("xbrl_tags", []):
                        tag_key = (tag_dict.get("taxonomy"), tag_dict.get("tag_name"))
                        if all(tag_key) and tag_key not in run_unique_tags:
                            batch_aggregated_data["xbrl_tags"].append(tag_dict)
                            run_unique_tags.add(tag_key)
                    if item.get("company_entity_name"):
                        for comp_rec in batch_aggregated_data["companies"]:
                            if comp_rec.get("cik") == item["cik"]:
                                comp_rec["entity_name_cf"] = item["company_entity_name"]
                                break
                
                # Submit the writing task to the separate writer process
                writer_future = writer_executor.submit(parquet_converter.process_batch_to_parquet, batch_aggregated_data, config.PARQUET_DIR)
                logger.info("Submitted batch to writer process. Continuing parsing...")

                # Clear the queue for the next batch
                parsed_results_queue = []
        
        # Final check to ensure the last submitted writer job completes
        if writer_future:
            logger.info("Waiting for the final Parquet writer process to complete...")
            writer_future.result()

    logger.info(f"Finished parsing and conversion. Processed {total_processed_ciks} CIKs. Skipped {skipped_ciks_count} CIKs.")
    logger.info(f"--- EDGAR JSON to Parquet Conversion Finished ---")