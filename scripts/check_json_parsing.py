
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set

# --- BEGIN: Add project root to sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

from data_processing import json_parse
from utils.logging_utils import setup_logging

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent.parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

def run_json_parsing_check(submission_path: Path, companyfacts_path: Path):
    logger.info(f"--- Starting JSON Parsing Integrity Check ---")
    logger.info(f"Submission JSON: {submission_path}")
    logger.info(f"CompanyFacts JSON: {companyfacts_path}")

    # --- Parse Submission JSON ---
    logger.info("\n--- Parsing Submission JSON ---")
    parsed_submission_data = json_parse.parse_submission_json_for_db(submission_path)

    if parsed_submission_data:
        logger.info("Submission Data Parsed Successfully.")
        logger.info(f"  Companies: {parsed_submission_data.get('companies', {}).get('primary_name', 'N/A')}")
        logger.info(f"  Tickers Count: {len(parsed_submission_data.get('tickers', []))}")
        logger.info(f"  Former Names Count: {len(parsed_submission_data.get('former_names', []))}")
        logger.info(f"  Filings Count: {len(parsed_submission_data.get('filings', []))}")

        # Print a sample filing
        if parsed_submission_data.get('filings'):
            logger.info("\n  --- Sample Filing ---")
            for k, v in parsed_submission_data['filings'][0].items():
                logger.info(f"    {k}: {v}")
    else:
        logger.error("Failed to parse submission data.")
        return

    # --- Parse CompanyFacts JSON ---
    logger.info("\n--- Parsing CompanyFacts JSON ---")
    # Pass relevant accession numbers from submission data to filter facts
    relevant_accession_numbers = {f['accession_number'] for f in parsed_submission_data.get('filings', [])}
    parsed_facts_data = json_parse.parse_company_facts_json_for_db(companyfacts_path, relevant_accession_numbers=relevant_accession_numbers)

    if parsed_facts_data:
        logger.info("CompanyFacts Data Parsed Successfully.")
        logger.info(f"  Company Entity Name: {parsed_facts_data.get('company_entity_name', 'N/A')}")
        logger.info(f"  XBRL Tags Count: {len(parsed_facts_data.get('xbrl_tags', []))}")
        logger.info(f"  XBRL Facts Count: {len(parsed_facts_data.get('xbrl_facts', []))}")

        # Print a sample XBRL tag
        if parsed_facts_data.get('xbrl_tags'):
            logger.info("\n  --- Sample XBRL Tag ---")
            for k, v in parsed_facts_data['xbrl_tags'][0].items():
                logger.info(f"    {k}: {v}")

        # Print a sample XBRL fact
        if parsed_facts_data.get('xbrl_facts'):
            logger.info("\n  --- Sample XBRL Fact ---")
            for k, v in parsed_facts_data['xbrl_facts'][0].items():
                logger.info(f"    {k}: {v}")
    else:
        logger.error("Failed to parse company facts data.")
        return

    logger.info("\n--- JSON Parsing Integrity Check Finished ---")

if __name__ == "__main__":
    # These paths would typically come from command-line arguments or config
    # For this specific check, we'll hardcode them based on user input.
    SUBMISSION_JSON_PATH = Path("/Users/dougstrouth/datasets_noicloud/edgar/downloads/extracted_json/submissions/CIK0000019617.json")
    COMPANYFACTS_JSON_PATH = Path("/Users/dougstrouth/datasets_noicloud/edgar/downloads/extracted_json/companyfacts/CIK0000019617.json")

    run_json_parsing_check(SUBMISSION_JSON_PATH, COMPANYFACTS_JSON_PATH)
