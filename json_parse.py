# -*- coding: utf-8 -*-
"""
Parses the downloaded company_tickers.json file to extract and format
CIK numbers suitable for use with SEC EDGAR APIs.

Assumes company_tickers.json exists in the DOWNLOAD_DIR specified in the .env file.
"""

import json
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# --- Configuration ---

# Load environment variables from .env file relative to this script's location
# Assumes this script might be in the same directory as the first script, or you adjust the path.
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env' # Assumes .env is in the same directory

if not dotenv_path.is_file():
    print(f"ERROR: .env file not found at {dotenv_path}", file=sys.stderr)
    print("Please ensure a .env file exists with DOWNLOAD_DIR defined.", file=sys.stderr)
    sys.exit(1)

load_dotenv(dotenv_path=dotenv_path)

# Get DOWNLOAD_DIR from environment variables
try:
    # Assuming the first script downloaded company_tickers.json here
    DOWNLOAD_DIR = Path(os.environ['DOWNLOAD_DIR']).resolve()
    TICKER_FILE_NAME = "company_tickers.json"
    TICKER_FILE_PATH = DOWNLOAD_DIR / TICKER_FILE_NAME
except KeyError as e:
    print(f"ERROR: Missing required environment variable in .env file: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
     print(f"Error configuring paths: {e}", file=sys.stderr)
     sys.exit(1)


# --- Logging Setup ---
log_file_path = script_dir / "prepare_api_ciks.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

# --- Constants ---
# Example API URL structures (replace CIK_PAD with formatted CIK)
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK_PAD}.json"
# Example: https://data.sec.gov/api/xbrl/companyconcept/CIK##########/us-gaap/AccountsPayableCurrent.json
COMPANY_CONCEPT_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK_PAD}/{taxonomy}/{tag}.json"

# --- Functions ---

def load_ticker_data(file_path: Path) -> dict | None:
    """Loads the JSON data from the company_tickers file."""
    if not file_path.is_file():
        logging.error(f"Ticker file not found: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"Successfully loaded ticker data from {file_path.name}")
        return data
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {file_path.name}: {e}")
        return None
    except OSError as e:
        logging.error(f"Error reading file {file_path.name}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred loading {file_path.name}: {e}", exc_info=True)
        return None

def extract_formatted_ciks(ticker_data: dict) -> list[str]:
    """Extracts CIKs and formats them (zero-padded to 10 digits)."""
    formatted_ciks = []
    if not ticker_data or not isinstance(ticker_data, dict):
        logging.warning("Invalid or empty ticker data provided.")
        return []

    for key, company_info in ticker_data.items():
        if isinstance(company_info, dict) and 'cik_str' in company_info:
            try:
                cik_raw = str(company_info['cik_str'])
                # Pad with leading zeros to ensure 10 digits
                cik_padded = cik_raw.zfill(10)
                formatted_ciks.append(cik_padded)
            except Exception as e:
                logging.warning(f"Could not process entry {key}: {company_info}. Error: {e}")
        else:
             logging.warning(f"Skipping invalid entry {key}: {company_info}")

    logging.info(f"Extracted and formatted {len(formatted_ciks)} CIKs.")
    # Remove duplicates if any (though CIKs should be unique per company)
    unique_ciks = sorted(list(set(formatted_ciks)))
    if len(unique_ciks) < len(formatted_ciks):
         logging.info(f"Removed {len(formatted_ciks) - len(unique_ciks)} duplicate CIKs.")

    return unique_ciks

def generate_api_url(template: str, cik: str, **kwargs) -> str:
    """Generates an API URL using a template and parameters."""
    # Basic formatting for known templates
    if "{CIK_PAD}" in template:
        url = template.replace("{CIK_PAD}", cik)
    else: # Fallback if template uses different placeholder
        url = template

    # Replace other placeholders if provided
    for key, value in kwargs.items():
         placeholder = "{" + key + "}"
         if placeholder in url:
              url = url.replace(placeholder, str(value))
    return url


# --- Main Execution ---
if __name__ == "__main__":
    logging.info(f"--- Starting CIK Preparation from {TICKER_FILE_PATH.name} ---")

    ticker_json_data = load_ticker_data(TICKER_FILE_PATH)

    if ticker_json_data:
        all_formatted_ciks = extract_formatted_ciks(ticker_json_data)

        if all_formatted_ciks:
            logging.info(f"Total unique formatted CIKs found: {len(all_formatted_ciks)}")

            # --- Example Usage ---
            # Print the first 5 CIKs
            logging.info("First 5 formatted CIKs:")
            for cik in all_formatted_ciks[:5]:
                print(cik)

            # Generate example URLs for the first CIK
            example_cik = all_formatted_ciks[0]
            logging.info(f"\nExample API URLs for CIK: {example_cik}")

            # Example 1: Company Facts API
            facts_url = generate_api_url(COMPANY_FACTS_URL_TEMPLATE, example_cik)
            print(f"Company Facts URL: {facts_url}")

            # Example 2: Company Concept API (needs taxonomy and tag)
            # Note: You would need to know the specific taxonomy (e.g., us-gaap) and tag (e.g., RevenueFromContractWithCustomerExcludingAssessedTax)
            example_taxonomy = "us-gaap"
            example_tag = "Assets" # Just an example tag
            concept_url = generate_api_url(
                COMPANY_CONCEPT_URL_TEMPLATE,
                example_cik,
                taxonomy=example_taxonomy,
                tag=example_tag
            )
            print(f"Example Company Concept URL ({example_taxonomy}/{example_tag}): {concept_url}")

            # Optional: Save the list of CIKs to a file
            # output_cik_file = script_dir / "formatted_ciks.txt"
            # try:
            #     with open(output_cik_file, 'w') as f:
            #         for cik in all_formatted_ciks:
            #             f.write(cik + '\n')
            #     logging.info(f"Saved formatted CIKs to {output_cik_file}")
            # except OSError as e:
            #     logging.error(f"Failed to save CIK list: {e}")

        else:
            logging.warning("No CIKs were extracted.")
    else:
        logging.error("Could not load ticker data. Cannot proceed.")

    logging.info("--- CIK Preparation Script Finished ---")