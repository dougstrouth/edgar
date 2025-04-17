# -*- coding: utf-8 -*-
"""
EDGAR Data Parser Script (e.g., save as edgar_data_parser.py)

Handles various JSON parsing tasks for SEC EDGAR data:
- Parses company_tickers.json to prepare formatted CIKs for API use.
- Parses CIK-specific JSON files from 'submissions.zip' extracts
  to structure company metadata and filing info for DB tables.
- Parses CIK-specific JSON files from 'companyfacts.zip' extracts
  to structure XBRL tag definitions and fact data (with numeric/text split)
  for DB tables.

Reads configuration (DOWNLOAD_DIR) from a .env file.
Prepares data structures suitable for loading into a relational DB (DuckDB).
"""

import json
import os
import sys
import logging
import math # For checking float validity if needed
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date, timezone

# --- Configuration ---

# Load environment variables from .env file relative to this script's location
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'

if not dotenv_path.is_file():
    print(f"ERROR: .env file not found at {dotenv_path}", file=sys.stderr)
    print("Please ensure a .env file exists with DOWNLOAD_DIR defined.", file=sys.stderr)
    sys.exit(1)

load_dotenv(dotenv_path=dotenv_path)

# Get DOWNLOAD_DIR from environment variables
try:
    DOWNLOAD_DIR = Path(os.environ['DOWNLOAD_DIR']).resolve()
    EXTRACT_BASE_DIR = DOWNLOAD_DIR / "extracted_json"
    TICKER_FILE_NAME = "company_tickers.json"
    TICKER_FILE_PATH = DOWNLOAD_DIR / TICKER_FILE_NAME
    EXAMPLE_SUBMISSIONS_CIK = "CIK0000001750" # Example CIK from submissions
    EXAMPLE_COMPANYFACTS_CIK = "CIK0000001750" # Example CIK from companyfacts
    EXAMPLE_SUBMISSION_JSON_PATH = EXTRACT_BASE_DIR / "submissions" / f"{EXAMPLE_SUBMISSIONS_CIK}.json"
    EXAMPLE_COMPANYFACTS_JSON_PATH = EXTRACT_BASE_DIR / "companyfacts" / f"{EXAMPLE_COMPANYFACTS_CIK}.json"
except KeyError as e:
    print(f"ERROR: Missing required environment variable in .env file: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
     print(f"Error configuring paths: {e}", file=sys.stderr)
     sys.exit(1)


# --- Logging Setup ---
log_file_path = script_dir / "edgar_data_parser.log" # Consolidated log file name
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logging.info(f"--- Parser Script Start ---") # Indicate which script log belongs to
logging.info(f"Using DOWNLOAD_DIR: {DOWNLOAD_DIR}")
logging.info(f"Using EXTRACT_BASE_DIR: {EXTRACT_BASE_DIR}")
logging.info(f"Using Ticker File: {TICKER_FILE_PATH}")


# --- Constants ---
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK_PAD}.json"
COMPANY_CONCEPT_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK_PAD}/{taxonomy}/{tag}.json"


# --- Helper Functions for Safe Date Parsing ---
def parse_datetime_string(dt_str: Optional[str]) -> Optional[datetime]:
    """Safely parse ISO 8601 format datetime strings, handling None and errors."""
    if not dt_str: return None
    try:
        if '.' in dt_str: dt_str = dt_str.split('.')[0]
        if dt_str.endswith('Z'): dt_str = dt_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse datetime string '{dt_str}': {e}")
        return None

def parse_date_string(date_str: Optional[str]) -> Optional[date]:
    """Safely parse YYYY-MM-DD date strings."""
    if not date_str: return None
    try:
        return date.fromisoformat(date_str)
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse date string '{date_str}': {e}")
        return None

# --- Functions for CIK Preparation ---

def load_ticker_data(file_path: Path) -> Optional[Dict]:
    """Loads the JSON data from the company_tickers file."""
    if not file_path.is_file(): logging.error(f"Ticker file not found: {file_path}"); return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        logging.info(f"Successfully loaded ticker data from {file_path.name}")
        return data
    except Exception as e:
        logging.error(f"Error loading/parsing {file_path.name}: {e}", exc_info=True)
        return None

def extract_formatted_ciks(ticker_data: Dict) -> List[str]:
    """Extracts CIKs and formats them (zero-padded to 10 digits)."""
    formatted_ciks = []
    if not ticker_data or not isinstance(ticker_data, dict):
        logging.warning("Invalid or empty ticker data provided for CIK extraction.")
        return []
    for key, company_info in ticker_data.items():
        if isinstance(company_info, dict) and 'cik_str' in company_info:
            try:
                cik_raw = str(company_info['cik_str'])
                cik_padded = cik_raw.zfill(10)
                formatted_ciks.append(cik_padded)
            except Exception as e:
                logging.warning(f"Could not process ticker entry {key}: {company_info}. Error: {e}")
    unique_ciks = sorted(list(set(formatted_ciks)))
    logging.info(f"Extracted {len(formatted_ciks)} CIKs, {len(unique_ciks)} unique.")
    return unique_ciks

def generate_api_url(template: str, cik: str, **kwargs) -> str:
    """Generates an API URL using a template and parameters."""
    url = template.replace("{CIK_PAD}", cik)
    for key, value in kwargs.items():
         url = url.replace("{" + key + "}", str(value))
    return url

# --- Function for Parsing Submission JSONs ('submissions.zip' content) ---

def parse_submission_json_for_db(file_path: Path) -> Optional[Dict[str, Union[Dict, List[Dict]]]]:
    """
    Loads and parses a CIK-specific submission JSON file (from submissions.zip),
    structuring output for companies, tickers, former_names, filings tables.
    Ensures basic string types where appropriate.
    """
    if not file_path.is_file(): logging.error(f"Submission JSON not found: {file_path}"); return None
    logging.info(f"Parsing submission JSON for DB: {file_path.name}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    except Exception as e: logging.error(f"Failed load/decode: {e}"); return None
    if not isinstance(data, dict) or 'cik' not in data: logging.error(f"Invalid structure: {file_path.name}"); return None

    parsed_db_data: Dict[str, Union[Dict, List[Dict]]] = {
        "companies": {}, "tickers": [], "former_names": [], "filings": []
    }

    try:
        cik_padded = str(data.get('cik')).zfill(10)
        now_ts = datetime.now(timezone.utc)

        # --- 1. Populate 'companies' data ---
        company_record = {
            "cik": cik_padded,
            "primary_name": str(data['name']) if data.get('name') else None,
            "entity_name_cf": None, # This comes from companyfacts
            "entity_type": str(data['entityType']) if data.get('entityType') else None,
            "sic": str(data['sic']) if data.get('sic') else None,
            "sic_description": str(data['sicDescription']) if data.get('sicDescription') else None,
            "ein": str(data['ein']) if data.get('ein') else None,
            "description": str(data['description']) if data.get('description') else None,
            "category": str(data['category']) if data.get('category') else None,
            "fiscal_year_end": str(data['fiscalYearEnd']) if data.get('fiscalYearEnd') else None,
            "state_of_incorporation": str(data['stateOfIncorporation']) if data.get('stateOfIncorporation') else None,
            "phone": str(data['phone']) if data.get('phone') else None,
            "flags": str(data['flags']) if data.get('flags') else None, # flags might be complex, ensure string
            "last_parsed_timestamp": now_ts
        }
        addresses = data.get('addresses', {})
        if isinstance(addresses, dict):
             mailing = addresses.get('mailing', {}) or {}
             business = addresses.get('business', {}) or {}
             # Explicitly convert address fields to string, handling None
             company_record.update({
                 k: str(v) if v is not None else None for k, v in {
                     "mailing_street1": mailing.get('street1'), "mailing_street2": mailing.get('street2'),
                     "mailing_city": mailing.get('city'), "mailing_state_or_country": mailing.get('stateOrCountry'),
                     "mailing_zip_code": mailing.get('zipCode'),
                     "business_street1": business.get('street1'), "business_street2": business.get('street2'),
                     "business_city": business.get('city'), "business_state_or_country": business.get('stateOrCountry'),
                     "business_zip_code": business.get('zipCode'),
                 }.items()
             })
        parsed_db_data["companies"] = company_record

        # --- 2. Populate 'tickers' data ---
        tickers = data.get('tickers', [])
        exchanges = data.get('exchanges', [])
        if isinstance(tickers, list) and isinstance(exchanges, list):
             num_tickers = min(len(tickers), len(exchanges))
             for i in range(num_tickers):
                  ticker_symbol = tickers[i]
                  exchange_name = exchanges[i]
                  # Check if both are non-empty strings
                  if ticker_symbol and isinstance(ticker_symbol, str) and \
                     exchange_name and isinstance(exchange_name, str):
                       parsed_db_data["tickers"].append({
                           "cik": cik_padded,
                           "ticker": str(ticker_symbol), # Ensure string
                           "exchange": str(exchange_name), # Ensure string
                           "source": "submissions.json"
                       })
                  else:
                       logging.debug(f"Skipping ticker entry for CIK {cik_padded} due to missing/empty ticker or exchange: Ticker='{ticker_symbol}', Exchange='{exchange_name}'")

        # --- 3. Populate 'former_names' data ---
        former_names_raw = data.get('formerNames', [])
        if isinstance(former_names_raw, list):
             for fn in former_names_raw:
                  if isinstance(fn, dict) and fn.get('name'):
                       parsed_db_data["former_names"].append({
                           "cik": cik_padded,
                           "former_name": str(fn['name']), # Ensure string
                           "date_from": parse_datetime_string(fn.get('from')),
                           "date_to": parse_datetime_string(fn.get('to'))
                       })

        # --- 4. Populate 'filings' data ---
        if 'filings' in data and isinstance(data['filings'], dict) and \
           'recent' in data['filings'] and isinstance(data['filings']['recent'], dict):
            recent = data['filings']['recent']
            keys = list(recent.keys())
            num_filings = len(recent.get('accessionNumber', []))
            for i in range(num_filings):
                filing_entry = {"cik": cik_padded}
                valid_entry = True
                for key in keys:
                    target_key, value = key, None # Default
                    if key in recent and isinstance(recent[key], list) and i < len(recent[key]):
                        raw_value = recent[key][i]
                        # Map JSON keys to DB columns and convert types
                        map_convert = {
                            'accessionNumber': ('accession_number', str),
                            'filingDate': ('filing_date', parse_date_string),
                            'reportDate': ('report_date', parse_date_string),
                            'acceptanceDateTime': ('acceptance_datetime', parse_datetime_string),
                            'act': ('act', str),
                            'form': ('form', str),
                            'fileNumber': ('file_number', str),
                            'filmNumber': ('film_number', str),
                            'items': ('items', str),
                            'size': ('size', lambda x: int(x) if x is not None and str(x).isdigit() else None), # Safer int conversion
                            'isXBRL': ('is_xbrl', lambda x: bool(x) if x is not None else None),
                            'isInlineXBRL': ('is_inline_xbrl', lambda x: bool(x) if x is not None else None),
                            'primaryDocument': ('primary_document', str),
                            'primaryDocDescription': ('primary_doc_description', str),
                        }
                        if key in map_convert:
                            target_key, converter = map_convert[key]
                            # Ensure converter gets string representation for str types
                            if converter == str:
                                value = str(raw_value) if raw_value is not None else None
                            else:
                                value = converter(raw_value)
                        else:
                            # Should not happen if map covers all keys, but handle defensively
                            target_key = key
                            value = str(raw_value) if raw_value is not None else None

                        filing_entry[target_key] = value

                # Ensure essential keys are present and have non-None values after conversion
                for req_k in ['accession_number', 'form', 'filing_date', 'cik']:
                    if req_k not in filing_entry or filing_entry[req_k] is None:
                        logging.warning(f"Missing required filing field '{req_k}' for index {i} in {file_path.name}. Skipping filing record.")
                        valid_entry = False
                        break
                if valid_entry:
                    parsed_db_data["filings"].append(filing_entry)
        else:
            logging.warning(f"No 'filings.recent' data found in {file_path.name}")

        logging.info(f"Structured submission data for DB from {file_path.name}")
        return parsed_db_data

    except Exception as e:
        logging.error(f"Critical error structuring submission data for CIK {data.get('cik', 'N/A')}: {e}", exc_info=True)
        return None


# --- Function for Parsing CompanyFacts JSONs ('companyfacts.zip' content) ---

def parse_company_facts_json_for_db(file_path: Path) -> Optional[Dict[str, Union[Optional[str], List[Dict]]]]:
    """
    Loads and parses a CIK-specific company facts JSON file (from companyfacts.zip),
    structuring output for xbrl_tags and xbrl_facts tables.
    Handles splitting numeric/text values for xbrl_facts.
    Ensures basic string types where appropriate.
    """
    if not file_path.is_file(): logging.error(f"Company facts JSON file not found: {file_path}"); return None
    logging.info(f"Parsing company facts JSON for DB: {file_path.name}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    except Exception as e: logging.error(f"Failed load/decode: {e}"); return None
    if not isinstance(data, dict) or 'cik' not in data or 'facts' not in data: logging.error(f"Invalid structure: {file_path.name}"); return None

    parsed_db_data: Dict[str, Union[Optional[str], List[Dict]]] = {
        "company_entity_name": str(data['entityName']) if data.get("entityName") else None,
        "xbrl_tags": [],
        "xbrl_facts": []
    }
    unique_tags = set()

    try:
        cik_padded = str(data.get('cik')).zfill(10)
        facts_data = data.get('facts', {})
        if not isinstance(facts_data, dict): return parsed_db_data

        for taxonomy_key, tags in facts_data.items():
            taxonomy = str(taxonomy_key) # Ensure taxonomy is string
            if not isinstance(tags, dict): continue
            for tag_name_key, tag_details in tags.items():
                tag_name = str(tag_name_key) # Ensure tag_name is string
                if not isinstance(tag_details, dict): continue

                label = str(tag_details['label']) if tag_details.get('label') else None
                description = str(tag_details['description']) if tag_details.get('description') else None
                tag_composite_key = (taxonomy, tag_name)

                # --- 1. Populate 'xbrl_tags' data ---
                if tag_composite_key not in unique_tags:
                     parsed_db_data["xbrl_tags"].append({
                         "taxonomy": taxonomy, "tag_name": tag_name,
                         "label": label, "description": description
                     })
                     unique_tags.add(tag_composite_key)

                # --- 2. Populate 'xbrl_facts' data ---
                units_data = tag_details.get('units')
                if not isinstance(units_data, dict): continue
                for unit_key, fact_instances in units_data.items():
                    unit = str(unit_key) # *** Ensure unit is string ***
                    if not isinstance(fact_instances, list): continue
                    for fact in fact_instances:
                        if not isinstance(fact, dict): continue

                        raw_value = fact.get('val')
                        value_numeric: Optional[float] = None
                        value_text: Optional[str] = None

                        # Handle numeric/text split
                        if raw_value is not None:
                            try:
                                value_numeric = float(raw_value)
                                if not math.isfinite(value_numeric):
                                    value_numeric = None
                                    value_text = str(raw_value)
                            except (ValueError, TypeError):
                                value_text = str(raw_value)
                            if value_numeric is None and value_text is None:
                                value_text = str(raw_value)

                        # Get and ensure string types for other fields
                        accession_number = str(fact['accn']) if fact.get('accn') else None
                        fy_val = fact.get('fy')
                        fp_val = fact.get('fp')
                        form_val = fact.get('form')
                        frame_val = fact.get('frame')

                        fact_record = {
                            "cik": cik_padded,
                            "taxonomy": taxonomy,
                            "tag_name": tag_name,
                            "accession_number": accession_number,
                            "unit": unit, # Already converted to string
                            "period_start_date": parse_date_string(fact.get('start')),
                            "period_end_date": parse_date_string(fact.get('end')),
                            "value_numeric": value_numeric,
                            "value_text": value_text,
                            "fy": int(fy_val) if fy_val is not None and str(fy_val).isdigit() else None,
                            "fp": str(fp_val) if fp_val is not None else None,
                            "form": str(form_val) if form_val is not None else None,
                            "filed_date": parse_date_string(fact.get('filed')),
                            "frame": str(frame_val) if frame_val is not None else None
                        }
                        # Filter out facts missing essential linking info
                        if not fact_record["accession_number"]:
                             logging.debug(f"Skipping fact with missing accession number: CIK {cik_padded}, Tag {taxonomy}:{tag_name}, End {fact.get('end')}")
                             continue
                        if not fact_record["form"]: # Also require form
                             logging.debug(f"Skipping fact with missing form: CIK {cik_padded}, Tag {taxonomy}:{tag_name}, Accn {accession_number}")
                             continue

                        parsed_db_data["xbrl_facts"].append(fact_record)

        logging.info(f"Structured facts data: {len(parsed_db_data['xbrl_tags'])} unique tags, {len(parsed_db_data['xbrl_facts'])} facts for CIK {cik_padded}.")
        return parsed_db_data

    except Exception as e:
        logging.error(f"Critical error structuring facts data for CIK {data.get('cik', 'N/A')}: {e}", exc_info=True)
        return None


# --- Main Execution (Example Usage) ---
if __name__ == "__main__":
    logging.info(f"--- Running EDGAR Data Parser (Example Mode) ---")

    # --- Task 1: Prepare CIKs ---
    logging.info("\n--- Task: Prepare CIKs ---")
    ticker_json_data = load_ticker_data(TICKER_FILE_PATH)
    all_formatted_ciks = []
    if ticker_json_data:
        all_formatted_ciks = extract_formatted_ciks(ticker_json_data)
        if all_formatted_ciks:
             logging.info(f"Total unique formatted CIKs prepared: {len(all_formatted_ciks)}")
             # logging.info("First 5 formatted CIKs: {}".format(", ".join(all_formatted_ciks[:5])))
        else: logging.warning("No CIKs were extracted from ticker file.")
    else: logging.error("Could not load ticker data.")


    # --- Task 2: Parse Example Submission JSON ---
    logging.info("\n--- Task: Parse Example Submission JSON for DB ---")
    if EXAMPLE_SUBMISSION_JSON_PATH.is_file():
        parsed_submission_data = parse_submission_json_for_db(EXAMPLE_SUBMISSION_JSON_PATH)
        if parsed_submission_data:
             logging.info(f"Example Parsed Submission Data Structure (Keys: {list(parsed_submission_data.keys())})")
             # print("\nSample Submission Company Data:\n", json.dumps(parsed_submission_data.get("companies", {}), indent=2, default=str))
             # filings_data_sub = parsed_submission_data.get("filings", [])
             # if filings_data_sub: print("\nSample Submission First Filing Data:\n", json.dumps(filings_data_sub[0], indent=2, default=str))
        else: logging.error(f"Failed parse/structure: {EXAMPLE_SUBMISSION_JSON_PATH.name}")
    else: logging.warning(f"Example submission JSON not found at {EXAMPLE_SUBMISSION_JSON_PATH}.")


    # --- Task 3: Parse Example CompanyFacts JSON ---
    logging.info("\n--- Task: Parse Example CompanyFacts JSON for DB ---")
    if EXAMPLE_COMPANYFACTS_JSON_PATH.is_file():
        parsed_facts_data = parse_company_facts_json_for_db(EXAMPLE_COMPANYFACTS_JSON_PATH)
        if parsed_facts_data:
             logging.info(f"Example Parsed CompanyFacts Data Structure (Keys: {list(parsed_facts_data.keys())})")
             # print(f"\nSample CompanyFacts Entity Name: {parsed_facts_data.get('company_entity_name')}")
             # tags_data = parsed_facts_data.get("xbrl_tags", [])
             # if tags_data: print("\nSample First Unique Tag Data:\n", json.dumps(tags_data[0], indent=2))
             # facts_data = parsed_facts_data.get("xbrl_facts", [])
             # if facts_data: print("\nSample First Fact Data:\n", json.dumps(facts_data[0], indent=2, default=str))
        else: logging.error(f"Failed parse/structure: {EXAMPLE_COMPANYFACTS_JSON_PATH.name}")
    else: logging.warning(f"Example companyfacts JSON not found at {EXAMPLE_COMPANYFACTS_JSON_PATH}.")


    logging.info("\n--- NOTE: This script only parses. Run edgar_data_loader.py to load to DB. ---")
    logging.info("--- EDGAR Data Parser Finished (Example Mode) ---")