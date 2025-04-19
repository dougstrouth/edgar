# -*- coding: utf-8 -*-
"""
EDGAR Data Parser Script (Refactored for Utilities)

Handles various JSON parsing tasks for SEC EDGAR data:
- Parses company_tickers.json to prepare formatted CIKs for API use.
- Parses CIK-specific JSON files from 'submissions.zip' extracts
  to structure company metadata and filing info for DB tables.
- Parses CIK-specific JSON files from 'companyfacts.zip' extracts
  to structure XBRL tag definitions and fact data (with numeric/text split)
  for DB tables.

Uses logging_utils for standardized logging.
Core functions now expect necessary paths as arguments rather than reading
from .env directly. Example usage block demonstrates loading config.
"""

import json
import os
import sys
import logging # Keep for level constants
import math
from pathlib import Path
# from dotenv import load_dotenv # No longer needed here
from typing import Dict, List, Any, Optional, Union, Tuple,Set # Added Tuple
from datetime import datetime, date, timezone

# --- Import Utilities ---
from logging_utils import setup_logging
# config_utils is imported only in the __main__ block for example usage

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants (Removed paths derived from .env) ---
# API URL templates remain useful
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK_PAD}.json"
COMPANY_CONCEPT_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK_PAD}/{taxonomy}/{tag}.json"

# --- Helper Functions for Safe Date Parsing (Using logger) ---
def parse_datetime_string(dt_str: Optional[str]) -> Optional[datetime]:
    """Safely parse ISO 8601 format datetime strings, handling None and errors."""
    if not dt_str: return None
    try:
        # Basic cleaning before parsing
        if isinstance(dt_str, (int, float)): dt_str = str(dt_str) # Handle potential numeric types
        if not isinstance(dt_str, str): raise TypeError("Input must be a string")
        if '.' in dt_str: dt_str = dt_str.split('.')[0] # Remove fractional seconds
        if dt_str.endswith('Z'): dt_str = dt_str[:-1] + '+00:00' # Handle Zulu time

        dt = datetime.fromisoformat(dt_str)
        # Ensure timezone-aware UTC
        if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse datetime string '{dt_str}': {e}")
        return None

def parse_date_string(date_str: Optional[str]) -> Optional[date]:
    """Safely parse YYYY-MM-DD date strings."""
    if not date_str: return None
    try:
        if isinstance(date_str, datetime): return date_str.date() # Handle if already datetime
        if not isinstance(date_str, str): raise TypeError("Input must be a string")
        return date.fromisoformat(date_str)
    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse date string '{date_str}': {e}")
        return None

# --- Functions for CIK Preparation (Using logger) ---

def load_ticker_data(ticker_file_path: Path) -> Optional[Dict]:
    """
    Loads the JSON data from the specified company_tickers file path.

    Args:
        ticker_file_path: The Path object pointing to the company_tickers.json file.

    Returns:
        A dictionary containing the loaded ticker data, or None on failure.
    """
    if not ticker_file_path.is_file():
        logger.error(f"Ticker file not found: {ticker_file_path}")
        return None
    try:
        with open(ticker_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded ticker data from {ticker_file_path.name}")
        return data
    except Exception as e:
        logger.error(f"Error loading/parsing {ticker_file_path.name}: {e}", exc_info=True)
        return None

def extract_formatted_ciks(ticker_data: Dict) -> List[str]:
    """Extracts CIKs and formats them (zero-padded to 10 digits)."""
    formatted_ciks = []
    if not ticker_data or not isinstance(ticker_data, dict):
        logger.warning("Invalid or empty ticker data provided for CIK extraction.")
        return []
    # Ticker data structure changed slightly (values are dicts now)
    for key, company_info in ticker_data.items():
        if isinstance(company_info, dict) and 'cik_str' in company_info:
            try:
                cik_raw = str(company_info['cik_str'])
                cik_padded = cik_raw.zfill(10)
                formatted_ciks.append(cik_padded)
            except Exception as e:
                logger.warning(f"Could not process ticker entry key '{key}': {company_info}. Error: {e}")
        # Original structure check (might be needed for older files?)
        # elif isinstance(company_info, list) and len(company_info) > 0 and 'cik_str' in company_info[0]:
        #     # Handle potential list structure if observed
        #     try:
        #         cik_raw = str(company_info[0]['cik_str'])
        #         cik_padded = cik_raw.zfill(10)
        #         formatted_ciks.append(cik_padded)
        #     except Exception as e:
        #          logger.warning(f"Could not process ticker list entry {key}: {company_info}. Error: {e}")

    unique_ciks = sorted(list(set(formatted_ciks)))
    logger.info(f"Extracted {len(formatted_ciks)} CIKs, {len(unique_ciks)} unique.")
    return unique_ciks

def generate_api_url(template: str, cik: str, **kwargs) -> str:
    """Generates an API URL using a template and parameters."""
    url = template.replace("{CIK_PAD}", cik)
    for key, value in kwargs.items():
         url = url.replace("{" + key + "}", str(value))
    return url

# --- Function for Parsing Submission JSONs (Using logger) ---

def parse_submission_json_for_db(file_path: Path) -> Optional[Dict[str, Union[Dict, List[Dict]]]]:
    """
    Loads and parses a CIK-specific submission JSON file (from submissions.zip),
    structuring output for companies, tickers, former_names, filings tables.
    Ensures basic string types where appropriate. Uses module logger.
    """
    if not file_path.is_file():
        logger.error(f"Submission JSON not found: {file_path}")
        return None
    logger.info(f"Parsing submission JSON for DB: {file_path.name}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    except Exception as e:
        logger.error(f"Failed load/decode submission JSON {file_path.name}: {e}", exc_info=True)
        return None
    if not isinstance(data, dict) or 'cik' not in data:
        logger.error(f"Invalid structure in submission JSON: {file_path.name}")
        return None

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
            "entity_name_cf": None,
            "entity_type": str(data['entityType']) if data.get('entityType') else None,
            "sic": str(data['sic']) if data.get('sic') else None,
            "sic_description": str(data['sicDescription']) if data.get('sicDescription') else None,
            "ein": str(data['ein']) if data.get('ein') else None,
            "description": str(data['description']) if data.get('description') else None,
            "category": str(data['category']) if data.get('category') else None,
            "fiscal_year_end": str(data['fiscalYearEnd']) if data.get('fiscalYearEnd') else None,
            "state_of_incorporation": str(data['stateOfIncorporation']) if data.get('stateOfIncorporation') else None,
            "phone": str(data['phone']) if data.get('phone') else None,
            "flags": str(data['flags']) if data.get('flags') else None,
            "last_parsed_timestamp": now_ts
        }
        addresses = data.get('addresses', {})
        if isinstance(addresses, dict):
             mailing = addresses.get('mailing', {}) or {}
             business = addresses.get('business', {}) or {}
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
                  ticker_symbol = tickers[i]; exchange_name = exchanges[i]
                  if ticker_symbol and isinstance(ticker_symbol, str) and exchange_name and isinstance(exchange_name, str):
                       parsed_db_data["tickers"].append({
                           "cik": cik_padded, "ticker": str(ticker_symbol),
                           "exchange": str(exchange_name), "source": "submissions.json"
                       })
                  else: logger.debug(f"Skipping ticker entry CIK {cik_padded}: Ticker='{ticker_symbol}', Exchange='{exchange_name}'")

        # --- 3. Populate 'former_names' data ---
        former_names_raw = data.get('formerNames', [])
        if isinstance(former_names_raw, list):
             for fn in former_names_raw:
                  if isinstance(fn, dict) and fn.get('name'):
                       parsed_db_data["former_names"].append({
                           "cik": cik_padded, "former_name": str(fn['name']),
                           "date_from": parse_datetime_string(fn.get('from')),
                           "date_to": parse_datetime_string(fn.get('to'))
                       })

        # --- 4. Populate 'filings' data ---
        if 'filings' in data and isinstance(data['filings'], dict) and 'recent' in data['filings'] and isinstance(data['filings']['recent'], dict):
            recent = data['filings']['recent']
            # Ensure all expected filing field arrays exist and have same length
            field_names = list(recent.keys())
            if not field_names or not all(isinstance(recent[f], list) for f in field_names):
                logger.warning(f"Malformed 'filings.recent' structure in {file_path.name}. Skipping filings.")
            else:
                try:
                    num_filings = len(recent[field_names[0]]) # Use length of first field
                    if not all(len(recent[f]) == num_filings for f in field_names):
                        logger.warning(f"Inconsistent array lengths in 'filings.recent' for {file_path.name}. Skipping filings.")
                    else:
                        for i in range(num_filings):
                            filing_entry = {"cik": cik_padded}; valid_entry = True
                            map_convert = { # Map JSON keys to DB columns and converter functions
                                'accessionNumber': ('accession_number', str), 'filingDate': ('filing_date', parse_date_string),
                                'reportDate': ('report_date', parse_date_string), 'acceptanceDateTime': ('acceptance_datetime', parse_datetime_string),
                                'act': ('act', str), 'form': ('form', str), 'fileNumber': ('file_number', str),
                                'filmNumber': ('film_number', str), 'items': ('items', str),
                                'size': ('size', lambda x: int(x) if x is not None and str(x).isdigit() else None),
                                'isXBRL': ('is_xbrl', lambda x: bool(x) if x is not None else None),
                                'isInlineXBRL': ('is_inline_xbrl', lambda x: bool(x) if x is not None else None),
                                'primaryDocument': ('primary_document', str), 'primaryDocDescription': ('primary_doc_description', str),
                            }
                            for json_key, (db_key, converter) in map_convert.items():
                                if json_key in recent:
                                     raw_value = recent[json_key][i]
                                     # Ensure converter gets string for str types, handle None
                                     if converter == str: value = str(raw_value) if raw_value is not None else None
                                     else: value = converter(raw_value)
                                     filing_entry[db_key] = value
                                else: filing_entry[db_key] = None # Field missing in JSON source

                            # Check required fields after conversion
                            for req_k in ['accession_number', 'form', 'filing_date', 'cik']:
                                if req_k not in filing_entry or filing_entry[req_k] is None:
                                    logger.warning(f"Missing required field '{req_k}' for filing index {i} in {file_path.name}. Skipping.")
                                    valid_entry = False; break
                            if valid_entry: parsed_db_data["filings"].append(filing_entry)
                except IndexError:
                     logger.warning(f"Index error processing filings in {file_path.name}, likely inconsistent lengths detected late.")
                except Exception as e_filing:
                     logger.error(f"Error processing filings structure in {file_path.name}: {e_filing}", exc_info=True)

        else: logger.warning(f"No valid 'filings.recent' data found in {file_path.name}")

        logger.info(f"Structured submission data for DB from {file_path.name}")
        return parsed_db_data

    except Exception as e:
        logger.error(f"Critical error structuring submission data CIK {data.get('cik', 'N/A')} from {file_path.name}: {e}", exc_info=True)
        return None


# --- Function for Parsing CompanyFacts JSONs (Using logger) ---

# --- Function for Parsing CompanyFacts JSONs (Using logger) ---

def parse_company_facts_json_for_db(file_path: Path) -> Optional[Dict[str, Union[Optional[str], List[Dict]]]]:
    """
    Loads and parses a CIK-specific company facts JSON file (from companyfacts.zip),
    structuring output for xbrl_tags and xbrl_facts tables.
    Handles splitting numeric/text values for xbrl_facts. Uses module logger.
    """
    if not file_path.is_file():
        logger.error(f"Company facts JSON file not found: {file_path}")
        return None
    logger.info(f"Parsing company facts JSON for DB: {file_path.name}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    except Exception as e:
        logger.error(f"Failed load/decode company facts JSON {file_path.name}: {e}", exc_info=True)
        return None
    if not isinstance(data, dict) or 'cik' not in data or 'facts' not in data:
        logger.error(f"Invalid structure in company facts JSON: {file_path.name}")
        return None

    parsed_db_data: Dict[str, Union[Optional[str], List[Dict]]] = {
        "company_entity_name": str(data['entityName']) if data.get("entityName") else None,
        "xbrl_tags": [],
        "xbrl_facts": []
    }
    unique_tags: Set[Tuple[str, str]] = set()

    try:
        cik_padded = str(data.get('cik')).zfill(10)
        facts_data = data.get('facts', {})
        if not isinstance(facts_data, dict):
             logger.warning(f"Facts data is not a dictionary in {file_path.name}"); return parsed_db_data

        for taxonomy_key, tags in facts_data.items():
            taxonomy = str(taxonomy_key)
            if not isinstance(tags, dict): continue
            for tag_name_key, tag_details in tags.items():
                tag_name = str(tag_name_key)
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
                    unit = str(unit_key)
                    if not isinstance(fact_instances, list): continue
                    for fact in fact_instances:
                        if not isinstance(fact, dict): continue

                        raw_value = fact.get('val')
                        value_numeric: Optional[float] = None
                        value_text: Optional[str] = None

                        # --- CORRECTED try...except block ---
                        if raw_value is not None:
                            try:
                                # Attempt conversion to float
                                value_numeric = float(raw_value)
                                # Check for non-finite numbers (inf, -inf, NaN)
                                if not math.isfinite(value_numeric):
                                    value_text = str(raw_value) # Store original string
                                    value_numeric = None      # Set numeric to None
                                else:
                                    # It's a valid finite number
                                    value_text = None # No need for text version
                            except (ValueError, TypeError):
                                # Conversion failed, treat as text
                                value_numeric = None
                                value_text = str(raw_value)

                            # Final check: Ensure text is populated if numeric is None
                            # (This handles edge cases and the non-finite case above)
                            if value_numeric is None and value_text is None:
                                value_text = str(raw_value)
                        # --- End CORRECTION ---

                        accession_number = str(fact['accn']) if fact.get('accn') else None
                        fy_val = fact.get('fy'); fp_val = fact.get('fp'); form_val = fact.get('form'); frame_val = fact.get('frame')

                        fact_record = {
                            "cik": cik_padded, "taxonomy": taxonomy, "tag_name": tag_name,
                            "accession_number": accession_number, "unit": unit,
                            "period_start_date": parse_date_string(fact.get('start')),
                            "period_end_date": parse_date_string(fact.get('end')),
                            "value_numeric": value_numeric, "value_text": value_text,
                            "fy": int(fy_val) if fy_val is not None and isinstance(fy_val, (int, float)) or (isinstance(fy_val, str) and fy_val.isdigit()) else None,
                            "fp": str(fp_val) if fp_val is not None else None,
                            "form": str(form_val) if form_val is not None else None,
                            "filed_date": parse_date_string(fact.get('filed')),
                            "frame": str(frame_val) if frame_val is not None else None
                        }
                        if not fact_record["accession_number"]: logger.debug(f"Skip fact missing accn: CIK {cik_padded}, Tag {taxonomy}:{tag_name}, End {fact.get('end')}"); continue
                        if not fact_record["form"]: logger.debug(f"Skip fact missing form: CIK {cik_padded}, Tag {taxonomy}:{tag_name}, Accn {accession_number}"); continue

                        parsed_db_data["xbrl_facts"].append(fact_record)

        logger.info(f"Structured facts data: {len(parsed_db_data['xbrl_tags'])} unique tags, {len(parsed_db_data['xbrl_facts'])} facts for CIK {cik_padded}.")
        return parsed_db_data

    except Exception as e:
        logger.error(f"Critical error structuring facts data CIK {data.get('cik', 'N/A')} from {file_path.name}: {e}", exc_info=True)
        return None

# --- Main Execution (Example Usage using Config) ---
if __name__ == "__main__":
    logger.info(f"--- Running EDGAR Data Parser (Example Mode) ---")

    # --- Load Config for Example Paths ---
    try:
        # config_utils needed only for example run to get paths
        from config_utils import AppConfig
        # Assume .env is relative to this script file for example run
        config = AppConfig(calling_script_path=Path(__file__))
        TICKER_FILE_PATH = config.TICKER_FILE_PATH
        EXAMPLE_SUBMISSIONS_DIR = config.SUBMISSIONS_DIR
        EXAMPLE_COMPANYFACTS_DIR = config.COMPANYFACTS_DIR
        EXAMPLE_SUBMISSIONS_CIK = "CIK0000320193" # Example: Apple CIK
        EXAMPLE_COMPANYFACTS_CIK = "CIK0000320193" # Example: Apple CIK
        EXAMPLE_SUBMISSION_JSON_PATH = EXAMPLE_SUBMISSIONS_DIR / f"{EXAMPLE_SUBMISSIONS_CIK}.json"
        EXAMPLE_COMPANYFACTS_JSON_PATH = EXAMPLE_COMPANYFACTS_DIR / f"{EXAMPLE_COMPANYFACTS_CIK}.json"
        config_loaded = True
    except SystemExit as e:
        logger.critical(f"Config failed for example: {e}. Cannot run examples.")
        config_loaded = False
    except Exception as e:
        logger.critical(f"Unexpected error loading config for example: {e}", exc_info=True)
        config_loaded = False


    if config_loaded:
        # --- Task 1: Prepare CIKs ---
        logger.info("\n--- Task: Prepare CIKs ---")
        # Pass the path from config to the function
        ticker_json_data = load_ticker_data(TICKER_FILE_PATH)
        all_formatted_ciks = []
        if ticker_json_data:
            all_formatted_ciks = extract_formatted_ciks(ticker_json_data)
            if all_formatted_ciks: logger.info(f"Total unique formatted CIKs prepared: {len(all_formatted_ciks)}")
            else: logger.warning("No CIKs were extracted from ticker file.")
        else: logger.error("Could not load ticker data.")

        # --- Task 2: Parse Example Submission JSON ---
        logger.info("\n--- Task: Parse Example Submission JSON for DB ---")
        if EXAMPLE_SUBMISSION_JSON_PATH.is_file():
            # Pass path to function
            parsed_submission_data = parse_submission_json_for_db(EXAMPLE_SUBMISSION_JSON_PATH)
            if parsed_submission_data: logger.info(f"Example Parsed Submission Data Structure (Keys: {list(parsed_submission_data.keys())})")
            else: logger.error(f"Failed parse/structure: {EXAMPLE_SUBMISSION_JSON_PATH.name}")
        else: logger.warning(f"Example submission JSON not found: {EXAMPLE_SUBMISSION_JSON_PATH}. Skipping.")

        # --- Task 3: Parse Example CompanyFacts JSON ---
        logger.info("\n--- Task: Parse Example CompanyFacts JSON for DB ---")
        if EXAMPLE_COMPANYFACTS_JSON_PATH.is_file():
             # Pass path to function
            parsed_facts_data = parse_company_facts_json_for_db(EXAMPLE_COMPANYFACTS_JSON_PATH)
            if parsed_facts_data: logger.info(f"Example Parsed CompanyFacts Data Structure (Keys: {list(parsed_facts_data.keys())})")
            else: logger.error(f"Failed parse/structure: {EXAMPLE_COMPANYFACTS_JSON_PATH.name}")
        else: logger.warning(f"Example companyfacts JSON not found: {EXAMPLE_COMPANYFACTS_JSON_PATH}. Skipping.")

    logger.info("\n--- NOTE: This script only parses. Run edgar_data_loader.py to load to DB. ---")
    logger.info("--- EDGAR Data Parser Finished (Example Mode) ---")