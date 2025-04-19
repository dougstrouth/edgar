# -*- coding: utf-8 -*-
"""
Downloads SEC EDGAR bulk data files (ZIPs and Ticker JSON), checks for
updates using HEAD requests, extracts JSON members from ZIP files to a
dedicated folder, samples one JSON per archive, and creates/updates a
DuckDB table cataloging downloaded files.

Uses:
- config_utils.AppConfig for loading configuration from .env.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.

Required .env variables:
    DOWNLOAD_DIR: Path to store downloaded files and derived data.
    DB_FILE: Path for the DuckDB database file.
"""

import requests
import os
import logging # Keep for level constants (e.g., logging.INFO)
import time
import sys
import zipfile
import json
from pathlib import Path
# from dotenv import load_dotenv # No longer needed here
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from tqdm import tqdm
import shutil # Keep import if might be needed later, currently unused
from typing import Optional, List, Dict, Tuple, Any # Added for type hints

import duckdb # Keep original duckdb import if needed for types etc.

# --- Import Utilities ---
from config_utils import AppConfig                 # Import configuration loader
from logging_utils import setup_logging            # Import logging setup function
from database_conn import ManagedDatabaseConnection # Import DB context manager

# --- Constants ---
# SCRIPT_NAME = Path(__file__).stem (defined in __main__)
# LOG_DIRECTORY = ... (defined in __main__)

# SEC requires a User-Agent header - **MUST BE SET BY USER**
# Consider moving this to .env or config if it changes per environment
SEC_USER_AGENT = "PersonalResearchProject dougstrouth@gmail.com"

DOWNLOAD_URLS = {
    "submissions": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
    "companyfacts": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    "company_tickers": "https://www.sec.gov/files/company_tickers.json"
}

HEADERS = {'User-Agent': SEC_USER_AGENT}
CATALOG_TABLE_NAME = "downloaded_archives"

# --- Helper Functions (Updated to accept logger) ---

def download_file(
    url: str,
    destination_folder: Path,
    filename: str,
    logger: logging.Logger # Pass logger instance
) -> Path | None:
    """Downloads a file, shows progress. Uses provided logger."""
    destination_folder.mkdir(parents=True, exist_ok=True)
    filepath = destination_folder / filename
    logger.info(f"Attempting to download {url} to {filepath}")
    progress_bar = None
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=600)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        if total_size == 0:
            logger.warning(f"Content-length is 0 for {url}. Progress bar may not be accurate.")

        block_size = 8192 * 16
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {filename}", leave=False, disable=(total_size == 0))

        downloaded_bytes = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

        if progress_bar: progress_bar.close()

        if total_size is not None and total_size > 0 and downloaded_bytes != total_size:
             logger.warning(f"Download size mismatch for {filename}. Expected {total_size}, got {downloaded_bytes}")
        elif downloaded_bytes == 0 and response.status_code == 200:
             logger.warning(f"Downloaded 0 bytes for {filename}, but request was successful. File might be empty.")

        final_size = filepath.stat().st_size
        logger.info(f"Successfully downloaded {filename} ({final_size / (1024*1024):.2f} MB)")
        return filepath

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error downloading {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error downloading {url}: {e}")
        return None
    except OSError as e:
        logger.error(f"File system error saving {filepath}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during download of {url}: {e}", exc_info=True)
        return None
    finally:
        if progress_bar is not None and not progress_bar.disable:
            try: progress_bar.close()
            except Exception: pass


def setup_database_and_table(
    db_con: duckdb.DuckDBPyConnection,
    db_file: Path, # Pass db_file path for logging clarity
    logger: logging.Logger # Pass logger instance
):
    """Creates the downloaded_archives catalog table in DuckDB if it doesn't exist."""
    logger.info(f"Setting up database table '{CATALOG_TABLE_NAME}' in {db_file}")
    try:
        db_con.execute(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_TABLE_NAME} (
                file_path VARCHAR PRIMARY KEY,
                file_name VARCHAR,
                url VARCHAR,
                size_bytes BIGINT,
                local_last_modified_utc TIMESTAMPTZ,
                download_timestamp_utc TIMESTAMPTZ,
                status VARCHAR
            );
        """)
        logger.info(f"Table '{CATALOG_TABLE_NAME}' created or already exists.")
    except Exception as e:
        logger.error(f"Failed to create table '{CATALOG_TABLE_NAME}': {e}", exc_info=True)
        raise


def upsert_archive_records(
    db_con: duckdb.DuckDBPyConnection,
    archive_records: List[Dict[str, Any]], # Use more specific type hint
    logger: logging.Logger # Pass logger instance
):
    """Inserts or updates archive download metadata into the DuckDB catalog table."""
    if not archive_records:
        logger.info("No archive records to insert/update in catalog.")
        return

    logger.info(f"Attempting to insert/update {len(archive_records)} records into '{CATALOG_TABLE_NAME}' table.")
    insert_data = [
        (
            r.get('file_path'), r.get('file_name'), r.get('url'),
            r.get('size_bytes'), r.get('local_last_modified_utc'),
            r.get('download_timestamp_utc'), r.get('status')
        ) for r in archive_records
    ]

    try:
        # Handled within ManagedDatabaseConnection context
        # db_con.begin()
        db_con.executemany(f"""
            INSERT INTO {CATALOG_TABLE_NAME} (
                file_path, file_name, url, size_bytes, local_last_modified_utc,
                download_timestamp_utc, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (file_path) DO UPDATE SET
                file_name = excluded.file_name,
                url = excluded.url,
                size_bytes = excluded.size_bytes,
                local_last_modified_utc = excluded.local_last_modified_utc,
                download_timestamp_utc = excluded.download_timestamp_utc,
                status = excluded.status;
            """, insert_data)
        # db_con.commit() # Handled by ManagedDatabaseConnection
        logger.info(f"Successfully inserted/updated {len(archive_records)} archive records in catalog.")
    except Exception as e:
        # db_con.rollback() # Handled by ManagedDatabaseConnection
        logger.error(f"Database error during archive record insertion: {e}", exc_info=True)
        # logger.error("Transaction rolled back.") # Handled by context manager
        raise


def extract_zip_json_members(
    zip_filepath: Path,
    extract_to_dir: Path,
    logger: logging.Logger # Pass logger instance
) -> List[Path]:
    """Extracts all '.json' files from a zip archive to a specified directory."""
    extracted_files = []
    try:
        extract_to_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Starting extraction of JSON files from {zip_filepath.name} to {extract_to_dir}")

        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            json_members = [
                m for m in zip_ref.infolist()
                if m.filename.lower().endswith('.json')
                and not m.is_dir()
                and not m.filename.startswith('__MACOSX/')
            ]
            if not json_members:
                logger.warning(f"No '.json' files found in {zip_filepath.name}")
                return []

            for member in tqdm(json_members, desc=f"Extracting {zip_filepath.stem}", unit="file", leave=False):
                try:
                    target_path = extract_to_dir / Path(member.filename).name
                    with open(target_path, "wb") as outfile:
                        outfile.write(zip_ref.read(member.filename))
                    if target_path.is_file(): extracted_files.append(target_path)
                    else: logger.warning(f"Post-extraction check failed for {target_path}. Member: {member.filename}")
                except KeyError: logger.error(f"KeyError extracting {member.filename} from {zip_filepath.name}. Skipping member.")
                except zipfile.BadZipFile: logger.error(f"Bad zip file error processing member {member.filename} in {zip_filepath.name}. Skipping member.")
                except OSError as e: logger.error(f"OS error extracting {member.filename} from {zip_filepath.name}: {e}. Skipping member.")
                except Exception as e: logger.error(f"Unexpected error extracting {member.filename} from {zip_filepath.name}: {e}", exc_info=True)

        logger.info(f"Finished extraction from {zip_filepath.name}. Extracted {len(extracted_files)} JSON files to {extract_to_dir}")
        return extracted_files

    except zipfile.BadZipFile:
        logger.error(f"Failed to open {zip_filepath.name}: Bad zip file.")
        return []
    except FileNotFoundError:
        logger.error(f"Failed to open {zip_filepath.name}: File not found during extraction attempt.")
        return []
    except Exception as e:
        logger.error(f"Error opening or processing zip file {zip_filepath.name}: {e}", exc_info=True)
        return []


def get_json_sample_data(
    json_filepath: Path,
    logger: logging.Logger, # Pass logger instance
    max_chars: int = 500
) -> str | None:
    """Reads the beginning of a JSON file to provide a sample string."""
    if not json_filepath or not json_filepath.is_file():
        logger.warning(f"Cannot sample JSON, file path invalid or does not exist: {json_filepath}")
        return None
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if len(first_line) > 20: sample = first_line
            else: f.seek(0); sample = f.read(max_chars)
            if len(sample) > max_chars: return sample[:max_chars] + "..."
            elif len(sample) == 0: logger.warning(f"JSON file is empty: {json_filepath.name}"); return "[Empty File]"
            else: return sample.replace('\n', ' ').replace('\r', '')
    except FileNotFoundError:
        logger.error(f"File not found during sampling: {json_filepath}")
        return None
    except UnicodeDecodeError:
        logger.warning(f"Could not decode {json_filepath.name} as UTF-8. Trying latin-1.")
        try:
            with open(json_filepath, 'r', encoding='latin-1') as f: sample = f.read(max_chars)
            if len(sample) > max_chars: return sample[:max_chars].replace('\n', ' ').replace('\r', '') + "..."
            else: return sample.replace('\n', ' ').replace('\r', '')
        except Exception as e: logger.error(f"Failed to read {json_filepath.name} even with latin-1: {e}"); return "[Error Reading File Content]"
    except Exception as e:
        logger.error(f"Failed to read sample from {json_filepath.name}: {e}", exc_info=True)
        return "[Error Reading File]"


def extract_and_sample_zip_archive(
    zip_filepath: Path,
    extract_dir: Path,
    logger: logging.Logger # Pass logger instance
) -> Tuple[str, str | None]:
    """Extracts JSON files from a ZIP archive and gets a sample from the first one."""
    logger.info(f"Processing ZIP archive for extraction and sampling: {zip_filepath.name}")
    extract_dir.mkdir(parents=True, exist_ok=True)

    # Pass logger to extraction function
    extracted_json_files = extract_zip_json_members(zip_filepath, extract_dir, logger)

    if not extracted_json_files:
        if not zip_filepath.exists(): return "Extraction Failed (ZIP Missing)", None
        logger.warning(f"No JSON files were extracted from {zip_filepath.name}.")
        return "No JSON Found", None

    first_json_path = extracted_json_files[0]
    logger.info(f"Sampling first extracted JSON: {first_json_path.name}")
    # Pass logger to sampling function
    sample_data = get_json_sample_data(first_json_path, logger)

    if sample_data is None: return "Extracted (Sample Failed)", None
    else: return "Extracted", sample_data


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()

    # --- Initialize Config and Logging ---
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.getLogger().critical(f"Unexpected error loading config: {e}", exc_info=True)
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    logger.info("--- Starting EDGAR Data Download, Extraction, and Cataloging ---")
    logger.info(f"Current working directory: {Path.cwd()}")
    logger.info(f"Script directory: {Path(__file__).resolve().parent}")
    logger.info(f"Using DOWNLOAD_DIR: {config.DOWNLOAD_DIR}")
    logger.info(f"Using EXTRACT_BASE_DIR: {config.EXTRACT_BASE_DIR}")
    logger.info(f"Using DB_FILE: {config.DB_FILE}")

    # --- Pre-run Checks ---
    if "PersonalResearchProject" not in SEC_USER_AGENT or "doug.strouth@gmail.com" not in SEC_USER_AGENT:
        logger.warning("SEC_USER_AGENT seems generic. Update it with specific project/contact info.")
    if "YourCompanyName" in SEC_USER_AGENT or "YourAppName" in SEC_USER_AGENT or "YourContactEmail@example.com" in SEC_USER_AGENT:
         logger.error("FATAL: SEC_USER_AGENT variable in the script must be updated.")
         logger.error("Please set it to identify your application and provide a contact email.")
         sys.exit("Stopping script: SEC User-Agent not configured.")

    try:
        config.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        config.EXTRACT_BASE_DIR.mkdir(parents=True, exist_ok=True)
        config.DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Could not create required directories: {e}")
        sys.exit("Stopping script: Cannot create directories.")

    # --- Download Files & Collect Metadata ---
    download_metadata_list = []
    current_run_timestamp = datetime.now(timezone.utc)

    logger.info("Starting file checks and downloads...")
    for key, url in DOWNLOAD_URLS.items():
        filename = Path(url).name
        # Use paths from config object
        local_filepath = config.DOWNLOAD_DIR / filename
        archive_stem = local_filepath.stem
        specific_extract_dir = config.EXTRACT_BASE_DIR / archive_stem

        status = "Unknown"
        needs_download = True
        download_success = False
        final_filepath = None
        file_size = None
        local_mtime_dt = None
        needs_processing = False

        # --- File Check / Download Logic (Uses logger) ---
        if local_filepath.exists():
            logger.info(f"Local file exists: {local_filepath}")
            try:
                local_stat = local_filepath.stat()
                local_mtime_timestamp = local_stat.st_mtime
                local_mtime_dt = datetime.fromtimestamp(local_mtime_timestamp, timezone.utc)
                file_size = local_stat.st_size
                logger.info(f"Local file last modified (UTC): {local_mtime_dt}, Size: {file_size} bytes")

                response = requests.head(url, headers=HEADERS, timeout=60)
                response.raise_for_status()

                if 'Last-Modified' in response.headers:
                    remote_last_modified_str = response.headers['Last-Modified']
                    remote_last_modified_dt = parsedate_to_datetime(remote_last_modified_str)
                    if remote_last_modified_dt.tzinfo is None:
                         remote_last_modified_dt = remote_last_modified_dt.replace(tzinfo=timezone.utc)
                    logger.info(f"Remote file last modified (UTC): {remote_last_modified_dt}")

                    if local_mtime_dt >= remote_last_modified_dt - timedelta(seconds=5):
                        logger.info(f"Local file '{filename}' is up-to-date or newer. Skipping download.")
                        needs_download = False
                        status = "Up-to-date"
                        final_filepath = local_filepath
                        download_success = True
                        if local_filepath.suffix.lower() == '.zip':
                             if not specific_extract_dir.exists() or not any(specific_extract_dir.iterdir()):
                                 logger.info(f"Zip '{filename}' is up-to-date, but extraction missing. Flagging for processing.")
                                 needs_processing = True
                             else:
                                 logger.info(f"Zip '{filename}' is up-to-date and seems extracted. No processing needed now.")
                                 needs_processing = False
                                 status = "Extracted (Up-to-date)"
                        else: needs_processing = False
                    else:
                         logger.info(f"Remote file '{filename}' is newer. Proceeding with download.")
                         status = "Requires Update"; needs_processing = True
                else:
                    logger.warning(f"No 'Last-Modified' header for {url}. Assuming update needed.")
                    status = "Requires Update (No Header)"; needs_processing = True

            except requests.exceptions.Timeout:
                 logger.warning(f"Timeout during HEAD check for {url}. Proceeding with download attempt.")
                 status = "Requires Download (Check Timeout)"; needs_processing = True
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error during HEAD check for {url}: {e}. Proceeding with download attempt.")
                status = "Requires Download (Check Failed)"; needs_processing = True
            except Exception as e:
                logger.warning(f"Unexpected error during HEAD check for {url}: {e}. Proceeding with download attempt.", exc_info=True)
                status = "Requires Download (Check Error)"; needs_processing = True
        else:
            logger.info(f"Local file not found: {local_filepath}. Proceeding with download.")
            status = "Requires Download (Not Found)"; needs_processing = True

        if needs_download:
            logger.info(f"Proceeding with download for {filename} (Reason: {status})")
            # Pass logger to download function
            downloaded_path = download_file(url, config.DOWNLOAD_DIR, filename, logger)
            if downloaded_path and downloaded_path.is_file():
                 final_filepath = downloaded_path; download_success = True
                 if status.startswith("Requires Update"): status = "Updated"
                 elif status.startswith("Requires Download"): status = "Downloaded"
                 try:
                      local_stat = final_filepath.stat()
                      file_size = local_stat.st_size
                      local_mtime_timestamp = local_stat.st_mtime
                      local_mtime_dt = datetime.fromtimestamp(local_mtime_timestamp, timezone.utc)
                 except OSError as e:
                      logger.error(f"Could not get stats for downloaded file {final_filepath}: {e}")
                      status = "Failed Post-Download Stat"; download_success = False; needs_processing = False
            else:
                logger.warning(f"Download failed for {key}.")
                status = "Failed"; download_success = False; needs_processing = False
        elif status == "Up-to-date" and needs_processing:
            logger.info(f"File {filename} is up-to-date but requires processing.")

        # --- Post-Download/Check Processing (Uses logger) ---
        sample_log_message = ""
        if final_filepath and final_filepath.is_file() and needs_processing:
            if final_filepath.suffix.lower() == '.zip':
                logger.info(f"Processing required for ZIP: {final_filepath.name}")
                # Pass logger to extraction/sampling function
                process_status, sample_data = extract_and_sample_zip_archive(final_filepath, specific_extract_dir, logger)
                status = process_status
                if sample_data:
                    sample_log_message = f"Sample data from {archive_stem}: {sample_data}"
                    logger.info(sample_log_message)
            elif final_filepath.suffix.lower() == '.json':
                 logger.info(f"File {final_filepath.name} is JSON. No extraction needed.")
                 needs_processing = False
            else:
                logger.warning(f"File {final_filepath.name} is not a ZIP or JSON. Skipping processing.")
                needs_processing = False
        elif status == "Extracted (Up-to-date)":
             logger.info(f"Skipping processing for {filename} as it's up-to-date and already extracted.")

        # --- Record Metadata (Logic unchanged, but context provides logger) ---
        if final_filepath and final_filepath.is_file():
             metadata = { "file_path": str(final_filepath.resolve()), "file_name": filename, "url": url, "size_bytes": file_size, "local_last_modified_utc": local_mtime_dt, "download_timestamp_utc": current_run_timestamp, "status": status }
             download_metadata_list.append(metadata)
        elif status == "Failed" or status.startswith("Failed") or status.startswith("Extraction Failed"):
             metadata = { "file_path": str(local_filepath.resolve()), "file_name": filename, "url": url, "size_bytes": None, "local_last_modified_utc": None, "download_timestamp_utc": current_run_timestamp, "status": status }
             download_metadata_list.append(metadata)
        elif not needs_download and not needs_processing and status == "Up-to-date":
             metadata = { "file_path": str(local_filepath.resolve()), "file_name": filename, "url": url, "size_bytes": file_size, "local_last_modified_utc": local_mtime_dt, "download_timestamp_utc": current_run_timestamp, "status": status }
             download_metadata_list.append(metadata)
        elif not needs_download and not needs_processing and status == "Extracted (Up-to-date)":
              metadata = { "file_path": str(local_filepath.resolve()), "file_name": filename, "url": url, "size_bytes": file_size, "local_last_modified_utc": local_mtime_dt, "download_timestamp_utc": current_run_timestamp, "status": status }
              download_metadata_list.append(metadata)
        else:
             logger.error(f"Could not reliably catalog metadata for {key} (Status: {status}, Needs Download: {needs_download}, Needs Processing: {needs_processing}).")
             metadata = { "file_path": str(local_filepath.resolve()), "file_name": filename, "url": url, "size_bytes": None, "local_last_modified_utc": None, "download_timestamp_utc": current_run_timestamp, "status": f"Cataloging Error ({status})" }
             if status != "Unknown": download_metadata_list.append(metadata)

        time.sleep(0.1) # Keep polite delay

    logger.info(f"Finished download checks and processing attempts. {len(download_metadata_list)} file statuses recorded for catalog update.")

    # --- Update DuckDB Catalog using ManagedDatabaseConnection ---
    try:
        logger.info(f"Connecting to DuckDB database: {config.DB_FILE}")
        # Use context manager for DB operations
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as db_conn:
            if db_conn is None:
                raise ConnectionError(f"Failed to establish database connection to {config.DB_FILE_STR}")

            # Pass logger to setup and upsert functions
            setup_database_and_table(db_conn, config.DB_FILE, logger)
            if download_metadata_list:
                upsert_archive_records(db_conn, download_metadata_list, logger)
            else:
                logger.warning("No download metadata was collected, skipping catalog database update.")
            # Commit/rollback/close handled by ManagedDatabaseConnection

    except ConnectionError as e:
         logger.critical(f"Database Connection Error: {e}. Catalog update failed.")
    except Exception as e:
        logger.error(f"An critical error occurred during database operations: {e}", exc_info=True)
        # Rollback/close handled by context manager if error occurred inside 'with'

    end_time = time.time()
    logger.info(f"--- Script Finished in {end_time - start_time:.2f} seconds ---")