# -*- coding: utf-8 -*-
"""
Downloads SEC EDGAR bulk data files (ZIPs and Ticker JSON), checks for
updates using HEAD requests, extracts JSON members from ZIP files to a
dedicated folder, samples one JSON per archive, and creates/updates a
DuckDB table cataloging downloaded files.

Reads configuration from a .env file in the same directory.
Required .env variables:
    DOWNLOAD_DIR: Path to store downloaded files and derived data.
    DB_FILE: Path for the DuckDB database file.
"""

import requests
import os
import logging
import time
import sys
import zipfile
import json # Using standard json library
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from tqdm import tqdm
import shutil # Still needed for potential cleanup if needed in future, but not used now

# --- Configuration ---

# Load environment variables from .env file
script_dir = Path(__file__).resolve().parent
dotenv_path = script_dir / '.env'

if not dotenv_path.is_file():
    print(f"ERROR: .env file not found at {dotenv_path}", file=sys.stderr)
    sys.exit(1)

load_dotenv(dotenv_path=dotenv_path)

# Get configuration from environment variables
try:
    DOWNLOAD_DIR = Path(os.environ['DOWNLOAD_DIR']).resolve()
    DB_FILE = Path(os.environ['DB_FILE']).resolve()
    # Define base directory for extracted JSON files
    EXTRACT_BASE_DIR = DOWNLOAD_DIR / "extracted_json"
except KeyError as e:
    print(f"ERROR: Missing required environment variable in .env file: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Could not configure paths: {e}", file=sys.stderr)
    sys.exit(1)

# SEC requires a User-Agent header - **MUST BE SET BY USER**
SEC_USER_AGENT = "PersonalResearchProject dougstrouth@gmail.com" # Make sure this is correct!

DOWNLOAD_URLS = {
    "submissions": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
    "companyfacts": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    "company_tickers": "https://www.sec.gov/files/company_tickers.json" # Note: This is already JSON
}

HEADERS = {'User-Agent': SEC_USER_AGENT}
CATALOG_TABLE_NAME = "downloaded_archives" # Name for the metadata table

# --- Logging Setup ---
log_file_path = script_dir / "download_extract.log" # Renamed log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s', # Added funcName
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logging.info(f"Current working directory: {Path.cwd()}")
logging.info(f"Script directory: {script_dir}")
logging.info(f"Using DOWNLOAD_DIR: {DOWNLOAD_DIR}")
logging.info(f"Using EXTRACT_BASE_DIR: {EXTRACT_BASE_DIR}")
logging.info(f"Using DB_FILE: {DB_FILE}")

# --- Helper Functions ---

def download_file(url: str, destination_folder: Path, filename: str) -> Path | None:
    """Downloads a file, shows progress. (Simplified - no HEAD check inside)"""
    destination_folder.mkdir(parents=True, exist_ok=True)
    filepath = destination_folder / filename
    logging.info(f"Attempting to download {url} to {filepath}")
    progress_bar = None
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=600) # Increased timeout
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        # Handle cases where content-length is not provided or zero
        if total_size == 0:
            logging.warning(f"Content-length is 0 for {url}. Progress bar may not be accurate.")
            # Set a dummy large number for tqdm if you want it to show progress based on bytes received
            # total_size = None # Or keep it 0 if that's acceptable for tqdm

        block_size = 8192 * 16 # 128 KiB
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {filename}", leave=False, disable=(total_size == 0))

        downloaded_bytes = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk: # filter out keep-alive new chunks
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

        if progress_bar:
             progress_bar.close() # Ensure closure

        # Verify downloaded size if total_size was known
        if total_size is not None and total_size > 0 and downloaded_bytes != total_size:
             logging.warning(f"Download size mismatch for {filename}. Expected {total_size}, got {downloaded_bytes}")
        elif downloaded_bytes == 0 and response.status_code == 200:
             logging.warning(f"Downloaded 0 bytes for {filename}, but request was successful. File might be empty.")


        final_size = filepath.stat().st_size
        logging.info(f"Successfully downloaded {filename} ({final_size / (1024*1024):.2f} MB)")
        return filepath

    except requests.exceptions.Timeout:
        logging.error(f"Timeout error downloading {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error downloading {url}: {e}")
        return None
    except OSError as e:
        logging.error(f"File system error saving {filepath}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during download of {url}: {e}", exc_info=True)
        return None
    finally:
        # Ensure progress bar is closed even if an error occurs mid-download
        if progress_bar is not None and not progress_bar.disable:
            try:
                progress_bar.close()
            except Exception:
                pass # Ignore errors closing progress bar


def setup_database_and_table(db_con: duckdb.DuckDBPyConnection):
    """
    Creates the downloaded_archives catalog table in DuckDB if it doesn't exist.
    (Removed parquet_path column)
    """
    logging.info(f"Setting up database table '{CATALOG_TABLE_NAME}' in {DB_FILE}")
    try:
        # Schema for cataloging downloaded archive files
        db_con.execute(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_TABLE_NAME} (
                file_path VARCHAR PRIMARY KEY,      -- Absolute path to the local file (ZIP or JSON)
                file_name VARCHAR,                  -- Base name (e.g., submissions.zip)
                url VARCHAR,                        -- Source URL
                size_bytes BIGINT,                  -- Size on disk
                local_last_modified_utc TIMESTAMPTZ,-- Last modified time of local file
                download_timestamp_utc TIMESTAMPTZ, -- When this script run completed check/download
                status VARCHAR                      -- 'Downloaded', 'Updated', 'Up-to-date', 'Failed', 'Extracted', 'Extraction Failed'
            );
        """)
        logging.info(f"Table '{CATALOG_TABLE_NAME}' created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create table '{CATALOG_TABLE_NAME}': {e}", exc_info=True)
        raise


def upsert_archive_records(db_con: duckdb.DuckDBPyConnection, archive_records: list[dict]):
    """
    Inserts or updates archive download metadata into the DuckDB catalog table.
    (Removed parquet_path parameter/column)
    """
    if not archive_records:
        logging.info("No archive records to insert/update in catalog.")
        return

    logging.info(f"Attempting to insert/update {len(archive_records)} records into '{CATALOG_TABLE_NAME}' table.")
    # Prepare data as list of tuples for executemany
    insert_data = [
        (
            r.get('file_path'),
            r.get('file_name'),
            r.get('url'),
            r.get('size_bytes'),
            r.get('local_last_modified_utc'),
            r.get('download_timestamp_utc'),
            r.get('status')
            # Removed parquet_path
        ) for r in archive_records
    ]

    try:
        db_con.begin()
        # Upsert based on file_path primary key
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
        db_con.commit()
        logging.info(f"Successfully inserted/updated {len(archive_records)} archive records in catalog.")
    except Exception as e:
        db_con.rollback()
        logging.error(f"Database error during archive record insertion: {e}", exc_info=True)
        logging.error("Transaction rolled back.")
        raise # Re-raise after rollback

# --- NEW: ZIP Extraction and Sampling Functions ---

def extract_zip_json_members(zip_filepath: Path, extract_to_dir: Path) -> list[Path]:
    """
    Extracts all '.json' files from a zip archive to a specified directory.

    Args:
        zip_filepath: Path to the .zip file.
        extract_to_dir: Directory where JSON files will be extracted.

    Returns:
        A list of paths to the extracted JSON files. Returns empty list on failure.
    """
    extracted_files = []
    try:
        extract_to_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Starting extraction of JSON files from {zip_filepath.name} to {extract_to_dir}")

        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            # Filter for files ending in .json, ignoring directories and MACOSX metadata
            json_members = [
                m for m in zip_ref.infolist()
                if m.filename.lower().endswith('.json')
                and not m.is_dir()
                and not m.filename.startswith('__MACOSX/')
            ]
            if not json_members:
                logging.warning(f"No '.json' files found in {zip_filepath.name}")
                return []

            for member in tqdm(json_members, desc=f"Extracting {zip_filepath.stem}", unit="file", leave=False):
                try:
                    # Extract the file, ZipFile handles path separators internally
                    target_path = extract_to_dir / Path(member.filename).name # Extract flat for simplicity now
                    # If you need to preserve internal directory structure:
                    # target_path = extract_to_dir / member.filename
                    # target_path.parent.mkdir(parents=True, exist_ok=True)
                    # zip_ref.extract(member, path=extract_to_dir) # This preserves structure

                    # Extract flat:
                    with open(target_path, "wb") as outfile:
                        outfile.write(zip_ref.read(member.filename))

                    if target_path.is_file():
                         extracted_files.append(target_path)
                    else:
                         logging.warning(f"Post-extraction check failed for {target_path}. Member: {member.filename}")

                except KeyError:
                     logging.error(f"KeyError extracting {member.filename} from {zip_filepath.name}. Might indicate corruption. Skipping member.")
                except zipfile.BadZipFile:
                     logging.error(f"Bad zip file error processing member {member.filename} in {zip_filepath.name}. Skipping member.")
                except OSError as e:
                     logging.error(f"OS error extracting {member.filename} from {zip_filepath.name}: {e}. Skipping member.")
                except Exception as e:
                     logging.error(f"Unexpected error extracting {member.filename} from {zip_filepath.name}: {e}", exc_info=True)

        logging.info(f"Finished extraction from {zip_filepath.name}. Extracted {len(extracted_files)} JSON files to {extract_to_dir}")
        return extracted_files

    except zipfile.BadZipFile:
        logging.error(f"Failed to open {zip_filepath.name}: Bad zip file.")
        return []
    except FileNotFoundError:
        logging.error(f"Failed to open {zip_filepath.name}: File not found during extraction attempt.")
        return []
    except Exception as e:
        logging.error(f"Error opening or processing zip file {zip_filepath.name}: {e}", exc_info=True)
        return []


def get_json_sample_data(json_filepath: Path, max_chars: int = 500) -> str | None:
    """
    Reads the beginning of a JSON file to provide a sample string.
    Tries to read the first line, falls back to first N characters.

    Args:
        json_filepath: Path to the JSON file.
        max_chars: Maximum characters to return for the sample.

    Returns:
        A string containing the sample data, or None if reading fails.
    """
    if not json_filepath or not json_filepath.is_file():
        logging.warning(f"Cannot sample JSON, file path invalid or does not exist: {json_filepath}")
        return None
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            # If the first line is substantial, use it. Otherwise, read start of file.
            if len(first_line) > 20: # Arbitrary threshold for a meaningful line
                 sample = first_line
            else:
                 # Rewind and read the first `max_chars` bytes/characters
                 f.seek(0)
                 sample = f.read(max_chars)

            # Truncate if necessary and add ellipsis
            if len(sample) > max_chars:
                return sample[:max_chars] + "..."
            elif len(sample) == 0:
                logging.warning(f"JSON file is empty: {json_filepath.name}")
                return "[Empty File]"
            else:
                # Clean potentially problematic characters for logging/printing
                return sample.replace('\n', ' ').replace('\r', '')

    except FileNotFoundError:
        logging.error(f"File not found during sampling: {json_filepath}")
        return None
    except UnicodeDecodeError:
        logging.warning(f"Could not decode {json_filepath.name} as UTF-8. Trying latin-1.")
        try:
            with open(json_filepath, 'r', encoding='latin-1') as f:
                 sample = f.read(max_chars)
            if len(sample) > max_chars:
                 return sample[:max_chars].replace('\n', ' ').replace('\r', '') + "..."
            else:
                 return sample.replace('\n', ' ').replace('\r', '')
        except Exception as e:
             logging.error(f"Failed to read {json_filepath.name} even with latin-1: {e}")
             return "[Error Reading File Content]"
    except Exception as e:
        logging.error(f"Failed to read sample from {json_filepath.name}: {e}", exc_info=True)
        return "[Error Reading File]"


def extract_and_sample_zip_archive(zip_filepath: Path, extract_dir: Path) -> tuple[str, str | None]:
    """
    Extracts JSON files from a ZIP archive and gets a sample from the first one.

    Args:
        zip_filepath: Path to the source ZIP file.
        extract_dir: Directory to store extracted JSON files.

    Returns:
        A tuple: (status_string, sample_data_string).
                 Sample data string is None if no JSON found or sampling failed.
    """
    logging.info(f"Processing ZIP archive for extraction and sampling: {zip_filepath.name}")
    extract_dir.mkdir(parents=True, exist_ok=True) # Ensure target exists

    extracted_json_files = extract_zip_json_members(zip_filepath, extract_dir)

    if not extracted_json_files:
        # Check if extraction failed or just no JSON files were present
        if not zip_filepath.exists(): # Check if the zip file disappeared somehow
             return "Extraction Failed (ZIP Missing)", None
        # We already logged warnings/errors in extract_zip_json_members if it failed
        # Assume if list is empty, it means no JSON files found or extraction failed earlier
        logging.warning(f"No JSON files were extracted from {zip_filepath.name}.")
        # Could try opening zip again to differentiate "failed" vs "no json" but maybe overkill
        return "No JSON Found", None

    # Successfully extracted at least one JSON file
    # Sample the first extracted file
    first_json_path = extracted_json_files[0]
    logging.info(f"Sampling first extracted JSON: {first_json_path.name}")
    sample_data = get_json_sample_data(first_json_path)

    if sample_data is None:
        # Sampling failed, but extraction worked
        return "Extracted (Sample Failed)", None
    else:
        # Extraction and sampling successful
        return "Extracted", sample_data


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("--- Starting EDGAR Data Download, Extraction, and Cataloging ---")

    # --- Pre-run Checks ---
    if "PersonalResearchProject" not in SEC_USER_AGENT or "doug.strouth@gmail.com" not in SEC_USER_AGENT:
        logging.warning("SEC_USER_AGENT seems generic. Update it with specific project/contact info.")
    if "YourCompanyName" in SEC_USER_AGENT or "YourAppName" in SEC_USER_AGENT or "YourContactEmail@example.com" in SEC_USER_AGENT:
         logging.error("FATAL: SEC_USER_AGENT variable in the script must be updated.")
         logging.error("Please set it to identify your application and provide a contact email.")
         sys.exit("Stopping script: SEC User-Agent not configured.")

    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        EXTRACT_BASE_DIR.mkdir(parents=True, exist_ok=True) # Ensure extract base exists
        DB_FILE.parent.mkdir(parents=True, exist_ok=True) # Ensure DB directory exists
    except OSError as e:
        logging.error(f"Could not create required directories: {e}")
        sys.exit("Stopping script: Cannot create directories.")

    # --- Download Files & Collect Metadata ---
    download_metadata_list = []
    current_run_timestamp = datetime.now(timezone.utc)

    logging.info("Starting file checks and downloads...")
    for key, url in DOWNLOAD_URLS.items():
        filename = Path(url).name
        local_filepath = DOWNLOAD_DIR / filename
        archive_stem = local_filepath.stem # e.g., "submissions"
        specific_extract_dir = EXTRACT_BASE_DIR / archive_stem # e.g., /path/to/extracted_json/submissions

        status = "Unknown"
        needs_download = True
        download_success = False
        final_filepath = None
        file_size = None
        local_mtime_dt = None
        needs_processing = False # Flag to indicate if extraction should run

        # --- File Check / Download Logic (same as before) ---
        if local_filepath.exists():
            logging.info(f"Local file exists: {local_filepath}")
            try:
                local_stat = local_filepath.stat()
                local_mtime_timestamp = local_stat.st_mtime
                local_mtime_dt = datetime.fromtimestamp(local_mtime_timestamp, timezone.utc)
                file_size = local_stat.st_size
                logging.info(f"Local file last modified (UTC): {local_mtime_dt}, Size: {file_size} bytes")

                # HEAD request to check remote modification time
                response = requests.head(url, headers=HEADERS, timeout=60)
                response.raise_for_status()

                if 'Last-Modified' in response.headers:
                    remote_last_modified_str = response.headers['Last-Modified']
                    remote_last_modified_dt = parsedate_to_datetime(remote_last_modified_str)
                    if remote_last_modified_dt.tzinfo is None:
                         remote_last_modified_dt = remote_last_modified_dt.replace(tzinfo=timezone.utc)
                    logging.info(f"Remote file last modified (UTC): {remote_last_modified_dt}")

                    if local_mtime_dt >= remote_last_modified_dt - timedelta(seconds=5):
                        logging.info(f"Local file '{filename}' is up-to-date or newer. Skipping download.")
                        needs_download = False
                        status = "Up-to-date"
                        final_filepath = local_filepath
                        download_success = True
                        # Check if already extracted for "Up-to-date" files
                        if local_filepath.suffix.lower() == '.zip':
                             if not specific_extract_dir.exists() or not any(specific_extract_dir.iterdir()):
                                 logging.info(f"Zip '{filename}' is up-to-date, but extraction missing. Flagging for processing.")
                                 needs_processing = True
                                 # Keep status as "Up-to-date" for download log, will update after processing attempt
                             else:
                                 logging.info(f"Zip '{filename}' is up-to-date and seems extracted. No processing needed now.")
                                 needs_processing = False
                                 status = "Extracted (Up-to-date)" # Assume previously extracted state is still valid
                        else:
                             needs_processing = False # Standalone JSON doesn't need extraction
                    else:
                         logging.info(f"Remote file '{filename}' is newer. Proceeding with download.")
                         status = "Requires Update"
                         needs_processing = True # Needs processing after update
                else:
                    logging.warning(f"No 'Last-Modified' header for {url}. Assuming update needed.")
                    status = "Requires Update (No Header)"
                    needs_processing = True

            except requests.exceptions.Timeout:
                 logging.warning(f"Timeout during HEAD check for {url}. Proceeding with download attempt.")
                 status = "Requires Download (Check Timeout)"
                 needs_processing = True
            except requests.exceptions.RequestException as e:
                logging.warning(f"Error during HEAD check for {url}: {e}. Proceeding with download attempt.")
                status = "Requires Download (Check Failed)"
                needs_processing = True
            except Exception as e:
                logging.warning(f"Unexpected error during HEAD check for {url}: {e}. Proceeding with download attempt.", exc_info=True)
                status = "Requires Download (Check Error)"
                needs_processing = True
        else:
            logging.info(f"Local file not found: {local_filepath}. Proceeding with download.")
            status = "Requires Download (Not Found)"
            needs_processing = True # Needs processing after download

        # --- Perform download only if needed ---
        if needs_download:
            logging.info(f"Proceeding with download for {filename} (Reason: {status})")
            downloaded_path = download_file(url, DOWNLOAD_DIR, filename)
            if downloaded_path and downloaded_path.is_file():
                 final_filepath = downloaded_path
                 download_success = True
                 if status.startswith("Requires Update"):
                     status = "Updated"
                 elif status.startswith("Requires Download"):
                     status = "Downloaded"
                 # Update stats after download
                 try:
                      local_stat = final_filepath.stat()
                      file_size = local_stat.st_size
                      local_mtime_timestamp = local_stat.st_mtime
                      local_mtime_dt = datetime.fromtimestamp(local_mtime_timestamp, timezone.utc)
                 except OSError as e:
                      logging.error(f"Could not get stats for downloaded file {final_filepath}: {e}")
                      status = "Failed Post-Download Stat"
                      download_success = False
                      needs_processing = False # Don't process if stats failed
            else:
                logging.warning(f"Download failed for {key}.")
                status = "Failed"
                download_success = False
                needs_processing = False # Can't process if download failed
        elif status == "Up-to-date" and needs_processing:
            # File is up-to-date but needs processing (extraction missing)
            logging.info(f"File {filename} is up-to-date but requires processing.")
            # Keep status 'Up-to-date' for now, will be updated after processing attempt below


        # --- Post-Download/Check Processing (Extraction & Sampling) ---
        sample_log_message = ""
        if final_filepath and final_filepath.is_file() and needs_processing:
            if final_filepath.suffix.lower() == '.zip':
                logging.info(f"Processing required for ZIP: {final_filepath.name}")
                process_status, sample_data = extract_and_sample_zip_archive(final_filepath, specific_extract_dir)

                # Update overall status based on processing result
                status = process_status # e.g., "Extracted", "No JSON Found", "Extraction Failed"

                if sample_data:
                    sample_log_message = f"Sample data from {archive_stem}: {sample_data}"
                    logging.info(sample_log_message) # Log the sample immediately

            elif final_filepath.suffix.lower() == '.json':
                 # Standalone JSON file - No extraction needed.
                 # Optionally, could add sampling for it too.
                 logging.info(f"File {final_filepath.name} is JSON. No extraction needed.")
                 # Update status if it was downloaded/updated
                 if status in ["Downloaded", "Updated"]:
                     pass # Keep download status
                 elif status == "Up-to-date":
                     pass # Keep up-to-date status
                 needs_processing = False # Mark as not needing further zip processing

            else:
                logging.warning(f"File {final_filepath.name} is not a ZIP or JSON. Skipping processing.")
                needs_processing = False
        elif status == "Extracted (Up-to-date)":
             logging.info(f"Skipping processing for {filename} as it's up-to-date and already extracted.")
             # No processing needed, status already set


        # --- Record Metadata ---
        if final_filepath and final_filepath.is_file():
             metadata = {
                 "file_path": str(final_filepath.resolve()),
                 "file_name": filename,
                 "url": url,
                 "size_bytes": file_size,
                 "local_last_modified_utc": local_mtime_dt,
                 "download_timestamp_utc": current_run_timestamp,
                 "status": status # Use the final status after potential processing
             }
             download_metadata_list.append(metadata)
        elif status == "Failed" or status.startswith("Failed") or status.startswith("Extraction Failed"):
             # Log failure in catalog even if file path isn't valid anymore
             metadata = {
                 "file_path": str(local_filepath.resolve()), # Log intended path
                 "file_name": filename,
                 "url": url,
                 "size_bytes": None,
                 "local_last_modified_utc": None,
                 "download_timestamp_utc": current_run_timestamp,
                 "status": status # Record the failure status
             }
             download_metadata_list.append(metadata)
        elif not needs_download and not needs_processing and status == "Up-to-date":
             # Case: Up-to-date standalone JSON file
             metadata = {
                 "file_path": str(local_filepath.resolve()),
                 "file_name": filename,
                 "url": url,
                 "size_bytes": file_size,
                 "local_last_modified_utc": local_mtime_dt,
                 "download_timestamp_utc": current_run_timestamp, # Still update check time
                 "status": status # Up-to-date
             }
             download_metadata_list.append(metadata)
        elif not needs_download and not needs_processing and status == "Extracted (Up-to-date)":
              # Case: Up-to-date and already extracted ZIP
              metadata = {
                 "file_path": str(local_filepath.resolve()),
                 "file_name": filename,
                 "url": url,
                 "size_bytes": file_size,
                 "local_last_modified_utc": local_mtime_dt,
                 "download_timestamp_utc": current_run_timestamp, # Still update check time
                 "status": status # Extracted (Up-to-date)
             }
              download_metadata_list.append(metadata)
        else:
             # Catch any other logic gaps or unexpected states
             logging.error(f"Could not reliably catalog metadata for {key} (Status: {status}, Needs Download: {needs_download}, Needs Processing: {needs_processing}).")
             # Log a generic failure state
             metadata = {
                 "file_path": str(local_filepath.resolve()),
                 "file_name": filename,
                 "url": url,
                 "size_bytes": None,
                 "local_last_modified_utc": None,
                 "download_timestamp_utc": current_run_timestamp,
                 "status": f"Cataloging Error ({status})"
             }
             if status != "Unknown": # Avoid adding duplicate failures if already marked
                 download_metadata_list.append(metadata)


        time.sleep(0.1) # Keep a small delay between URL processing

    logging.info(f"Finished download checks and processing attempts. {len(download_metadata_list)} file statuses recorded for catalog update.")

    # --- Update DuckDB Catalog ---
    db_conn = None
    try:
        logging.info(f"Connecting to DuckDB database: {DB_FILE}")
        db_conn = duckdb.connect(database=str(DB_FILE), read_only=False)

        # 1. Setup and update the main catalog table
        setup_database_and_table(db_conn)
        if download_metadata_list:
            upsert_archive_records(db_conn, download_metadata_list)
        else:
            logging.warning("No download metadata was collected, skipping catalog database update.")

        # 2. Removed: Creation of DuckDB tables for Parquet data

    except Exception as e:
        logging.error(f"An critical error occurred during database operations: {e}", exc_info=True)
    finally:
        if db_conn:
            try:
                db_conn.close()
                logging.info("Database connection closed.")
            except Exception as e:
                logging.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logging.info(f"--- Script Finished in {end_time - start_time:.2f} seconds ---")