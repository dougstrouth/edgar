# -*- coding: utf-8 -*-
"""
Downloads SEC EDGAR bulk data ZIP files, reads JSON content directly
from the zips, converts it to Parquet format using DuckDB's
native capabilities via Pandas integration, and saves the Parquet files.

Reads configuration from a .env file in the same directory.
Required .env variables:
    DOWNLOAD_DIR: Path to store downloaded zip files.
    DB_FILE: Path for the DuckDB database file (optional for this script).
    PARQUET_DIR: Path to store output Parquet files.
"""

import requests
import zipfile
import os
import logging
import time
import sys
import json                     # Added for JSON parsing
import pandas as pd             # Added for DataFrame handling
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from tqdm import tqdm

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
    PARQUET_DIR = Path(os.environ['PARQUET_DIR']).resolve() # Output for Parquet
    # DB_FILE is not strictly needed for this version, but keep if needed later
    # DB_FILE = Path(os.environ['DB_FILE']).resolve()
except KeyError as e:
    print(f"ERROR: Missing required environment variable in .env file: {e}", file=sys.stderr)
    sys.exit(1)

# SEC requires a User-Agent header - **MUST BE SET BY USER**
SEC_USER_AGENT = "YourCompanyName/YourAppName YourContactEmail@example.com" # CHANGE THIS!

DOWNLOAD_URLS = {
    "submissions": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
    "companyfacts": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    # Keep ticker map download, handle separately if needed
    "company_tickers": "https://www.sec.gov/files/company_tickers.json"
}

# Files within zips we want to process (can be extended)
# For now, process all JSON files within the relevant zips
PROCESS_EXTENSIONS = {'.json'}
RELEVANT_ZIPS = {"submissions.zip", "companyfacts.zip"}


HEADERS = {'User-Agent': SEC_USER_AGENT}

# --- Logging Setup ---
log_file_path = script_dir / "json_to_parquet.log" # Renamed log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler() # Also print logs to console
    ]
)
logging.info(f"Current working directory: {Path.cwd()}")
logging.info(f"Script directory: {script_dir}")
logging.info(f"Using DOWNLOAD_DIR: {DOWNLOAD_DIR}")
logging.info(f"Using PARQUET_DIR: {PARQUET_DIR}")
# logging.info(f"Using DB_FILE: {DB_FILE}") # Uncomment if DB_FILE is used


# --- Helper Functions ---

def download_file(url: str, destination_folder: Path, filename: str) -> Path | None:
    """Downloads a file, checks updates, shows progress. (No change from previous)"""
    # --- Function content is the same as the previous version ---
    # --- Includes HEAD request check and tqdm download progress ---
    destination_folder.mkdir(parents=True, exist_ok=True)
    filepath = destination_folder / filename
    logging.info(f"Attempting to download {url} to {filepath}")
    progress_bar = None # Initialize progress_bar to None
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=300) # 5 min timeout
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        # Use tqdm for download progress if Content-Length is available
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192 * 16 # 128KB chunks
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {filename}", leave=False)

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                progress_bar.update(len(chunk))
                f.write(chunk)

        progress_bar.close() # Close the progress bar on successful completion
        if total_size != 0 and progress_bar.n != total_size:
             logging.warning(f"Download size mismatch for {filename}. Expected {total_size}, got {progress_bar.n}")

        logging.info(f"Successfully downloaded {filename} ({filepath.stat().st_size / (1024*1024):.2f} MB)")
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
        # Ensure progress bar is closed even on error if it exists and hasn't been closed
        if progress_bar is not None and hasattr(progress_bar, 'fp') and not progress_bar.fp.isclosed():
             progress_bar.close()

def parse_json_to_dataframe(json_content_stream, source_filename) -> pd.DataFrame | None:
    """
    Parses JSON content from a stream into a Pandas DataFrame.
    placeholder implementation - needs customization for actual EDGAR JSON structures.
    """
    try:
        data = json.load(json_content_stream)

        # --- !! Placeholder Logic - Needs Adaptation !! ---
        # This needs to be adapted based on whether it's a submission file
        # or a companyfacts file, and extract the relevant structured data.
        # Example for a hypothetical simple list-of-objects JSON:
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
             df = pd.DataFrame(data)
             logging.debug(f"Successfully parsed {source_filename} into DataFrame with {len(df)} rows.")
             return df
        # Example for submissions-like structure (highly simplified):
        elif isinstance(data, dict) and 'filings' in data and 'recent' in data['filings']:
             # This would need significant work to flatten the columnar arrays correctly
             logging.warning(f"Parsing logic for submission file '{source_filename}' not fully implemented.")
             # As a demo, just put metadata in a single-row DataFrame
             df = pd.DataFrame([{'cik': data.get('cik'), 'name': data.get('name')}])
             return df
        # Example for companyfacts-like structure (highly simplified):
        elif isinstance(data, dict) and 'facts' in data:
            # This would need complex logic to flatten facts, units, concepts etc.
            logging.warning(f"Parsing logic for companyfacts file '{source_filename}' not fully implemented.")
            # As a demo, just put metadata in a single-row DataFrame
            df = pd.DataFrame([{'cik': data.get('cik'), 'entityName': data.get('entityName')}])
            return df
        else:
             logging.warning(f"Could not parse JSON structure from {source_filename} into a known DataFrame format.")
             return None
        # --- !! End Placeholder Logic !! ---

    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error in {source_filename}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error creating DataFrame from {source_filename}: {e}", exc_info=True)
        return None


def convert_json_member_to_parquet(zip_file: zipfile.ZipFile, member: zipfile.ZipInfo, parquet_base_dir: Path):
    """
    Reads a JSON member from zip, parses to DataFrame, writes to Parquet using DuckDB.
    """
    output_filename = Path(member.filename).with_suffix('.parquet')
    output_path = (parquet_base_dir / output_filename).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logging.debug(f"Processing zip member: {member.filename} -> {output_path}")

    try:
        # 1. Read JSON from zip stream
        with zip_file.open(member.filename) as json_stream:
            # 2. Parse JSON to DataFrame (using placeholder logic)
            df = parse_json_to_dataframe(json_stream, member.filename)

        if df is None or df.empty:
            logging.warning(f"Skipping Parquet write for {member.filename} due to parsing failure or empty DataFrame.")
            return False # Indicate failure

        # 3. Write DataFrame to Parquet using an in-memory DuckDB instance
        # Use 'duckdb.connect()' for a temporary in-memory database
        with duckdb.connect() as mem_con:
            # Register DataFrame as a temporary table/view
            mem_con.register('temp_df_view', df)
            # Use COPY TO for efficient writing, enabling ZSTD compression
            sql = f"COPY temp_df_view TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);"
            mem_con.execute(sql)
            logging.debug(f"Successfully wrote {member.filename} content to {output_path}")
            # Integrity check hint: Could compare row count: df_rows = len(df); parquet_rows = mem_con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
            return True # Indicate success

    except duckdb.Error as e:
         logging.error(f"DuckDB Error writing Parquet for {member.filename} to {output_path}: {e}")
         return False
    except Exception as e:
        logging.error(f"Failed to convert {member.filename} to Parquet: {e}", exc_info=True)
        # Clean up potentially partially written file
        if output_path.exists():
             try:
                  output_path.unlink()
             except OSError:
                   logging.error(f"Could not delete incomplete parquet file: {output_path}")
        return False


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("--- Starting EDGAR JSON->Parquet Conversion Process ---")

    # --- Pre-run Checks ---
    if "YourCompanyName/YourAppName" in SEC_USER_AGENT or "YourContactEmail@example.com" in SEC_USER_AGENT:
        logging.error("FATAL: SEC_USER_AGENT variable in the script must be updated.")
        logging.error("Please set it to identify your application and provide a contact email.")
        sys.exit("Stopping script: SEC User-Agent not configured.")

    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        PARQUET_DIR.mkdir(parents=True, exist_ok=True) # Ensure Parquet output dir exists
    except OSError as e:
        logging.error(f"Could not create required directories (Download: {DOWNLOAD_DIR}, Parquet: {PARQUET_DIR}): {e}")
        sys.exit("Stopping script: Cannot create directories.")

    # --- Download Files (with update check) ---
    # --- This section is the same as the previous version ---
    downloaded_files = {}
    logging.info("Starting file checks and downloads...")
    for key, url in DOWNLOAD_URLS.items():
        filename = Path(url).name
        local_filepath = DOWNLOAD_DIR / filename
        needs_download = True # Assume download is needed by default

        if local_filepath.exists():
            logging.info(f"Local file exists: {local_filepath}")
            try:
                local_mtime_timestamp = local_filepath.stat().st_mtime
                local_mtime_dt = datetime.fromtimestamp(local_mtime_timestamp, timezone.utc)
                logging.info(f"Local file last modified (UTC): {local_mtime_dt}")

                response = requests.head(url, headers=HEADERS, timeout=30)
                response.raise_for_status()

                if 'Last-Modified' in response.headers:
                    remote_last_modified_str = response.headers['Last-Modified']
                    remote_last_modified_dt = parsedate_to_datetime(remote_last_modified_str)
                    if remote_last_modified_dt.tzinfo is None:
                         remote_last_modified_dt = remote_last_modified_dt.replace(tzinfo=timezone.utc)

                    logging.info(f"Remote file last modified (UTC): {remote_last_modified_dt}")

                    if local_mtime_dt >= remote_last_modified_dt - timedelta(seconds=1):
                        logging.info(f"Local file '{filename}' is up-to-date or newer. Skipping download.")
                        needs_download = False
                        if key not in downloaded_files:
                             downloaded_files[key] = local_filepath
                    else:
                         logging.info(f"Remote file '{filename}' is newer. Proceeding with download.")
                else:
                    logging.warning(f"Remote server did not provide 'Last-Modified' header for {url}. Will download.")

            except requests.exceptions.Timeout:
                 logging.warning(f"Timeout during HEAD request for {url}. Will download.")
            except requests.exceptions.RequestException as e:
                logging.warning(f"HEAD request failed for {url}: {e}. Will download.")
            except Exception as e:
                logging.warning(f"Error comparing file dates for {url}: {e}. Will download.")
        else:
            logging.info(f"Local file not found: {local_filepath}. Proceeding with download.")

        if needs_download:
            filepath = download_file(url, DOWNLOAD_DIR, filename)
            if filepath:
                downloaded_files[key] = filepath
            else:
                logging.warning(f"Download failed for {key}, skipping processing for this file.")
                if key in downloaded_files: del downloaded_files[key]
        elif key not in downloaded_files and local_filepath.exists():
             downloaded_files[key] = local_filepath

        time.sleep(0.5) # Keep the delay

    logging.info(f"Files available for processing: {list(downloaded_files.values())}")
    if not downloaded_files:
         logging.warning("No files available for processing (downloads skipped or failed). Exiting.")
         sys.exit(0)

    # --- Process ZIP files: Read JSON -> Convert to Parquet ---
    logging.info("Starting JSON to Parquet conversion...")
    total_processed_count = 0
    total_success_count = 0

    for key, zip_filepath in downloaded_files.items():
        # Only process the relevant large zip files
        if zip_filepath.name not in RELEVANT_ZIPS:
            logging.info(f"Skipping processing for non-target file: {zip_filepath.name}")
            continue

        if not zip_filepath or not zip_filepath.is_file():
            logging.warning(f"Skipping processing for non-existent or invalid file path associated with key '{key}': {zip_filepath}")
            continue

        logging.info(f"Processing zip file: {zip_filepath.name}")
        processed_in_zip = 0
        success_in_zip = 0
        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zf:
                # Filter members by extension
                json_members = [
                    m for m in zf.infolist()
                    if not m.is_dir() and Path(m.filename).suffix.lower() in PROCESS_EXTENSIONS
                ]

                # Use tqdm for progress on processing members within the zip
                for member in tqdm(json_members, desc=f"Converting {zip_filepath.name}", unit='file', leave=False):
                     processed_in_zip += 1
                     # Define output base dir for this specific zip file's contents
                     parquet_output_base = PARQUET_DIR / Path(zip_filepath.stem) # e.g. ./parquet_data/submissions/
                     if convert_json_member_to_parquet(zf, member, parquet_output_base):
                          success_in_zip += 1

            logging.info(f"Finished processing {zip_filepath.name}. {success_in_zip}/{processed_in_zip} JSON members successfully converted to Parquet.")
            total_processed_count += processed_in_zip
            total_success_count += success_in_zip

        except zipfile.BadZipFile:
            logging.error(f"Error: {zip_filepath.name} is not a valid zip file or is corrupted. Skipping.")
        except FileNotFoundError:
             logging.error(f"Error: Zip file not found at {zip_filepath}. Skipping.")
        except Exception as e:
            logging.error(f"An unexpected error occurred processing {zip_filepath.name}: {e}", exc_info=True)


    end_time = time.time()
    logging.info(f"--- JSON to Parquet Conversion Finished ---")
    logging.info(f"Total JSON members processed: {total_processed_count}")
    logging.info(f"Total successfully converted to Parquet: {total_success_count}")
    logging.info(f"Total time: {end_time - start_time:.2f} seconds")