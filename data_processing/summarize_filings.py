# -*- coding: utf-8 -*-
"""
Summarizes filings from the EDGAR database.

This script requires the following additional packages:
- beautifulsoup4
- transformers
- torch
"""
import sys
import logging
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from transformers import pipeline
from tqdm import tqdm
from datetime import datetime, timedelta
import time

# --- BEGIN: Add project root to sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection

# --- Constants ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
MAX_WORKERS = 4  # Adjust as needed

# --- Initialize Summarization Pipeline ---
try:
    summarizer = pipeline("summarization", model="google/pegasus-xsum")
except Exception as e:
    logging.error(f"Failed to initialize summarization pipeline: {e}")
    summarizer = None

def get_filings_to_summarize(db_conn):
    """
    Gets the list of filings from the last 3 years that need to be summarized.
    """
    three_years_ago = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    query = f"""
    SELECT f.cik, f.accession_number, f.primary_document
    FROM filings f
    LEFT JOIN filing_summaries fs ON f.accession_number = fs.accession_number
    WHERE fs.accession_number IS NULL
    AND f.form IN ('10-K', '10-Q')
    AND f.filing_date >= '{three_years_ago}';
    """
    return db_conn.query(query).fetchall()

def download_and_parse_filing(cik, accession_number, primary_document):
    """
    Downloads and parses a single filing to extract its text content, with retries and backoff.
    """
    filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number.replace('-', '')}/{primary_document}"
    
    retries = 3
    backoff_factor = 2
    
    for i in range(retries):
        try:
            # A small delay to stay within rate limits
            time.sleep(0.1)
            response = requests.get(filing_url, headers={"User-Agent": "Gemini Agent"}, timeout=60)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove tables and other non-prose elements
            for table in soup.find_all('table'):
                table.decompose()
                
            return soup.get_text()
        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                wait_time = backoff_factor * (2 ** i)
                logging.warning(f"Rate limited. Retrying {accession_number} in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Failed to download filing {accession_number}: {e}")
                return None
    
    logging.error(f"Failed to download filing {accession_number} after {retries} retries.")
    return None

def summarize_text(text):
    """
    Summarizes the given text using the transformers pipeline.
    """
    if not summarizer or not text:
        return None
        
    try:
        # The model has a max input length, so we truncate the text if necessary.
        # A more sophisticated approach would be to chunk the text and summarize each chunk.
        max_length = summarizer.model.config.max_position_embeddings
        summary = summarizer(text[:max_length], max_length=150, min_length=40, do_sample=False)
        return summary[0]['summary_text']
    except Exception as e:
        logging.error(f"Failed to summarize text: {e}")
        return None

def process_filing(filing_info):
    """

    Downloads, parses, summarizes, and stores the summary for a single filing.
    """
    cik, accession_number, primary_document = filing_info
    text = download_and_parse_filing(cik, accession_number, primary_document)
    if text:
        summary = summarize_text(text)
        if summary:
            return accession_number, summary, "google/pegasus-xsum"
    return None, None, None

def main():
    """
    Main function for the summarization script.
    """
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)
    
    if not summarizer:
        logger.critical("Summarization pipeline not available. Exiting.")
        sys.exit(1)

    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)

    with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR) as db_conn:
        filings_to_summarize = get_filings_to_summarize(db_conn)
        logger.info(f"Found {len(filings_to_summarize)} filings to summarize.")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            with tqdm(total=len(filings_to_summarize), desc="Summarizing filings") as pbar:
                future_to_filing = {executor.submit(process_filing, filing): filing for filing in filings_to_summarize}
                for future in as_completed(future_to_filing):
                    accession_number, summary, model = future.result()
                    if summary:
                        db_conn.execute(
                            "INSERT INTO filing_summaries (accession_number, summary_text, summary_model) VALUES (?, ?, ?)",
                            (accession_number, summary, model),
                        )
                        logger.info(f"Stored summary for filing {accession_number}")
                    pbar.update(1)

if __name__ == "__main__":
    main()
