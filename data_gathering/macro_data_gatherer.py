# -*- coding: utf-8 -*-
"""
Macroeconomic Data Gatherer (FRED)

Fetches key macroeconomic time series data from the Federal Reserve Economic
Data (FRED) API, checks for copyright restrictions, and saves the data to
a Parquet file.

This script requires a FRED_API_KEY to be set in the .env file.
"""

import sys
import time
import logging
import shutil
from pathlib import Path

import pandas as pd
from fredapi import Fred

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
import utils.parquet_converter as parquet_converter

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
# List of key macroeconomic series IDs from FRED
FRED_SERIES_IDS = [
    "GDP",          # Gross Domestic Product
    "CPIAUCSL",     # Consumer Price Index for All Urban Consumers (Inflation)
    "UNRATE",       # Civilian Unemployment Rate
    "DFF",          # Federal Funds Effective Rate (Interest Rates)
    "T10Y2Y",       # 10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity
    "M2SL",         # M2 Money Supply
    "ICSA",         # Initial Claims (unemployment)
    "INDPRO",       # Industrial Production Index
    "HOUST",        # Housing Starts: Total: New Privately Owned Housing Units Started
    "PSAVERT",      # Personal Saving Rate
]

MACRO_TABLE_NAME = "macro_economic_data"

def run_macro_data_pipeline(config: AppConfig):
    """
    Main orchestration function for the FRED data gathering pipeline.
    """
    logger.info("--- Starting FRED Macroeconomic Data Pipeline ---")
    start_time = time.time()

    # --- Cleanliness Step: Ensure a fresh start for the Parquet directory ---
    target_parquet_dir = config.PARQUET_DIR / MACRO_TABLE_NAME
    if target_parquet_dir.exists():
        logger.info(f"Cleaning previous Parquet data from {target_parquet_dir}...")
        shutil.rmtree(target_parquet_dir)

    # 1. Get API Key from config
    api_key = config.get_optional_var("FRED_API_KEY")
    if not api_key:
        logger.critical("FRED_API_KEY not found in .env file. Cannot continue.")
        sys.exit(1)

    # 2. Initialize FRED API client
    try:
        fred = Fred(api_key=api_key)
        logger.info("Successfully initialized FRED API client.")
    except Exception as e:
        logger.critical(f"Failed to initialize FRED API client: {e}", exc_info=True)
        sys.exit(1)

    # 3. Fetch and process each series
    all_series_data = []
    api_delay = config.get_optional_float("FRED_API_DELAY", 0.5) # 500ms delay between calls

    for series_id in FRED_SERIES_IDS:
        logger.info(f"Processing series: {series_id}")
        try:
            # --- Compliance Check: Check for copyright restrictions ---
            series_info = fred.get_series_info(series_id)
            if 'copyright' in series_info.get('notes', '').lower():
                logger.warning(f"SKIPPING series '{series_id}' due to copyright restrictions found in notes.")
                continue

            # Fetch the series data
            series_df = fred.get_series(series_id).reset_index()
            series_df.columns = ['date', 'value']
            series_df['series_id'] = series_id

            # Drop rows with NaT dates or NaN values
            series_df.dropna(inplace=True)

            if not series_df.empty:
                all_series_data.append(series_df)
                logger.info(f"Successfully fetched {len(series_df)} data points for '{series_id}'.")

            # --- Compliance: Be a good citizen and delay between requests ---
            time.sleep(api_delay)

        except Exception as e:
            logger.error(f"Failed to fetch or process series '{series_id}': {e}", exc_info=True)

    # 4. Combine and save to Parquet
    if not all_series_data:
        logger.warning("No data was fetched. Exiting without creating Parquet file.")
    else:
        final_df = pd.concat(all_series_data, ignore_index=True)
        # Reorder columns to match the table schema for correct positional loading
        final_df = final_df[['series_id', 'date', 'value']]
        final_df = parquet_converter._prepare_df_for_storage(
            final_df,
            str_cols=['series_id'],
            date_cols=['date'],
            numeric_cols=['value']
        )
        parquet_converter.save_dataframe_to_parquet(final_df, config.PARQUET_DIR / MACRO_TABLE_NAME)

    end_time = time.time()
    logger.info("--- FRED Macroeconomic Data Pipeline Finished ---")
    logger.info(f"Total Time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
        run_macro_data_pipeline(app_config)
    except SystemExit as e:
        logger.critical(f"Configuration failed, cannot start: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred at startup: {e}", exc_info=True)
        sys.exit(1)