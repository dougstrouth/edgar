# -*- coding: utf-8 -*-
"""
Market Risk Data Gatherer (Fama-French)

Fetches key market risk factor data from the Fama-French Data Library,
cleans it, and saves it to a Parquet file.

This script uses the pandas-datareader library, which does not require an API key.
"""

import sys
import time
import logging
import warnings
import shutil
from pathlib import Path

import pandas as pd
import pandas_datareader.data as web



# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from data_processing import parquet_converter

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
# Fama-French datasets available via pandas-datareader
# We will fetch the 5-factor models (daily and monthly)
FAMA_FRENCH_DATASETS = {
    "F-F_Research_Data_5_Factors_2x3_daily": "ff_5_factor_daily",
    "F-F_Research_Data_5_Factors_2x3": "ff_5_factor_monthly",
}

MARKET_RISK_TABLE_NAME = "market_risk_factors"

def run_market_risk_pipeline(config: AppConfig):
    """
    Main orchestration function for the Fama-French data gathering pipeline.
    """
    logger.info("--- Starting Fama-French Market Risk Data Pipeline ---")
    start_time = time.time()

    # --- Cleanliness Step ---
    target_parquet_dir = config.PARQUET_DIR / MARKET_RISK_TABLE_NAME
    if target_parquet_dir.exists():
        logger.info(f"Cleaning previous Parquet data from {target_parquet_dir}...")
        shutil.rmtree(target_parquet_dir)

    all_factors_data = []
    api_delay = 0.5 # Polite delay between requests

    for dataset_name, model_name in FAMA_FRENCH_DATASETS.items():
        logger.info(f"Processing dataset: {dataset_name}")
        try:
            # Fetch the data. The result is a dictionary of DataFrames.
            # We are interested in the first one (index 0) which contains the factors.
            # Suppress the date_parser FutureWarning from pandas_datareader as we handle parsing manually.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                ff_data = web.DataReader(dataset_name, 'famafrench', start='1900-01-01')[0]

            ff_data.reset_index(inplace=True)
            
            ff_data.rename(columns={'Date': 'date', 'Mkt-RF': 'mkt_minus_rf'}, inplace=True)
            ff_data['factor_model'] = model_name

            # Convert percentage values to decimals
            for col in ['mkt_minus_rf', 'SMB', 'HML', 'RMW', 'CMA', 'RF']:
                if col in ff_data.columns:
                    ff_data[col] = pd.to_numeric(ff_data[col], errors='coerce') / 100.0

            # Standardize the date column
            if isinstance(ff_data['date'].dtype, pd.PeriodDtype):
                 ff_data['date'] = ff_data['date'].dt.to_timestamp().dt.date
            else:
                 ff_data['date'] = pd.to_datetime(ff_data['date']).dt.date

            # Normalize factor column names to lowercase to align with DuckDB schema
            rename_map = {c: c.lower() for c in ['SMB', 'HML', 'RMW', 'CMA', 'RF'] if c in ff_data.columns}
            ff_data.rename(columns=rename_map, inplace=True)

            # Basic duplicate diagnostics (per date within this model)
            if ff_data.duplicated(subset=['date']).any():
                dup_count = ff_data.duplicated(subset=['date']).sum()
                logger.warning(f"Detected {dup_count} duplicate date rows within model '{model_name}'. Keeping first occurrences.")
                ff_data = ff_data.drop_duplicates(subset=['date'], keep='first')

            all_factors_data.append(ff_data)
            logger.info(f"Successfully fetched and processed {len(ff_data)} data points for '{dataset_name}'.")
            time.sleep(api_delay)

        except Exception as e:
            logger.error(f"Failed to fetch or process dataset '{dataset_name}': {e}", exc_info=True)

    if all_factors_data:
        final_df = pd.concat(all_factors_data, ignore_index=True)
        # Unified column order matching DuckDB schema (lowercase factor names)
        cols_order = ['date', 'factor_model', 'mkt_minus_rf', 'smb', 'hml', 'rmw', 'cma', 'rf']
        final_df = final_df[[c for c in cols_order if c in final_df.columns]]

        # Drop any cross-model duplicates on (date, factor_model) just in case
        before = len(final_df)
        final_df = final_df.drop_duplicates(subset=['date', 'factor_model'], keep='first')
        after = len(final_df)
        if after < before:
            logger.info(f"Removed {before - after} cross-model duplicate rows (date, factor_model).")

        parquet_converter.save_dataframe_to_parquet(final_df, config.PARQUET_DIR / MARKET_RISK_TABLE_NAME)

    end_time = time.time()
    logger.info(f"--- Fama-French Market Risk Data Pipeline Finished ---")
    logger.info(f"Total Time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
        run_market_risk_pipeline(app_config)
    except Exception as e:
        logger.critical(f"An unexpected error occurred at startup: {e}", exc_info=True)
        sys.exit(1)