# -*- coding: utf-8 -*-
"""
Main Orchestrator for the EDGAR Data Pipeline.

This script provides a command-line interface (CLI) to run the various
stages of the data processing pipeline, including fetching archives, loading
data into the database, gathering supplementary stock data, and validation.

Usage:
    - Run the full pipeline in order:
      python main.py
      python main.py all

    - Run a specific step:
      python main.py fetch
      python main.py parse-to-parquet
      python main.py load
      python main.py gather-stocks
      python main.py gather-info
      python main.py cleanup
      python main.py validate


"""

import argparse
import sys
import logging
import runpy
from pathlib import Path
import time
from typing import Optional, List
import multiprocessing
import os

# Ensure all subdirectories are in the Python path.
# This allows for cleaner imports in the individual scripts.
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / 'utils'))
sys.path.append(str(PROJECT_ROOT / 'data_gathering'))
sys.path.append(str(PROJECT_ROOT / 'data_processing'))

try:
    # These are for the orchestrator's own logging and config awareness.
    # The individual scripts will load their own instances when run.
    from utils.config_utils import AppConfig
    from utils.logging_utils import setup_logging
except ImportError as e:
    print(f"FATAL: Could not import utility modules. Ensure that the 'utils' directory is in the Python path. Error: {e}", file=sys.stderr)
    sys.exit(1)

# --- Setup Logging for the orchestrator itself ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Script Definitions ---
# Get the directory where this main.py script is located.
SCRIPT_DIR = Path(__file__).resolve().parent

SCRIPTS = {
    "fetch": SCRIPT_DIR / "data_gathering/fetch_edgar_archives.py",
    "parse-to-parquet": SCRIPT_DIR / "data_processing/parse_to_parquet.py",
    "json_to_duckdb": SCRIPT_DIR / "data_processing/json_to_duckdb_refactored.py",
    "load": SCRIPT_DIR / "data_processing/edgar_data_loader.py",
    "generate_backlog": SCRIPT_DIR / "scripts/generate_prioritized_backlog.py",
    "stage_fetch_plan": SCRIPT_DIR / "scripts/stage_stock_fetch_plan.py",
    "gather_stocks": SCRIPT_DIR / "data_gathering/stock_data_gatherer.py",
    "gather_stocks_polygon": SCRIPT_DIR / "data_gathering/stock_data_gatherer_polygon.py",
    "gather-stocks-polygon": SCRIPT_DIR / "data_gathering/stock_data_gatherer_polygon.py",
    "enrich_tickers": SCRIPT_DIR / "data_gathering/ticker_enrichment_massive.py",
    "enrich-tickers": SCRIPT_DIR / "data_gathering/ticker_enrichment_massive.py",
    "gather_ticker_info": SCRIPT_DIR / "data_gathering/ticker_info_gatherer_polygon.py",
    "gather-ticker-info": SCRIPT_DIR / "data_gathering/ticker_info_gatherer_polygon.py",
    "gather_info": SCRIPT_DIR / "data_gathering/stock_info_gatherer.py",
    "load_info": SCRIPT_DIR / "data_processing/load_supplementary_data.py",
    "load_ticker_info": SCRIPT_DIR / "data_processing/load_ticker_info.py",
    "load-ticker-info": SCRIPT_DIR / "data_processing/load_ticker_info.py",
    "gather_macro": SCRIPT_DIR / "data_gathering/macro_data_gatherer.py",
    "load_macro": SCRIPT_DIR / "data_processing/load_supplementary_data.py",
    "gather_market_risk": SCRIPT_DIR / "data_gathering/market_risk_gatherer.py",
    "load_market_risk": SCRIPT_DIR / "data_processing/load_supplementary_data.py",
    "load_stocks": SCRIPT_DIR / "data_processing/load_supplementary_data.py",
    "feature_eng": SCRIPT_DIR / "data_processing/feature_engineering.py",
    "summarize": SCRIPT_DIR / "data_processing/summarize_filings.py",
    "validate": SCRIPT_DIR / "utils/validate_edgar_db.py",
    "cleanup": SCRIPT_DIR / "scripts/cleanup_artifacts.py",
    "investigate_orphans": SCRIPT_DIR / "investigate_orphans.py",
    "inspect": SCRIPT_DIR / "scripts/downloads_scanner.py",
}

def run_script_target(script_path, script_args):
    """Target function for multiprocessing to execute the script."""
    original_argv = sys.argv
    sys.argv = [str(script_path)] + (script_args if script_args else [])
    try:
        runpy.run_path(str(script_path), run_name='__main__')
    finally:
        sys.argv = original_argv

def run_script(script_key: str, script_args: Optional[List[str]] = None, timeout: Optional[int] = None) -> bool:
    """
    Runs a script defined in the SCRIPTS dictionary using runpy with a timeout.

    Args:
        script_key: The key corresponding to the script to run.
        script_args: A list of command-line arguments to pass to the script.
        timeout: The maximum time in seconds to wait for the script to complete.

    Returns:
        True if the script ran successfully, False otherwise.
    """
    script_path = SCRIPTS.get(script_key)
    if not script_path or not script_path.is_file():
        logger.error(f"Script for '{script_key}' not found at: {script_path}")
        return False

    logger.info(f"---===[ Running Step: {script_key.upper()} ]===---")

    start_time = time.time()
    process = multiprocessing.Process(target=run_script_target, args=(script_path, script_args))
    process.start()
    process.join(timeout)

    if process.is_alive():
        logger.warning(f"---===[ Step '{script_key.upper()}' timed out after {timeout} seconds. Terminating... ]===---")
        process.terminate()
        process.join()
        logger.warning(f"---===[ Step '{script_key.upper()}' terminated. ]===---")
        return True # Return True to allow the pipeline to continue

    end_time = time.time()
    if process.exitcode == 0:
        logger.info(f"---===[ Finished Step: {script_key.upper()} in {end_time - start_time:.2f}s ]===---")
        return True
    else:
        logger.error(f"---===[ Step '{script_key.upper()}' FAILED with exit code {process.exitcode} in {end_time - start_time:.2f}s ]===---")
        return False

def main():
    """Parses command-line arguments and runs the requested pipeline steps."""
    parser = argparse.ArgumentParser(description="Orchestrator for the EDGAR Data Pipeline.")
    parser.add_argument(
        "step",
        nargs="?",
        default="all",
        choices=[
            "all", "fetch", "parse-to-parquet", "load", "summarize", "validate", "cleanup", "feature_eng",
            "gather_info", "load_info", "gather_macro", "load_macro",
            "gather_market_risk", "load_market_risk", "investigate_orphans", "inspect",
            "gather_stocks_polygon", "gather-stocks-polygon", "load_stocks", "generate_backlog",
            "enrich_tickers", "enrich-tickers", "gather_ticker_info", "gather-ticker-info",
            "load_ticker_info", "load-ticker-info"
        ],
        help="The pipeline step to run. 'all' runs every step in sequence. Default is 'all'."
    )

    args, remaining_args = parser.parse_known_args()

    # Load config to log paths, though individual scripts will load it too.
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        logger.info(f"Orchestrator configured. DB target: {config.DB_FILE_STR}")
    except SystemExit:
        logger.critical("Configuration failed. Check .env file. Exiting orchestrator.")
        sys.exit(1)

    if args.step == "all":
        # Run cleanup at the end to free up space
        pipeline_steps_with_args = [
            ("fetch", None, None), # Fetches zips
            ("parse-to-parquet", None, None), # Parses JSON to Parquet
            ("load", None, None), # Loads Parquet into DuckDB
            ("validate", None, None),
            ("gather_macro", None, None),
            ("load_macro", ["macro_economic_data", "--full-refresh"], None),
            ("validate", None, None),
            ("gather_market_risk", None, None),
            ("load_market_risk", ["market_risk_factors", "--full-refresh"], None),
            ("validate", None, None),
            ("cleanup", ['--all', '--cache'], None)
        ]
        logger.info("Running full pipeline...")
        for step_name, script_args, timeout in pipeline_steps_with_args:
            if not run_script(step_name, script_args=script_args, timeout=timeout):
                logger.error(f"Full pipeline stopped due to failure in step: '{step_name}'.")
                sys.exit(1)
        logger.info("Full pipeline completed successfully!")
    else:
        # Map CLI argument to script key
        script_key = args.step

        # Prepare arguments for specific loader scripts that require a source name
        final_args = remaining_args
        if script_key == "load_macro":
            final_args = ["macro_economic_data"] + remaining_args
        elif script_key == "load_info":
            final_args = ["yf_info_fetch_errors", "yf_income_statement", "yf_balance_sheet", "yf_cash_flow"] + remaining_args
        elif script_key == "load_market_risk":
            final_args = ["market_risk_factors"] + remaining_args
        elif script_key == "load_stocks":
            final_args = ["stock_history"] + remaining_args

        if not run_script(script_key, script_args=final_args):
            sys.exit(1)

if __name__ == "__main__":
    main()