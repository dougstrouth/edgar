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

This script assumes all other project scripts (fetch_edgar_archives.py,
edgar_data_loader.py, etc.) are in the same directory.
"""

import argparse
import sys
import logging
import runpy
from pathlib import Path
import time
from typing import Optional, List

# Ensure utility modules can be found. This is generally good practice,
# especially if the script is run from a different working directory.
sys.path.append(str(Path(__file__).resolve().parent))

try:
    # These are for the orchestrator's own logging and config awareness.
    # The individual scripts will load their own instances when run.
    from config_utils import AppConfig
    from logging_utils import setup_logging
except ImportError as e:
    print(f"FATAL: Could not import utility modules. Make sure they are in the same directory or Python path: {e}", file=sys.stderr)
    sys.exit(1)

# --- Setup Logging for the orchestrator itself ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Script Definitions ---
# Get the directory where this main.py script is located.
SCRIPT_DIR = Path(__file__).resolve().parent

SCRIPTS = {
    "fetch": SCRIPT_DIR / "fetch_edgar_archives.py",
    "parse_to_parquet": SCRIPT_DIR / "parse_to_parquet.py",
    "load": SCRIPT_DIR / "edgar_data_loader.py", # This now loads from Parquet
    "gather_stocks": SCRIPT_DIR / "stock_data_gatherer.py",
    "gather_info": SCRIPT_DIR / "stock_info_gatherer.py",
    "load_stocks": SCRIPT_DIR / "load_supplementary_data.py",
    "load_info": SCRIPT_DIR / "load_supplementary_data.py",
    "gather_macro": SCRIPT_DIR / "macro_data_gatherer.py",
    "load_macro": SCRIPT_DIR / "load_supplementary_data.py",
    "gather_market_risk": SCRIPT_DIR / "market_risk_gatherer.py",
    "load_market_risk": SCRIPT_DIR / "load_supplementary_data.py",
    "feature_eng": SCRIPT_DIR / "feature_engineering.py",
    "validate": SCRIPT_DIR / "validate_edgar_db.py",
    "cleanup": SCRIPT_DIR / "cleanup_artifacts.py",
}

def run_script(script_key: str, script_args: Optional[List[str]] = None) -> bool:
    """
    Runs a script defined in the SCRIPTS dictionary using runpy.

    Args:
        script_key: The key corresponding to the script to run.

    Returns:
        script_args: A list of command-line arguments to pass to the script.
        True if the script ran successfully, False otherwise.
    """
    script_path = SCRIPTS.get(script_key)
    if not script_path or not script_path.is_file():
        logger.error(f"Script for '{script_key}' not found at: {script_path}")
        return False

    logger.info(f"---===[ Running Step: {script_key.upper()} ]===---")

    start_time = time.time()
    try:
        # Temporarily replace sys.argv for the script being run
        original_argv = sys.argv
        # The first element of argv is the script name, followed by args
        sys.argv = [str(script_path)] + (script_args if script_args else [])

        # run_path executes the script as if it were the main entry point.
        # This correctly triggers the `if __name__ == "__main__"` block in each script.
        runpy.run_path(str(script_path), run_name='__main__')
        end_time = time.time()
        logger.info(f"---===[ Finished Step: {script_key.upper()} in {end_time - start_time:.2f}s ]===---")
        return True
    except SystemExit as e:
        # Scripts might call sys.exit(). A non-zero code indicates an error.
        end_time = time.time()
        if e.code != 0 and e.code is not None:
            logger.error(f"---===[ Step '{script_key.upper()}' FAILED with exit code {e.code} in {end_time - start_time:.2f}s ]===---")
            return False
        logger.info(f"---===[ Finished Step: {script_key.upper()} with exit code {e.code} in {end_time - start_time:.2f}s ]===---")
        return True
    except Exception as e:
        end_time = time.time()
        logger.critical(f"---===[ Step '{script_key.upper()}' FAILED with an unhandled exception in {end_time - start_time:.2f}s ]===---", exc_info=True)
        return False
    finally:
        sys.argv = original_argv # Always restore the original sys.argv

def main():
    """Parses command-line arguments and runs the requested pipeline steps."""
    parser = argparse.ArgumentParser(description="Orchestrator for the EDGAR Data Pipeline.")
    parser.add_argument(
        "step",
        nargs="?",
        default="all",
        choices=[
            "all", "fetch", "parse_to_parquet", "load", "validate", "cleanup", "feature_eng",
            "gather_stocks", "load_stocks", "gather_info", "load_info", "gather_macro", "load_macro",
            "gather_market_risk", "load_market_risk"
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
            ("fetch", None),
            ("parse_to_parquet", None),
            ("load", None),
            ("gather_stocks", ["--full-refresh"]),
            ("load_stocks", ["stock_history", "stock_fetch_errors", "yf_untrackable_tickers", "--full-refresh"]),
            ("gather_info", None),
            ("load_info", ["all_yf", "--full-refresh"]),
            ("gather_macro", None),
            ("load_macro", ["macro_economic_data", "--full-refresh"]),
            ("gather_market_risk", None),
            ("load_market_risk", ["market_risk_factors", "--full-refresh"]),
            ("validate", None),
            ("cleanup", ['--all', '--cache'])
        ]
        logger.info("Running full pipeline...")
        for step_name, script_args in pipeline_steps_with_args:
            if not run_script(step_name, script_args=script_args):
                logger.error(f"Full pipeline stopped due to failure in step: '{step_name}'.")
                sys.exit(1)
        logger.info("Full pipeline completed successfully!")
    else:
        # Map CLI argument to script key
        script_key = args.step

        # Prepare arguments for specific loader scripts that require a source
        final_args = remaining_args
        if script_key == "load_macro":
            final_args = ["macro_economic_data"] + remaining_args
        elif script_key == "load_stocks":
            final_args = ["stock_history", "stock_fetch_errors", "yf_untrackable_tickers"] + remaining_args
        elif script_key == "load_info":
            final_args = ["all_yf"] + remaining_args
        elif script_key == "load_market_risk":
            final_args = ["market_risk_factors"] + remaining_args

        if not run_script(script_key, script_args=final_args):
            sys.exit(1)

if __name__ == "__main__":
    main()