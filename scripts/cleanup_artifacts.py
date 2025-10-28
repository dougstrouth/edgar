# -*- coding: utf-8 -*-
"""
Pipeline Artifact Cleanup Utility

Provides functionality to safely remove intermediate data artifacts created
during the pipeline run, such as the large extracted JSON directory or the
intermediate Parquet files. This helps manage disk space.
"""

import sys
import shutil
import argparse
import logging, glob
from pathlib import Path

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging

def remove_directory(dir_path: Path, logger: logging.Logger):
    """Safely removes a directory and all its contents."""
    if dir_path.exists() and dir_path.is_dir():
        logger.info(f"Removing directory: {dir_path}")
        try:
            shutil.rmtree(dir_path)
            logger.info(f"Successfully removed {dir_path}")
        except OSError as e:
            logger.error(f"Error removing directory {dir_path}: {e}", exc_info=True)
    else:
        logger.warning(f"Directory not found, skipping removal: {dir_path}")

def remove_glob_patterns(root_dir: Path, patterns: list[str], logger: logging.Logger):
    """Safely removes directories and files matching glob patterns in the root directory."""
    for pattern in patterns:
        logger.info(f"Searching for items matching '{pattern}' in {root_dir}...")
        # Use glob.glob to handle this more robustly
        for path_str in glob.glob(str(root_dir / pattern)):
            path_to_remove = Path(path_str)
            if path_to_remove.is_dir():
                remove_directory(path_to_remove, logger)
            elif path_to_remove.is_file():
                try:
                    path_to_remove.unlink()
                    logger.info(f"Successfully removed file {path_to_remove}")
                except OSError as e:
                    logger.error(f"Error removing file {path_to_remove}: {e}", exc_info=True)

def main():
    """Main function to parse arguments and perform cleanup."""
    parser = argparse.ArgumentParser(description="Clean up intermediate data artifacts.")
    parser.add_argument("--json", action="store_true", help="Remove the extracted_json directory.")
    parser.add_argument("--parquet", action="store_true", help="Remove the parquet_data directory.")
    parser.add_argument("--cache", action="store_true", help="Remove temporary cache directories (e.g., .yfinance_cache_*, .mypy_cache, .cache).")
    parser.add_argument("--all", action="store_true", help="Remove all intermediate artifact directories (json and parquet).")

    args = parser.parse_args()

    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    logger.info("--- Starting Artifact Cleanup ---")

    if not any([args.json, args.parquet, args.all]):
        logger.warning("No cleanup target specified. Use --json, --parquet, --cache, or --all. Exiting.")
        sys.exit(0)

    if args.json or args.all:
        remove_directory(config.EXTRACT_BASE_DIR, logger)

    if args.parquet or args.all:
        remove_directory(config.PARQUET_DIR, logger)

    if args.cache:
        # Remove the new centralized cache and any old lingering ones
        patterns_to_remove = [".cache", ".mypy_cache", ".yfinance_cache_*"]
        remove_glob_patterns(config.PROJECT_ROOT, patterns_to_remove, logger)

    logger.info("--- Artifact Cleanup Finished ---")

if __name__ == "__main__":
    main()