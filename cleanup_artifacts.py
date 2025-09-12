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
import logging
from pathlib import Path

# --- Import Utilities ---
from config_utils import AppConfig
from logging_utils import setup_logging

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

def main():
    """Main function to parse arguments and perform cleanup."""
    parser = argparse.ArgumentParser(description="Clean up intermediate data artifacts.")
    parser.add_argument("--json", action="store_true", help="Remove the extracted_json directory.")
    parser.add_argument("--parquet", action="store_true", help="Remove the parquet_data directory.")
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
        logger.warning("No cleanup target specified. Use --json, --parquet, or --all. Exiting.")
        sys.exit(0)

    if args.json or args.all:
        remove_directory(config.EXTRACT_BASE_DIR, logger)

    if args.parquet or args.all:
        remove_directory(config.PARQUET_DIR, logger)

    logger.info("--- Artifact Cleanup Finished ---")

if __name__ == "__main__":
    main()