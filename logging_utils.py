# -*- coding: utf-8 -*-
"""
Logging Configuration Utility

Provides a standardized function to set up logging with both
file and console (stream) handlers for project scripts.
"""

import logging
import sys
from pathlib import Path

def setup_logging(
    script_name: str,
    log_dir: Path,
    level: int = logging.INFO,
    log_format: str = '%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s] %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S'
) -> logging.Logger:
    """
    Sets up logging for a script with standardized file and stream handlers.

    Args:
        script_name: The name of the script/module (used for logger name and log file name).
        log_dir: The directory where the log file should be stored.
        level: The logging level (e.g., logging.INFO, logging.DEBUG).
        log_format: The format string for log messages.
        date_format: The format string for the date/time in log messages.

    Returns:
        A configured logging.Logger instance.

    Raises:
        OSError: If the log directory cannot be created.
    """
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"FATAL: Could not create log directory {log_dir}: {e}", file=sys.stderr)
        raise

    log_file_path = log_dir / f"{script_name}.log"

    # --- Configure the specific logger ---
    logger = logging.getLogger(script_name)
    logger.setLevel(level)

    # Prevent adding handlers multiple times if function is called again
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # Create and add File Handler
    try:
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as e:
        print(f"Warning: Could not set up file handler for {log_file_path}: {e}", file=sys.stderr)


    # Create and add Stream Handler (console)
    sh = logging.StreamHandler(sys.stdout) # Explicitly use stdout
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # --- Optional: Configure root logger minimally if needed ---
    # This prevents libraries used (like requests) from potentially flooding
    # if they log extensively at DEBUG level and the root logger isn't configured.
    # logging.basicConfig(level=logging.WARNING) # Example: Set root logger higher

    logger.info(f"Logging configured for '{script_name}'. Log file: {log_file_path}")
    return logger

# --- Example Usage (within another script) ---
# if __name__ == "__main__":
#     SCRIPT_NAME = Path(__file__).stem # Get name like 'my_script' from 'my_script.py'
#     LOG_DIRECTORY = Path(__file__).resolve().parent / "logs" # Example: logs subdir
#
#     logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.DEBUG)
#
#     logger.debug("This is a debug message.")
#     logger.info("This is an info message.")
#     logger.warning("This is a warning.")
#     logger.error("This is an error.")
#     try:
#         1 / 0
#     except ZeroDivisionError:
#         logger.exception("This is an exception message.")