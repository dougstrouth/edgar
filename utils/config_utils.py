# -*- coding: utf-8 -*-
"""
Configuration Loading Utility

Provides a class to load essential configuration variables (like database path,
download directories) from a .env file located relative to the calling script
or in a standard project location.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

# Set up a logger specifically for config loading issues
config_logger = logging.getLogger(__name__)
# Basic config in case logging isn't set up by the main script yet
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')


class AppConfig:
    """
    Loads and holds common application configuration settings from a .env file.

    Looks for .env in the same directory as the calling script first,
    then potentially in the parent directory if needed.

    Attributes:
        DOWNLOAD_DIR (Path): Resolved path to the main download directory.
        DB_FILE (Path): Resolved path to the DuckDB database file.
        PROJECT_ROOT (Path): Resolved path to the project's root directory.
        DB_FILE_STR (str): String representation of the DB_FILE path.
        EXTRACT_BASE_DIR (Path): Resolved path to the base for extracted files.
        SEC_USER_AGENT (str): The User-Agent string for SEC API requests.
        PARQUET_DIR (Path): Resolved path for intermediate Parquet files.
        SUBMISSIONS_DIR (Path): Resolved path to extracted submissions.
        COMPANYFACTS_DIR (Path): Resolved path to extracted companyfacts.
        TICKER_FILE_PATH (Path): Resolved path to the company_tickers.json file.
        env_file_path (Optional[Path]): Path to the loaded .env file, if found.
    """

    def __init__(self, calling_script_path: Optional[Path] = None):
        """
        Initializes the configuration by loading the .env file and variables.

        Args:
            calling_script_path: The __file__ path of the script calling this config.
                                 Used to find the .env file relatively. If None,
                                 assumes .env is in the current working directory or parent.
        """
        self.env_file_path: Optional[Path] = None
        if calling_script_path:
            self.PROJECT_ROOT = calling_script_path.resolve().parent
            self.env_file_path = self.PROJECT_ROOT / '.env'
            if not self.env_file_path.is_file():
                # Try parent directory as a fallback
                parent_env_path = self.PROJECT_ROOT.parent / '.env'
                if parent_env_path.is_file():
                    # If .env is in parent, assume parent is project root
                    self.PROJECT_ROOT = self.PROJECT_ROOT.parent
                    self.env_file_path = parent_env_path
                else:
                    config_logger.warning(f".env file not found in {script_dir} or {script_dir.parent}")
                    self.env_file_path = None # Explicitly mark as not found
        else:
            # If no script path given, check CWD then parent
            cwd = Path.cwd()
            self.PROJECT_ROOT = cwd
            self.env_file_path = cwd / '.env'
            if not self.env_file_path.is_file():
                 parent_env_path = cwd.parent / '.env'
                 if parent_env_path.is_file():
                      self.env_file_path = parent_env_path
                      self.PROJECT_ROOT = cwd.parent
                 else:
                      config_logger.warning(f".env file not found in {cwd} or {cwd.parent}")
                      self.env_file_path = None

        if self.env_file_path:
            load_dotenv(dotenv_path=self.env_file_path, override=True)
            config_logger.info(f"Loaded environment variables from: {self.env_file_path}")
        else:
            config_logger.warning("No .env file loaded. Relying on environment variables.")
            # Proceed, assuming variables might be set directly in the environment

        # Load essential variables
        try:
            download_dir_str = os.environ['DOWNLOAD_DIR']
            db_file_str = os.environ['DB_FILE']
            self.SEC_USER_AGENT: str = os.environ['SEC_USER_AGENT']

            self.DOWNLOAD_DIR: Path = Path(download_dir_str).resolve()
            self.DB_FILE: Path = Path(db_file_str).resolve()
            self.DB_FILE_STR: str = str(self.DB_FILE) # Convenience string version

            # Create directories if they don't exist (optional, could be handled by scripts)
            # self.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            # self.DB_FILE.parent.mkdir(parents=True, exist_ok=True)

            # Derive other common paths
            self.EXTRACT_BASE_DIR: Path = self.DOWNLOAD_DIR / "extracted_json"
            self.PARQUET_DIR: Path = self.DOWNLOAD_DIR / "parquet_data"
            self.SUBMISSIONS_DIR: Path = self.EXTRACT_BASE_DIR / "submissions"
            self.COMPANYFACTS_DIR: Path = self.EXTRACT_BASE_DIR / "companyfacts"
            self.TICKER_FILE_PATH: Path = self.DOWNLOAD_DIR / "company_tickers.json"

            # --- Performance & Tuning Settings ---
            self.DUCKDB_MEMORY_LIMIT: str = self.get_optional_var("DUCKDB_MEMORY_LIMIT", '8GB')
            self.DUCKDB_TEMP_DIR: Optional[Path] = (
                Path(temp_dir_str).resolve()
                if (temp_dir_str := self.get_optional_var("DUCKDB_TEMP_DIR"))
                else None
            )
            self.MAX_CPU_IO_WORKERS: int = self.get_optional_int("MAX_CPU_IO_WORKERS", 8)


            config_logger.info("Core configuration paths loaded successfully.")
            config_logger.info(f"  DOWNLOAD_DIR: {self.DOWNLOAD_DIR}")
            config_logger.info(f"  PARQUET_DIR: {self.PARQUET_DIR}")
            config_logger.info(f"  DB_FILE: {self.DB_FILE}")

            # Validate the User-Agent
            if "PersonalResearchProject" in self.SEC_USER_AGENT or "your.email@example.com" in self.SEC_USER_AGENT:
                config_logger.warning("SEC_USER_AGENT in .env seems generic. Update it with your specific project/contact info.")
            elif not self.SEC_USER_AGENT:
                raise KeyError("SEC_USER_AGENT")

        except KeyError as e:
            msg = f"FATAL: Missing required environment variable: {e}. Check .env file or environment."
            config_logger.error(msg)
            sys.exit(msg) # Exit if core config is missing
        except Exception as e:
            msg = f"FATAL: Error initializing configuration paths: {e}"
            config_logger.exception(msg) # Log full traceback
            sys.exit(msg)

    # --- Optional: Helpers for script-specific variables ---
    def get_optional_var(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Gets an optional string variable from the environment."""
        return os.environ.get(key, default)

    def get_optional_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        """Gets an optional integer variable from the environment."""
        val = os.environ.get(key)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                config_logger.warning(f"Could not parse env var '{key}' value '{val}' as int. Using default.")
                return default
        return default

    def get_optional_float(self, key: str, default: Optional[float] = None) -> Optional[float]:
        """Gets an optional float variable from the environment."""
        val = os.environ.get(key)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                config_logger.warning(f"Could not parse env var '{key}' value '{val}' as float. Using default.")
                return default
        return default

    def get_optional_bool(self, key: str, default: bool = False) -> bool:
        """Gets an optional boolean variable from the environment."""
        val = os.environ.get(key, '').lower()
        if val in ('true', '1', 'yes', 'y', 'on'):
            return True
        if val in ('false', '0', 'no', 'n', 'off'):
            return False
        return default

# --- Example Usage (within another script) ---
# if __name__ == "__main__":
#     # In your main script (e.g., edgar_data_loader.py)
#     # Assuming this config_utils.py is in the same directory or python path
#
#     # Pass the path of the *current* script to find .env relatively
#     try:
#         config = AppConfig(calling_script_path=Path(__file__))
#         print(f"DB File from config: {config.DB_FILE}")
#         print(f"Submissions dir: {config.SUBMISSIONS_DIR}")
#
#         # Get an optional script-specific setting
#         my_limit = config.get_optional_int("MY_SCRIPT_LIMIT", 100)
#         print(f"My script limit: {my_limit}")
#
#     except SystemExit as e:
#          print(f"Exited during config load: {e}")
#     except Exception as e:
#          print(f"An unexpected error occurred: {e}")