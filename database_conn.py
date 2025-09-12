# -*- coding: utf-8 -*-
"""
Database Connection Utility for DuckDB.

Provides a function and a context manager for establishing
and managing connections to the project's DuckDB database,
configured via the .env file.
"""

import duckdb
import os
import logging
from pathlib import Path
from typing import Optional, Union
from typing import Dict, Any

# Assuming the calling script will load dotenv,
# so os.environ['DB_FILE'] should be available.

def get_db_connection(
    db_path_override: Optional[Union[str, Path]] = None,
    read_only: bool = False,
    pragma_settings: Optional[Dict[str, Any]] = None
) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Establishes a connection to the DuckDB database.

    Reads the DB file path from the 'DB_FILE' environment variable,
    unless overridden by db_path_override. Handles ':memory:' databases.

    Args:
        db_path_override: If provided, uses this path (str or Path object)
                          instead of DB_FILE env var. Use ':memory:'.
        read_only: Set to True to open the database in read-only mode.
        pragma_settings: A dictionary of PRAGMA settings to apply on connect.

    Returns:
        A DuckDB connection object or None if connection fails.

    Raises:
        KeyError: If 'DB_FILE' is not set in environment variables and
                  db_path_override is not provided.
        Exception: Catches and logs DuckDB connection errors.
    """
    conn = None
    db_path_str = ""
    final_db_path = None # Store the Path object if it's a file

    if db_path_override:
        # Handle both str and Path objects for the override
        db_path_str = str(db_path_override)
        logging.info(f"Using override DB path: {db_path_str}")
        if db_path_override != ':memory:':
             final_db_path = Path(db_path_override) # Convert to Path if not already
    else:
        try:
            db_path_str = os.environ['DB_FILE']
            final_db_path = Path(db_path_str)
            logging.info(f"Using DB path from environment: {db_path_str}")
        except KeyError:
            logging.error("DB_FILE environment variable not set and no override provided.")
            raise # Re-raise KeyError if DB_FILE is mandatory and not overridden

    if not db_path_str:
        logging.error("Database path is empty after checking override and environment.")
        return None

    # --- Connection Logic ---    
    is_memory = (db_path_str == ':memory:')
    try:
        # Ensure parent directory exists only if it's a file path
        if final_db_path:
             if not final_db_path.exists() and not read_only:
                 logging.warning(f"DB path does not exist: {final_db_path}. Attempting creation.")
                 try: final_db_path.parent.mkdir(parents=True, exist_ok=True)
                 except Exception as dir_e: logging.error(f"Cannot create dir {final_db_path.parent}: {dir_e}"); return None

             elif not final_db_path.is_file() and read_only:
                  logging.error(f"Database file not found for read-only connection: {final_db_path}")
                  return None

        # Connect using the string path (works for :memory: and file paths)
        conn = duckdb.connect(database=db_path_str, read_only=read_only)

        # --- Apply PRAGMA settings ---
        if conn and pragma_settings:
            for key, value in pragma_settings.items():
                try:
                    # Use proper formatting for string values
                    if isinstance(value, str):
                        conn.execute(f"PRAGMA {key} = '{value}';")
                    else:
                        conn.execute(f"PRAGMA {key} = {value};")
                    logging.info(f"Set PRAGMA {key} = {value}")
                except Exception as pragma_e:
                    logging.warning(f"Could not set PRAGMA {key} = {value}: {pragma_e}")

        logging.info(f"Connected: {db_path_str} (RO: {read_only})")
        return conn
    except Exception as e:
        logging.error(f"Failed connection to {db_path_str}: {e}", exc_info=True)
        if conn: # Attempt to close if connection object exists but failed init
            try: conn.close()
            except: pass
        return None


class ManagedDatabaseConnection:
    """
    Context manager for DuckDB connections using get_db_connection.

    Ensures the database connection is closed automatically.

    Usage:
        with ManagedDatabaseConnection(read_only=True) as conn:
            if conn:
                # Perform database operations
                results = conn.execute("SELECT 42").fetchall()
            else:
                # Handle connection failure
                print("Failed to connect to database.")
    """
    def __init__(self, db_path_override: Optional[str] = None, read_only: bool = False, pragma_settings: Optional[Dict[str, Any]] = None):
        self._db_path_override = db_path_override
        self._read_only = read_only
        self._pragma_settings = pragma_settings
        self.connection = None

    def __enter__(self) -> Optional[duckdb.DuckDBPyConnection]:
        """Establishes the database connection."""
        self.connection = get_db_connection(
            db_path_override=self._db_path_override,
            read_only=self._read_only,
            pragma_settings=self._pragma_settings
        )
        # Allow the 'with' block to check if connection is None
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the database connection."""
        if self.connection:
            try:
                self.connection.close()
                logging.debug(f"DB connection closed by context manager.")
            except Exception as e:
                logging.error(f"Error closing DB connection in context manager: {e}", exc_info=True)
        # Return False to propagate exceptions, True to suppress them
        return False