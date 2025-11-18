
import duckdb
import os
import sys
from pathlib import Path

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

try:
    from utils.config_utils import AppConfig
except ImportError as e:
    print(f"FATAL: Could not import utility modules: {e}", file=sys.stderr)
    sys.exit(1)

try:
    config = AppConfig(calling_script_path=Path(__file__))
    db_file = config.DB_FILE_STR
    print(f"Connecting to database: {db_file}")
    con = duckdb.connect(database=db_file, read_only=False)
    
    print("Clearing the downloaded_archives table...")
    con.execute("DELETE FROM downloaded_archives;")
    print("Table cleared.")

except Exception as e:
    print(f"An error occurred: {e}", file=sys.stderr)
    sys.exit(1)

