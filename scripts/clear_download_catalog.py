import duckdb
import sys
from pathlib import Path

# Add project root to sys.path to allow importing utils
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig

config = AppConfig(calling_script_path=Path(__file__))
db_file = config.DB_FILE_STR
print(f"Connecting to database: {db_file}")
with duckdb.connect(database=db_file, read_only=False) as con:
    print("Clearing the downloaded_archives table...")
    con.execute("DELETE FROM downloaded_archives;")
    print("Table cleared.")
