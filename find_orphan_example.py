
import duckdb
import sys
from pathlib import Path

# Add project root to sys.path to allow importing utils
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

try:
    from utils.config_utils import AppConfig
except ImportError as e:
    print(f"FATAL: Could not import utility modules: {e}", file=sys.stderr)
    sys.exit(1)

def find_first_orphan(db_file: str):
    """Connects to the database and retrieves the first orphaned fact found."""
    try:
        with duckdb.connect(database=db_file, read_only=True) as con:
            print(f"Successfully connected to {db_file}")

            # Check if the table exists
            tables = con.execute("SHOW TABLES;").fetchall()
            if 'xbrl_facts_orphaned' not in [table[0] for table in tables]:
                print("The 'xbrl_facts_orphaned' table does not exist in the database.")
                return

            # Fetch one record
            result = con.execute("SELECT cik, accession_number FROM xbrl_facts_orphaned LIMIT 1;").fetchone()

            if result:
                cik, accession_number = result
                print("\n--- Found an Orphaned Fact ---")
                print(f"CIK: {cik}")
                print(f"Accession Number: {accession_number}")
                print("\nUse this information to perform a targeted fetch and test your fix.")
            else:
                print("\n--- No Orphaned Facts Found ---")
                print("The 'xbrl_facts_orphaned' table is empty.")

    except duckdb.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        db_file_path = config.DB_FILE_STR
        find_first_orphan(db_file_path)
    except SystemExit as e:
        print(f"Configuration failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred during script execution: {e}", file=sys.stderr)
        sys.exit(1)
