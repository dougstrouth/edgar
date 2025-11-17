
import sys
import json
from pathlib import Path
import logging

# Add project root to sys.path to allow importing project modules
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from data_processing.json_parse import parse_submission_json_for_db

# Setup a basic logger to see the warnings from the parser
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
# Set the json_parse logger to DEBUG to see detailed parsing messages
logging.getLogger('data_processing.json_parse').setLevel(logging.DEBUG)


def test_specific_cik(cik: str):
    """
    Tests the submission JSON parsing for a single CIK to isolate issues.
    """
    print(f"--- Testing parser for CIK: {cik} ---")
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        submissions_dir = config.SUBMISSIONS_DIR
        file_path = submissions_dir / f"CIK{cik}.json"

        if not file_path.is_file():
            print(f"ERROR: JSON file not found at {file_path}")
            print("Please run the fetch command first for this CIK:")
            print(f"python main.py fetch --ciks {cik.lstrip('0')}")
            return

        print(f"Found file: {file_path}")
        print("Calling parse_submission_json_for_db...")
        
        parsed_data = parse_submission_json_for_db(file_path)

        if not parsed_data:
            print("\n--- RESULT ---")
            print("Parsing returned None. This indicates a critical error during parsing.")
            return

        filings = parsed_data.get("filings", [])
        print("\n--- RESULT ---")
        print(f"Parsing complete. Found {len(filings)} filings for CIK {cik}.")

        if not filings:
            print("\nWARNING: No filings were parsed.")
            print("This is the likely cause of the orphan fact issue.")
            print("Check the log output above for warnings about 'inconsistent array lengths'.")
            
            print("\nTo investigate further, checking the JSON structure...")
            with open(file_path, 'r') as f:
                data = json.load(f)
            recent_filings = data.get('filings', {}).get('recent', {})
            if recent_filings:
                print("Lengths of arrays in 'filings.recent':")
                for key, value in recent_filings.items():
                    if isinstance(value, list):
                        print(f"  - {key}: {len(value)}")
                    else:
                        print(f"  - {key}: Not a list")
            else:
                print("'filings.recent' not found in the JSON.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # CIK of the orphaned fact provided
    ORPHAN_CIK = "0001108426"
    test_specific_cik(ORPHAN_CIK)

