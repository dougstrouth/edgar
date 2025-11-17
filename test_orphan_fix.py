
import sys
from pathlib import Path
import json

# Add project root to sys.path to allow importing from utils and data_processing
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

from data_processing.json_parse import parse_submission_json_for_db, parse_company_facts_json_for_db
from utils.config_utils import AppConfig

def run_test():
    """
    Runs a targeted test to investigate an orphaned fact.
    """
    ORPHAN_CIK = "0001108426"
    ORPHAN_ACCESSION_NUMBER = "0001193125-10-246348"

    print("--- Starting Targeted Orphan Test ---")

    try:
        config = AppConfig(calling_script_path=Path(__file__))
        submissions_dir = config.SUBMISSIONS_DIR
        companyfacts_dir = config.COMPANYFACTS_DIR
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return

    # 1. Test the submission file parsing
    submission_file = submissions_dir / f"CIK{ORPHAN_CIK}.json"
    print(f"\n--- Checking Submission File: {submission_file} ---")

    if not submission_file.exists():
        print(f"ERROR: Submission file not found. Please run the fetch for CIK {ORPHAN_CIK} first.")
        return

    parsed_submission_data = parse_submission_json_for_db(submission_file)

    found_filing = False
    if parsed_submission_data and parsed_submission_data.get("filings"):
        for filing in parsed_submission_data["filings"]:
            if filing.get("accession_number") == ORPHAN_ACCESSION_NUMBER:
                print("\n--- Found Matching Filing in Submission Data ---")
                print(json.dumps(filing, indent=2, default=str))
                found_filing = True
                break
        if not found_filing:
            print(f"\n--- ERROR: Did not find accession number {ORPHAN_ACCESSION_NUMBER} in parsed submission data. ---")
            print(f"Found {len(parsed_submission_data['filings'])} filings in total.")

    else:
        print("ERROR: Failed to parse submission data or no filings found.")


    # 2. Test the company facts file parsing
    facts_file = companyfacts_dir / f"CIK{ORPHAN_CIK}.json"
    print(f"\n--- Checking Company Facts File: {facts_file} ---")

    if not facts_file.exists():
        print(f"WARNING: Company facts file not found. This may be expected if it wasn't downloaded.")
        # This might not be a critical error for this specific test if the orphan is due to submission parsing.
    else:
        parsed_facts_data = parse_company_facts_json_for_db(facts_file)
        found_fact = False
        if parsed_facts_data and parsed_facts_data.get("xbrl_facts"):
            for fact in parsed_facts_data["xbrl_facts"]:
                if fact.get("accession_number") == ORPHAN_ACCESSION_NUMBER:
                    # We don't need to print it, just confirm it exists
                    found_fact = True
            if found_fact:
                print(f"\n--- Found facts associated with accession number {ORPHAN_ACCESSION_NUMBER} in company facts data. ---")
            else:
                print(f"\n--- WARNING: Did not find any facts for accession number {ORPHAN_ACCESSION_NUMBER} in company facts data. ---")
        else:
            print("WARNING: Failed to parse company facts data or no facts found.")


    print("\n--- Test Complete ---")


if __name__ == "__main__":
    run_test()
