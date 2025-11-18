import sys
import json
from pathlib import Path
import logging
import pytest

# Ensure project root is importable when tests run from the repository root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from data_processing.json_parse import parse_submission_json_for_db

# Configure logging so parser warnings/errors are visible during test runs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
logging.getLogger('data_processing.json_parse').setLevel(logging.DEBUG)


ORPHAN_CIK = "0001108426"


def test_parse_submission_json_for_orphan_cik():
    """Parses a real submission JSON for the known orphan CIK when available.

    If the submission JSON isn't available locally, the test is skipped so CI
    does not fail for missing external data.
    """
    config = AppConfig(calling_script_path=Path(__file__))
    submissions_dir = config.SUBMISSIONS_DIR
    file_path = submissions_dir / f"CIK{ORPHAN_CIK}.json"

    if not file_path.is_file():
        pytest.skip(f"Submission JSON not found: {file_path}. Run fetch step to create it.")

    parsed_data = parse_submission_json_for_db(file_path)

    assert parsed_data is not None, "Parser returned None for a valid submission JSON"
    assert isinstance(parsed_data, dict), "Parsed data should be a dict"
    assert 'filings' in parsed_data, "Parsed data missing 'filings' key"

    filings = parsed_data.get('filings', [])
    assert isinstance(filings, list), "'filings' should be a list"
