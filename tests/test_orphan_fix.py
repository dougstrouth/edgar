import sys
from pathlib import Path
import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_processing.json_parse import (
    parse_submission_json_for_db,
    parse_company_facts_json_for_db,
)
from utils.config_utils import AppConfig

ORPHAN_CIK = "0001108426"
ORPHAN_ACCESSION_NUMBER = "0001193125-10-246348"


def test_submission_contains_orphan_accession():
    config = AppConfig(calling_script_path=Path(__file__))
    submissions_dir = config.SUBMISSIONS_DIR
    submission_file = submissions_dir / f"CIK{ORPHAN_CIK}.json"
    # If the real submission file isn't present, fall back to the test fixture
    if not submission_file.exists():
        fixture = PROJECT_ROOT / 'tests' / 'fixtures' / 'submission_sample.json'
        if fixture.exists():
            submission_file = fixture
        else:
            pytest.skip(f"Submission file not present and no fixture available: {submission_file}")

    parsed_submission_data = parse_submission_json_for_db(submission_file)
    assert parsed_submission_data is not None, "Parser returned None for submission JSON"
    filings = parsed_submission_data.get('filings', [])
    assert isinstance(filings, list), "Parsed 'filings' should be a list"

    # If we used a real submission file, assert the specific orphan accession exists.
    if submission_file.name.startswith(f"CIK{ORPHAN_CIK}"):
        accession_found = any(f.get('accession_number') == ORPHAN_ACCESSION_NUMBER for f in filings)
        assert accession_found, f"Accession {ORPHAN_ACCESSION_NUMBER} not found in parsed filings"


def test_company_facts_contains_related_facts_if_present():
    config = AppConfig(calling_script_path=Path(__file__))
    companyfacts_dir = config.COMPANYFACTS_DIR
    facts_file = companyfacts_dir / f"CIK{ORPHAN_CIK}.json"

    # Fall back to fixture if the real company facts file is missing
    if not facts_file.exists():
        fixture = PROJECT_ROOT / 'tests' / 'fixtures' / 'companyfacts_sample.json'
        if fixture.exists():
            facts_file = fixture
        else:
            pytest.skip(f"Company facts file not present and no fixture available: {facts_file}")

    parsed_facts = parse_company_facts_json_for_db(facts_file)
    assert parsed_facts is not None, "Parser returned None for company facts JSON"
    xbrl_facts = parsed_facts.get('xbrl_facts', [])
    assert isinstance(xbrl_facts, list), "Parsed 'xbrl_facts' should be a list"

    # If we used a real company facts file, assert the specific accession exists.
    if facts_file.name.startswith(f"CIK{ORPHAN_CIK}"):
        found = any(f.get('accession_number') == ORPHAN_ACCESSION_NUMBER for f in xbrl_facts)
        assert found, f"No facts found for accession {ORPHAN_ACCESSION_NUMBER} in company facts"
