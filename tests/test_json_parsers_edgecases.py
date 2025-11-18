from pathlib import Path
import pytest

from data_processing.json_parse import parse_submission_json_for_db, parse_company_facts_json_for_db


FIXTURES_DIR = Path(__file__).resolve().parent / 'fixtures'


def test_malformed_filings_are_skipped():
    file_path = FIXTURES_DIR / 'submission_malformed.json'
    assert file_path.exists()

    parsed = parse_submission_json_for_db(file_path)
    # Parser should return a structure, but skip malformed filings
    assert parsed is not None
    assert 'filings' in parsed
    assert isinstance(parsed['filings'], list)
    assert len(parsed['filings']) == 0


def test_empty_companyfacts_file_returns_none():
    file_path = FIXTURES_DIR / 'companyfacts_empty.json'
    # Ensure file exists and is zero-length
    assert file_path.exists()
    assert file_path.stat().st_size == 0

    parsed = parse_company_facts_json_for_db(file_path)
    assert parsed is None


def test_nonfinite_values_are_recorded_as_text():
    file_path = FIXTURES_DIR / 'companyfacts_nonfinite.json'
    assert file_path.exists()

    parsed = parse_company_facts_json_for_db(file_path)
    assert parsed is not None
    facts = parsed.get('xbrl_facts', [])
    assert isinstance(facts, list)
    # Find the two accession records added in the fixture
    accns = {f['accession_number']: f for f in facts}
    assert '0001193125-20-000001' in accns
    assert '0001193125-20-000002' in accns

    f1 = accns['0001193125-20-000001']
    f2 = accns['0001193125-20-000002']
    assert f1['value_numeric'] is None and f1['value_text'] == 'NaN'
    assert f2['value_numeric'] is None and f2['value_text'] == 'Infinity'
