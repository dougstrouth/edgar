from pathlib import Path
import pytest

from data_processing.json_parse import parse_submission_json_for_db, parse_company_facts_json_for_db


FIXTURES_DIR = Path(__file__).resolve().parent / 'fixtures'


def test_parse_submission_json_minimal():
    file_path = FIXTURES_DIR / 'submission_sample.json'
    assert file_path.exists()

    parsed = parse_submission_json_for_db(file_path)
    assert parsed is not None
    assert 'companies' in parsed and 'filings' in parsed and 'tickers' in parsed

    companies = parsed['companies']
    assert companies.get('cik') == '0000320193' or companies.get('cik') == '320193'.zfill(10)

    tickers = parsed['tickers']
    assert isinstance(tickers, list) and len(tickers) == 1
    assert tickers[0]['ticker'] == 'AAPL'

    filings = parsed['filings']
    assert isinstance(filings, list) and len(filings) == 1
    filing = filings[0]
    assert filing['form'] == '10-K'
    assert filing['accession_number'] == '0001193125-20-000001'


def test_parse_company_facts_minimal():
    file_path = FIXTURES_DIR / 'companyfacts_sample.json'
    assert file_path.exists()

    parsed = parse_company_facts_json_for_db(file_path)
    assert parsed is not None
    assert 'xbrl_tags' in parsed and 'xbrl_facts' in parsed

    tags = parsed['xbrl_tags']
    assert any(t['tag_name'] == 'Revenues' for t in tags)

    facts = parsed['xbrl_facts']
    assert isinstance(facts, list) and len(facts) >= 1
    f = facts[0]
    assert f['accession_number'] == '0001193125-20-000001'
    assert f['value_numeric'] == 1000000.0 or f['value_text'] is None
