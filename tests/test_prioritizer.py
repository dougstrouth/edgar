import duckdb
from pathlib import Path
from datetime import date, timedelta

from utils.prioritizer import prioritize_tickers_hybrid


def test_prioritize_tickers_hybrid(tmp_path):
    db_file = tmp_path / "test_prioritizer.duckdb"
    conn = duckdb.connect(database=str(db_file))

    # Create minimal tables
    conn.execute("CREATE TABLE tickers (cik VARCHAR, ticker VARCHAR);")
    conn.execute("CREATE TABLE filings (accession_number VARCHAR, cik VARCHAR, filing_date DATE);")

    # Prepare sample data for three tickers mapping to three CIKs
    conn.execute("INSERT INTO tickers VALUES ('0000001','AAA'), ('0000002','BBB'), ('0000003','CCC');")

    today = date.today()
    # AAA: many recent filings
    for i in range(10):
        conn.execute("INSERT INTO filings VALUES (?, ?, ?);", [f"accA{i}", '0000001', today - timedelta(days=i)])
    # BBB: one older filing beyond lookback
    conn.execute("INSERT INTO filings VALUES ('accB0', '0000002', ?);", [today - timedelta(days=400)])
    # CCC: no filings

    conn.close()

    tickers = ['AAA', 'BBB', 'CCC']
    ranked = prioritize_tickers_hybrid(str(db_file), tickers, lookback_days=365)
    # ranked is list of (ticker, score)
    order = [t for t, s in ranked]

    assert order[0] == 'AAA', "AAA should be top-ranked due to many recent filings"
    assert order[-1] == 'CCC', "CCC should be lowest ranked due to no filings"
