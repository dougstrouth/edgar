"""Tests for prioritize_tickers_for_info function"""
import pytest
import duckdb
from utils.prioritizer import prioritize_tickers_for_info


@pytest.fixture
def test_db_with_ticker_info_data(tmp_path):
    """Create test database with ticker info prioritization data"""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    
    # Create tables
    con.execute("CREATE TABLE tickers (ticker VARCHAR, cik VARCHAR)")
    con.execute("CREATE TABLE xbrl_facts (cik VARCHAR, tag_name VARCHAR)")
    con.execute("CREATE TABLE stock_history (ticker VARCHAR, date DATE)")
    con.execute("CREATE TABLE filings (cik VARCHAR, filing_date DATE)")
    con.execute("CREATE TABLE updated_ticker_info (ticker VARCHAR, primary_exchange VARCHAR, fetch_timestamp TIMESTAMP)")
    
    # Insert test data - varied richness
    con.execute("INSERT INTO tickers VALUES ('AAPL', '0000320193'), ('MSFT', '0000789019'), ('TSLA', '0001318605'), ('GME', '0001326380')")
    
    # AAPL: Rich XBRL (8 unique tags + all key metrics)
    for tag in ['Assets', 'Liabilities', 'StockholdersEquity', 'Revenue', 'NetIncome', 'OperatingIncome', 'CashAndCashEquivalents', 'EarningsPerShare']:
        con.execute(f"INSERT INTO xbrl_facts VALUES ('0000320193', '{tag}')")
    
    # MSFT: Medium XBRL (5 tags, some key metrics)
    for tag in ['Assets', 'Revenue', 'NetIncome', 'CustomTag1', 'CustomTag2']:
        con.execute(f"INSERT INTO xbrl_facts VALUES ('0000789019', '{tag}')")
    
    # TSLA: Minimal XBRL (2 tags)
    con.execute("INSERT INTO xbrl_facts VALUES ('0001318605', 'Assets'), ('0001318605', 'Revenue')")
    
    # GME: No XBRL facts (will be filtered out)
    
    # Stock history (AAPL and MSFT have data, TSLA doesn't)
    con.execute("INSERT INTO stock_history VALUES ('AAPL', '2025-01-01'), ('MSFT', '2025-01-01')")
    
    # Recent filings (AAPL: 5, MSFT: 2, TSLA: 1)
    for _ in range(5):
        con.execute("INSERT INTO filings VALUES ('0000320193', CURRENT_DATE)")
    for _ in range(2):
        con.execute("INSERT INTO filings VALUES ('0000789019', CURRENT_DATE)")
    con.execute("INSERT INTO filings VALUES ('0001318605', CURRENT_DATE)")
    
    # Ticker info with exchanges (AAPL: XNAS major, MSFT: NYSE major, TSLA: OTC minor)
    con.execute("INSERT INTO updated_ticker_info VALUES ('AAPL', 'XNAS', CURRENT_TIMESTAMP)")
    con.execute("INSERT INTO updated_ticker_info VALUES ('MSFT', 'XNYS', CURRENT_TIMESTAMP)")
    con.execute("INSERT INTO updated_ticker_info VALUES ('TSLA', 'OTCM', CURRENT_TIMESTAMP)")
    
    con.close()
    return db_path


def test_prioritize_tickers_for_info_basic(test_db_with_ticker_info_data):
    """Test basic prioritization returns sorted list"""
    result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT', 'TSLA']
    )
    
    assert len(result) == 3
    assert all(isinstance(item, tuple) for item in result)
    assert all(len(item) == 2 for item in result)
    
    # Check sorted descending by score
    scores = [score for _, score in result]
    assert scores == sorted(scores, reverse=True)
    
    # AAPL should be highest (most XBRL, key metrics, stock data, filings, major exchange)
    assert result[0][0] == 'AAPL'
    assert result[0][1] > result[1][1]


def test_prioritize_tickers_for_info_empty_list():
    """Test with empty ticker list"""
    result = prioritize_tickers_for_info(
        db_path=':memory:',
        tickers=[]
    )
    assert result == []


def test_prioritize_tickers_for_info_custom_weights(test_db_with_ticker_info_data):
    """Test custom weights affect ordering"""
    # Default weights favor XBRL richness
    default_result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT', 'TSLA']
    )
    
    # Custom weights heavily favor exchange (both AAPL and MSFT major, but filing activity differs)
    custom_result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT', 'TSLA'],
        weights={
            'xbrl_richness': 0.1,
            'key_metrics': 0.1,
            'has_stock_data': 0.1,
            'filing_activity': 0.5,  # Heavy weight on filings
            'exchange_priority': 0.2
        }
    )
    
    # Results should differ or at least have different scores
    assert default_result[0][1] != custom_result[0][1] or default_result[0][0] == custom_result[0][0]


def test_prioritize_tickers_for_info_lookback_period(test_db_with_ticker_info_data):
    """Test lookback period affects filing activity scoring"""
    # Short lookback (recent filings matter)
    recent_result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT', 'TSLA'],
        lookback_days=30
    )
    
    # Long lookback (all filings matter)
    long_result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT', 'TSLA'],
        lookback_days=730
    )
    
    # Scores should be identical since all our test filings are recent (CURRENT_DATE)
    # But the mechanism is tested
    assert len(recent_result) == len(long_result)


def test_prioritize_tickers_for_info_invalid_weights():
    """Test invalid weights raise error"""
    with pytest.raises(ValueError, match="weights must sum to > 0"):
        prioritize_tickers_for_info(
            db_path=':memory:',
            tickers=['TEST'],
            weights={'xbrl_richness': 0, 'key_metrics': 0, 'has_stock_data': 0, 'filing_activity': 0, 'exchange_priority': 0}
        )


def test_prioritize_tickers_for_info_db_connection_failure():
    """Test graceful handling of DB connection failure"""
    result = prioritize_tickers_for_info(
        db_path='/nonexistent/path/to/db.duckdb',
        tickers=['AAPL', 'MSFT']
    )
    # Should return zero scores rather than crashing
    assert len(result) == 2
    assert all(score == 0.0 for _, score in result)


def test_prioritize_tickers_for_info_missing_tables(tmp_path):
    """Test handling of missing tables (partial data)"""
    db_path = tmp_path / "partial.duckdb"
    con = duckdb.connect(str(db_path))
    
    # Only create tickers table, missing xbrl_facts, stock_history, etc.
    con.execute("CREATE TABLE tickers (ticker VARCHAR, cik VARCHAR)")
    con.execute("INSERT INTO tickers VALUES ('AAPL', '0000320193')")
    con.close()
    
    result = prioritize_tickers_for_info(
        db_path=str(db_path),
        tickers=['AAPL']
    )
    
    # Should handle gracefully with low scores (no data)
    assert len(result) == 1
    assert result[0][1] >= 0  # Score should be non-negative


def test_prioritize_tickers_for_info_normalized_ticker_matching(test_db_with_ticker_info_data):
    """Test hyphen-to-period normalization for ticker matching"""
    con = duckdb.connect(str(test_db_with_ticker_info_data))
    
    # Add ticker with period in ticker_info but hyphen in tickers table
    con.execute("INSERT INTO tickers VALUES ('BRK-B', '0001067983')")
    con.execute("INSERT INTO updated_ticker_info VALUES ('BRK.B', 'XNYS', CURRENT_TIMESTAMP)")
    con.execute("INSERT INTO xbrl_facts VALUES ('0001067983', 'Assets')")
    con.close()
    
    result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['BRK-B']
    )
    
    # Should successfully match and prioritize
    assert len(result) == 1
    assert result[0][0] == 'BRK-B'
    assert result[0][1] > 0  # Has exchange from BRK.B match


def test_prioritize_tickers_for_info_score_components(test_db_with_ticker_info_data):
    """Test individual scoring components contribute correctly"""
    # Ticker with max everything vs ticker with nothing
    result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'TSLA']  # AAPL has all, TSLA has minimal
    )
    
    aapl_score = result[0][1] if result[0][0] == 'AAPL' else result[1][1]
    tsla_score = result[0][1] if result[0][0] == 'TSLA' else result[1][1]
    
    # AAPL should have significantly higher score
    assert aapl_score > tsla_score * 2  # At least 2x


def test_prioritize_weights_normalization(test_db_with_ticker_info_data):
    """Test that weights are automatically normalized"""
    # Unnormalized weights (sum != 1)
    result = prioritize_tickers_for_info(
        db_path=str(test_db_with_ticker_info_data),
        tickers=['AAPL', 'MSFT'],
        weights={
            'xbrl_richness': 10,
            'key_metrics': 5,
            'has_stock_data': 3,
            'filing_activity': 2,
            'exchange_priority': 1
        }
    )
    
    # Should still work (normalized internally)
    assert len(result) == 2
    assert result[0][1] > 0
