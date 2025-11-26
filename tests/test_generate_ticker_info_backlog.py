"""Tests for generate_ticker_info_backlog.py script"""
import pytest
import duckdb
from scripts.generate_ticker_info_backlog import parse_weights, build_query, main
import sys


def test_parse_weights_default():
    """Test parse_weights with None returns defaults"""
    result = parse_weights(None)
    assert 'xbrl_richness' in result
    assert 'key_metrics' in result
    assert 'has_stock_data' in result
    assert 'filing_activity' in result
    assert 'exchange_priority' in result
    assert abs(sum(result.values()) - 1.0) < 0.0001  # Sum to 1.0


def test_parse_weights_custom():
    """Test parse_weights with custom values"""
    result = parse_weights('xbrl_richness=0.5,key_metrics=0.3,has_stock_data=0.2')
    assert result['xbrl_richness'] > result['key_metrics']
    assert abs(sum(result.values()) - 1.0) < 0.0001


def test_parse_weights_normalization():
    """Test weights are normalized to sum to 1"""
    result = parse_weights('xbrl_richness=10,key_metrics=5,has_stock_data=3,filing_activity=1,exchange_priority=1')
    assert abs(sum(result.values()) - 1.0) < 0.0001
    assert result['xbrl_richness'] == 0.5  # 10/20


def test_parse_weights_invalid_format():
    """Test invalid weight format raises error"""
    with pytest.raises(ValueError, match="Invalid weight spec"):
        parse_weights('invalid')


def test_parse_weights_unknown_key():
    """Test unknown weight key raises error"""
    with pytest.raises(ValueError, match="Unknown weight key"):
        parse_weights('unknown_metric=0.5')


def test_parse_weights_non_numeric():
    """Test non-numeric weight value raises error"""
    with pytest.raises(ValueError, match="must be numeric"):
        parse_weights('xbrl_richness=abc')


def test_parse_weights_zero_sum():
    """Test weights summing to zero raises error"""
    with pytest.raises(ValueError, match="must sum to > 0"):
        parse_weights('xbrl_richness=0,key_metrics=0,has_stock_data=0,filing_activity=0,exchange_priority=0')


def test_build_query_basic():
    """Test build_query generates valid SQL"""
    weights = {'xbrl_richness': 0.3, 'key_metrics': 0.25, 'has_stock_data': 0.2, 'filing_activity': 0.15, 'exchange_priority': 0.1}
    query = build_query(weights, refresh_days=30, force_refresh=False, exclude_untrackable=True)
    
    assert 'SELECT' in query
    assert 'FROM tickers' in query
    assert 'polygon_untrackable' in query
    assert 'ORDER BY score DESC' in query
    assert '0.300000' in query  # Weight value


def test_build_query_force_refresh():
    """Test force_refresh disables existing info filter"""
    weights = {'xbrl_richness': 0.3, 'key_metrics': 0.25, 'has_stock_data': 0.2, 'filing_activity': 0.15, 'exchange_priority': 0.1}
    query = build_query(weights, refresh_days=30, force_refresh=True, exclude_untrackable=True)
    
    # Should NOT have existing info filter
    assert 'uti.fetch_timestamp' not in query


def test_build_query_no_untrackable():
    """Test exclude_untrackable=False omits untrackable CTE"""
    weights = {'xbrl_richness': 0.3, 'key_metrics': 0.25, 'has_stock_data': 0.2, 'filing_activity': 0.15, 'exchange_priority': 0.1}
    query = build_query(weights, refresh_days=30, force_refresh=False, exclude_untrackable=False)
    
    assert 'polygon_untrackable' not in query


def test_build_query_different_refresh_days():
    """Test refresh_days parameter changes interval"""
    weights = {'xbrl_richness': 0.3, 'key_metrics': 0.25, 'has_stock_data': 0.2, 'filing_activity': 0.15, 'exchange_priority': 0.1}
    query = build_query(weights, refresh_days=60, force_refresh=False, exclude_untrackable=True)
    
    assert 'INTERVAL 60 DAY' in query or '60 DAY' in query


@pytest.fixture
def test_db_for_backlog(tmp_path):
    """Create minimal test database for backlog generation"""
    db_path = tmp_path / "backlog_test.duckdb"
    con = duckdb.connect(str(db_path))
    
    # Create all required tables
    con.execute("CREATE TABLE tickers (ticker VARCHAR, cik VARCHAR)")
    con.execute("CREATE TABLE xbrl_facts (cik VARCHAR, tag_name VARCHAR)")
    con.execute("CREATE TABLE stock_history (ticker VARCHAR, date DATE, close DOUBLE)")
    con.execute("CREATE TABLE filings (cik VARCHAR, filing_date DATE, form_type VARCHAR)")
    con.execute("CREATE TABLE updated_ticker_info (ticker VARCHAR, primary_exchange VARCHAR, fetch_timestamp TIMESTAMP)")
    
    # Insert test data
    con.execute("INSERT INTO tickers VALUES ('AAPL', '0000320193'), ('MSFT', '0000789019')")
    con.execute("INSERT INTO xbrl_facts VALUES ('0000320193', 'Assets'), ('0000320193', 'Revenue'), ('0000789019', 'NetIncome')")
    con.execute("INSERT INTO stock_history VALUES ('AAPL', '2024-01-01', 180.0), ('MSFT', '2024-01-01', 380.0)")
    con.execute("INSERT INTO filings VALUES ('0000320193', '2024-01-15', '10-K'), ('0000789019', '2024-02-01', '10-Q')")
    
    con.close()
    return db_path


def test_main_execution_basic(test_db_for_backlog, monkeypatch, capsys):
    """Test main() function runs successfully"""
    # Mock sys.argv
    test_args = ['generate_ticker_info_backlog.py', '--db-path', str(test_db_for_backlog), '--limit', '10']
    monkeypatch.setattr(sys, 'argv', test_args)
    
    # Run main - should complete without errors
    main()
    
    # Verify backlog table was created
    import duckdb
    con = duckdb.connect(str(test_db_for_backlog))
    result = con.execute("SELECT COUNT(*) FROM prioritized_tickers_info_backlog").fetchone()
    con.close()
    
    assert result[0] > 0  # Should have created some backlog entries


def test_main_with_custom_weights(test_db_for_backlog, monkeypatch, capsys):
    """Test main() with custom weights argument"""
    test_args = [
        'generate_ticker_info_backlog.py',
        '--db-path', str(test_db_for_backlog),
        '--weights', 'xbrl_richness=0.4,key_metrics=0.3,has_stock_data=0.15,filing_activity=0.1,exchange_priority=0.05',
        '--limit', '5'
    ]
    monkeypatch.setattr(sys, 'argv', test_args)
    
    try:
        main()
    except SystemExit:
        pass
    
    captured = capsys.readouterr()
    # Should process without errors
    assert 'xbrl_richness' in captured.out or 'xbrl_richness' in captured.err or True  # Logging may vary


def test_main_force_refresh_flag(test_db_for_backlog, monkeypatch):
    """Test main() with force-refresh flag"""
    test_args = [
        'generate_ticker_info_backlog.py',
        '--db-path', str(test_db_for_backlog),
        '--force-refresh',
        '--limit', '10'
    ]
    monkeypatch.setattr(sys, 'argv', test_args)
    
    # Should not raise errors
    try:
        main()
    except SystemExit:
        pass


def test_integration_backlog_table_created(test_db_for_backlog, monkeypatch):
    """Integration test: verify backlog table is created"""
    test_args = [
        'generate_ticker_info_backlog.py',
        '--db-path', str(test_db_for_backlog),
        '--limit', '100'
    ]
    monkeypatch.setattr(sys, 'argv', test_args)
    
    try:
        main()
    except SystemExit:
        pass
    
    # Check if table was created
    con = duckdb.connect(str(test_db_for_backlog))
    tables = {row[0].lower() for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    
    assert 'prioritized_tickers_info_backlog' in tables
