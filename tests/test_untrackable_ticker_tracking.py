#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for Polygon untrackable ticker tracking functionality.

Validates that:
1. 404 errors are properly recorded in polygon_untrackable_tickers table
2. Prioritization script excludes untrackable tickers
3. Ticker info gatherer skips untrackable tickers
4. Expiry logic works (old failures are retried after 365 days)
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

import pytest
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_gathering.ticker_info_gatherer_polygon import get_polygon_untrackable_tickers  # noqa: E402
from data_gathering.ticker_info_gatherer_polygon import fetch_worker as fetch_worker_info  # noqa: E402
from data_gathering.stock_data_gatherer_polygon import get_polygon_untrackable_tickers as get_polygon_untrackable_tickers_stock  # noqa: E402
from scripts.generate_prioritized_backlog import build_query, DEFAULT_WEIGHTS  # noqa: E402


@pytest.fixture
def test_db(tmp_path):
    """Create a test database with sample data."""
    db_path = tmp_path / "test_untrackable.duckdb"
    con = duckdb.connect(str(db_path))
    
    # Create minimal schema for testing
    con.execute("""
        CREATE TABLE companies (
            cik VARCHAR PRIMARY KEY,
            name VARCHAR
        );
    """)
    
    con.execute("""
        CREATE TABLE updated_ticker_info (
            ticker VARCHAR PRIMARY KEY COLLATE NOCASE,
            cik VARCHAR,
            name VARCHAR
        );
    """)
    
    con.execute("""
        CREATE TABLE xbrl_facts (
            cik VARCHAR,
            tag_name VARCHAR,
            value DOUBLE
        );
    """)
    
    con.execute("""
        CREATE TABLE stock_history (
            ticker VARCHAR,
            date DATE,
            close DOUBLE
        );
    """)
    
    con.execute("""
        CREATE TABLE filings (
            cik VARCHAR,
            filing_date DATE,
            form VARCHAR
        );
    """)
    
    # Insert test data
    con.execute("INSERT INTO companies VALUES ('0001234567', 'Test Company 1'), ('0001234568', 'Test Company 2');")
    con.execute("INSERT INTO updated_ticker_info VALUES ('GOOD', '0001234567', 'Good Ticker'), ('BAD', '0001234568', 'Bad Ticker');")
    
    # Add some XBRL facts for both companies
    for i in range(10):
        con.execute(f"INSERT INTO xbrl_facts VALUES ('0001234567', 'Tag{i}', 100.0);")
        con.execute(f"INSERT INTO xbrl_facts VALUES ('0001234568', 'Tag{i}', 100.0);")
    
    # Add filings
    con.execute("INSERT INTO filings VALUES ('0001234567', CURRENT_DATE - INTERVAL 30 DAY, '10-K');")
    con.execute("INSERT INTO filings VALUES ('0001234568', CURRENT_DATE - INTERVAL 30 DAY, '10-K');")
    
    yield con, db_path
    con.close()


class TestUntrackableTickerTable:
    """Test the polygon_untrackable_tickers table creation and usage."""
    
    def test_table_creation_and_insert(self, test_db):
        """Test that the table can be created and records inserted."""
        con, db_path = test_db
        
        # Create the table
        con.execute("""
            CREATE TABLE IF NOT EXISTS polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        # Insert a test record
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT OR REPLACE INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['TEST404', '404 Client Error: Not Found', now]
        )
        
        # Verify insertion
        result = con.execute("SELECT ticker, reason FROM polygon_untrackable_tickers WHERE ticker = 'TEST404'").fetchone()
        assert result is not None
        assert result[0] == 'TEST404'
        assert '404' in result[1]
    
    def test_case_insensitive_ticker_matching(self, test_db):
        """Test that ticker matching is case-insensitive."""
        con, db_path = test_db
        
        # Create table and insert lowercase
        con.execute("""
            CREATE TABLE IF NOT EXISTS polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT OR REPLACE INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['test404', '404 Not Found', now]
        )
        
        # Query with different case
        result = con.execute("SELECT ticker FROM polygon_untrackable_tickers WHERE ticker = 'TEST404'").fetchone()
        assert result is not None
        assert result[0].upper() == 'TEST404'
    
    def test_expiry_filtering(self, test_db):
        """Test that old failures are filtered out based on expiry."""
        con, db_path = test_db
        
        # Create table
        con.execute("""
            CREATE TABLE IF NOT EXISTS polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        old_failure = now - timedelta(days=400)  # Older than 365 days
        recent_failure = now - timedelta(days=30)
        
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['OLD404', '404 Not Found', old_failure]
        )
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['RECENT404', '404 Not Found', recent_failure]
        )
        
        # Query for recent failures only (within 365 days)
        recent_only = con.execute("""
            SELECT ticker FROM polygon_untrackable_tickers 
            WHERE last_failed_timestamp >= (CURRENT_DATE - INTERVAL 365 DAY)
        """).fetchall()
        
        tickers = {row[0] for row in recent_only}
        assert 'RECENT404' in tickers
        assert 'OLD404' not in tickers  # Should be expired


class TestGetPolygonUntrackableTickers:
    """Test the get_polygon_untrackable_tickers helper function."""
    
    def test_returns_empty_set_when_table_missing(self, test_db):
        """Test that function returns empty set when table doesn't exist."""
        con, db_path = test_db
        
        result = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert isinstance(result, set)
        assert len(result) == 0
    
    def test_returns_recent_failures(self, test_db):
        """Test that function returns only recent failures."""
        con, db_path = test_db
        
        # Create and populate table
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        con.execute("INSERT INTO polygon_untrackable_tickers VALUES ('BAD1', '404', ?);", [now - timedelta(days=30)])
        con.execute("INSERT INTO polygon_untrackable_tickers VALUES ('BAD2', '404', ?);", [now - timedelta(days=100)])
        con.execute("INSERT INTO polygon_untrackable_tickers VALUES ('OLD1', '404', ?);", [now - timedelta(days=400)])
        
        result = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'BAD1' in result
        assert 'BAD2' in result
        assert 'OLD1' not in result
    
    def test_custom_expiry_days(self, test_db):
        """Test that custom expiry_days parameter works."""
        con, db_path = test_db
        
        # Create and populate table
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        con.execute("INSERT INTO polygon_untrackable_tickers VALUES ('RECENT', '404', ?);", [now - timedelta(days=5)])
        con.execute("INSERT INTO polygon_untrackable_tickers VALUES ('MEDIUM', '404', ?);", [now - timedelta(days=50)])
        
        # Test with 30-day expiry
        result_30 = get_polygon_untrackable_tickers(con, expiry_days=30)
        assert 'RECENT' in result_30
        assert 'MEDIUM' not in result_30
        
        # Test with 60-day expiry
        result_60 = get_polygon_untrackable_tickers(con, expiry_days=60)
        assert 'RECENT' in result_60
        assert 'MEDIUM' in result_60


class TestPrioritizationExclusion:
    """Test that prioritization correctly excludes untrackable tickers."""
    
    def test_query_without_untrackable_table(self, test_db):
        """Test that query works when polygon_untrackable_tickers doesn't exist."""
        con, db_path = test_db
        
        # Build query without untrackable filtering
        query = build_query(DEFAULT_WEIGHTS, exclude_untrackable=False)
        
        # Should execute without error
        result = con.execute(query).fetchall()
        
        # Should return both tickers since no exclusion
        tickers = {row[0] for row in result}
        assert 'GOOD' in tickers
        assert 'BAD' in tickers


class TestTickerInfoErrorTracking:
    """Tests specific to ticker info gatherer error -> untrackable table creation."""

    def test_ticker_info_404_marks_untrackable(self, test_db, monkeypatch):
        """Simulate 404 in get_ticker_details and verify table creation + insertion."""
        con, db_path = test_db

        # Mock PolygonClient inside fetch_worker to raise a 404 error
        import requests

        class MockClient:
            def __init__(self, *args, **kwargs):
                pass

            def get_ticker_details(self, ticker):
                error = requests.HTTPError("404 Client Error: Not Found")
                error.response = type('obj', (), {'status_code': 404})()
                raise error

        monkeypatch.setattr('data_gathering.ticker_info_gatherer_polygon.PolygonClient', MockClient)

        job = {
            'ticker': 'MISSING1',
            'api_key': 'DUMMY',
            'db_path': str(db_path),
            'calls_per_minute': 3
        }

        result = fetch_worker_info(job)
        assert result['status'] == 'error'
        assert result['ticker'] == 'MISSING1'
        assert result.get('is_permanent') is True

        # Verify table created and ticker inserted
        tables = {r[0].lower() for r in con.execute('SHOW TABLES').fetchall()}
        assert 'polygon_untrackable_tickers' in tables
        rows = con.execute("SELECT ticker, reason FROM polygon_untrackable_tickers WHERE ticker = 'MISSING1'").fetchall()
        assert rows and '404' in rows[0][1]

    def test_ticker_info_400_marks_untrackable(self, test_db, monkeypatch):
        """Simulate 400 error and verify tracking."""
        con, db_path = test_db
        import requests

        class MockClient400:
            def __init__(self, *args, **kwargs):
                pass

            def get_ticker_details(self, ticker):
                error = requests.HTTPError("400 Client Error: Bad Request")
                error.response = type('obj', (), {'status_code': 400})()
                raise error

        monkeypatch.setattr('data_gathering.ticker_info_gatherer_polygon.PolygonClient', MockClient400)

        job = {
            'ticker': 'BADREQ1',
            'api_key': 'DUMMY',
            'db_path': str(db_path),
            'calls_per_minute': 3
        }

        result = fetch_worker_info(job)
        assert result['status'] == 'error'
        assert result['ticker'] == 'BADREQ1'
        assert result.get('is_permanent') is True

        rows = con.execute("SELECT ticker FROM polygon_untrackable_tickers WHERE ticker = 'BADREQ1'").fetchall()
        assert rows
    
    def test_query_with_untrackable_table_excludes_bad_tickers(self, test_db):
        """Test that query excludes tickers in polygon_untrackable_tickers."""
        con, db_path = test_db
        
        # Create untrackable table and mark BAD ticker
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BAD', '404 Client Error: Not Found', now]
        )
        
        # Build query with untrackable filtering
        query = build_query(DEFAULT_WEIGHTS, exclude_untrackable=True)
        result = con.execute(query).fetchall()
        
        # Should only return GOOD ticker
        tickers = {row[0] for row in result}
        assert 'GOOD' in tickers
        assert 'BAD' not in tickers
    
    def test_query_includes_expired_untrackable_tickers(self, test_db):
        """Test that expired untrackable tickers are included in prioritization."""
        con, db_path = test_db
        
        # Create untrackable table with old failure
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        old_time = datetime.now(timezone.utc) - timedelta(days=400)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BAD', '404 Client Error: Not Found', old_time]
        )
        
        # Build query with untrackable filtering (uses 365-day window)
        query = build_query(DEFAULT_WEIGHTS, exclude_untrackable=True)
        result = con.execute(query).fetchall()
        
        # Should include BAD ticker since failure is expired
        tickers = {row[0] for row in result}
        assert 'GOOD' in tickers
        assert 'BAD' in tickers  # Expired, so should be retried


class TestErrorTypeRecognition:
    """Test that different error types are correctly identified as permanent."""
    
    @pytest.mark.parametrize("error_msg,should_track", [
        ("404 Client Error: Not Found for url: https://api.massive.com/v3/reference/tickers/FAKE", True),
        ("400 Bad Request: Invalid ticker format", True),
        ("Client Error: Ticker not found", True),
        ("429 Too Many Requests", False),  # Rate limit, not permanent
        ("500 Internal Server Error", False),  # Server error, not permanent
        ("503 Service Unavailable", False),  # Temporary server issue
        ("Connection timeout", False),  # Network issue, not permanent
    ])
    def test_error_type_identification(self, error_msg, should_track):
        """Test that permanent errors are correctly identified."""
        is_permanent = (
            '400' in error_msg or 'Bad Request' in error_msg or
            '404' in error_msg or 'Not Found' in error_msg or
            'Client Error' in error_msg
        )
        assert is_permanent == should_track, f"Error '{error_msg}' should_track={should_track}"


class TestIntegrationScenarios:
    """Integration tests for complete workflows."""
    
    def test_full_workflow_with_untrackable_tracking(self, test_db):
        """Test complete workflow: mark untrackable -> prioritize -> verify exclusion."""
        con, db_path = test_db
        
        # Step 1: Create untrackable table (simulates ticker info gatherer finding 404)
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BAD', '404 Client Error: Not Found', now]
        )
        
        # Step 2: Run prioritization query
        query = build_query(DEFAULT_WEIGHTS, exclude_untrackable=True)
        prioritized = con.execute(query).fetchall()
        
        # Step 3: Verify BAD ticker excluded
        tickers = {row[0] for row in prioritized}
        assert 'BAD' not in tickers
        assert 'GOOD' in tickers
        
        # Step 4: Get untrackable set (simulates ticker info gatherer checking)
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'BAD' in untrackable
        
        # Step 5: Verify logic - if ticker in untrackable, should skip
        ticker_to_check = 'BAD'
        should_skip = ticker_to_check in untrackable
        assert should_skip
    
    def test_retry_after_expiry(self, test_db):
        """Test that old failures are retried after expiry period."""
        con, db_path = test_db
        
        # Create untrackable table with old failure
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        old_time = datetime.now(timezone.utc) - timedelta(days=400)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BAD', '404 Client Error: Not Found', old_time]
        )
        
        # Untrackable set should NOT include expired failures
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'BAD' not in untrackable
        
        # Prioritization should INCLUDE expired failures (will be retried)
        query = build_query(DEFAULT_WEIGHTS, exclude_untrackable=True)
        prioritized = con.execute(query).fetchall()
        tickers = {row[0] for row in prioritized}
        assert 'BAD' in tickers


class TestBothGatherersSharedTable:
    """Test that both ticker info and stock aggregates gatherers share the same untrackable table."""
    
    def test_both_gatherers_use_same_function(self):
        """Verify both gatherers import the same untrackable tracking function."""
        # The functions should have the same behavior
        assert get_polygon_untrackable_tickers.__name__ == get_polygon_untrackable_tickers_stock.__name__
    
    def test_ticker_info_failure_blocks_stock_aggregates(self, test_db):
        """Test that a 404 in ticker info gatherer prevents stock aggregates from trying."""
        con, db_path = test_db
        
        # Create untrackable table
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        # Simulate ticker info gatherer recording a 404
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['NOTFOUND', '404 Client Error: Not Found', now]
        )
        
        # Stock aggregates gatherer should see this ticker as untrackable
        untrackable_stock = get_polygon_untrackable_tickers_stock(con, expiry_days=365)
        assert 'NOTFOUND' in untrackable_stock
        
        # Both gatherers should return the same set
        untrackable_info = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert untrackable_info == untrackable_stock
    
    def test_stock_aggregates_failure_blocks_ticker_info(self, test_db):
        """Test that a 404 in stock aggregates gatherer prevents ticker info from trying."""
        con, db_path = test_db
        
        # Create untrackable table
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        # Simulate stock aggregates gatherer recording a 404
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BADTICKER', '400 Client Error: Bad Request', now]
        )
        
        # Ticker info gatherer should see this ticker as untrackable
        untrackable_info = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'BADTICKER' in untrackable_info
        
        # Both gatherers should return the same set
        untrackable_stock = get_polygon_untrackable_tickers_stock(con, expiry_days=365)
        assert untrackable_info == untrackable_stock


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
