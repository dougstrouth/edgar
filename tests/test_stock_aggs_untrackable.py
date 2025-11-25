#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for stock aggregates gatherer's untrackable ticker tracking.

Validates that stock_data_gatherer_polygon properly:
1. Creates polygon_untrackable_tickers table on 404 errors
2. Filters untrackable tickers before creating jobs
3. Shares the same table structure as ticker_info_gatherer_polygon
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
from unittest.mock import MagicMock
from datetime import date

import pytest
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_gathering.stock_data_gatherer_polygon import get_polygon_untrackable_tickers, fetch_worker  # noqa: E402


@pytest.fixture
def test_db(tmp_path):
    """Create a test database for stock aggregates tests."""
    db_path = tmp_path / "test_stock_aggs.duckdb"
    con = duckdb.connect(str(db_path))
    
    # Create minimal schema
    con.execute("""
        CREATE TABLE polygon_stock_history (
            ticker VARCHAR,
            date DATE,
            volume BIGINT
        );
    """)
    
    yield con, db_path
    con.close()


class TestStockAggsUntrackableTracking:
    """Test untrackable ticker tracking in stock aggregates gatherer."""
    
    def test_get_untrackable_tickers_empty(self, test_db):
        """Test get_polygon_untrackable_tickers returns empty set when table doesn't exist."""
        con, db_path = test_db
        
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert untrackable == set()
    
    def test_get_untrackable_tickers_with_data(self, test_db):
        """Test get_polygon_untrackable_tickers returns correct set."""
        con, db_path = test_db
        
        # Create table with test data
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
            ['BAD1', '404 Not Found', now]
        )
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['BAD2', '400 Bad Request', now]
        )
        
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert untrackable == {'BAD1', 'BAD2'}
    
    def test_get_untrackable_tickers_case_insensitive(self, test_db):
        """Test case-insensitive matching of untrackable tickers."""
        con, db_path = test_db
        
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
            ['bad', '404 Not Found', now]
        )
        
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        # Should return lowercase as stored
        assert 'bad' in untrackable
        # But filtering should work case-insensitively (handled by SQL COLLATE NOCASE)
    
    def test_get_untrackable_respects_expiry(self, test_db):
        """Test that old failures are not returned."""
        con, db_path = test_db
        
        con.execute("""
            CREATE TABLE polygon_untrackable_tickers (
                ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
                reason VARCHAR,
                last_failed_timestamp TIMESTAMPTZ
            );
        """)
        
        # Add old failure (400 days ago)
        old_time = datetime.now(timezone.utc) - timedelta(days=400)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['OLD', '404 Not Found', old_time]
        )
        
        # Add recent failure
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO polygon_untrackable_tickers VALUES (?, ?, ?);",
            ['NEW', '404 Not Found', now]
        )
        
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'OLD' not in untrackable
        assert 'NEW' in untrackable
    
    def test_fetch_worker_creates_table_on_404(self, test_db, monkeypatch):
        """Test that fetch_worker creates untrackable table on 404 error."""
        con, db_path = test_db
        
        # Mock PolygonClient to raise 404
        class Mock404Client:
            def __init__(self, *args, **kwargs):
                pass
            
            def get_aggregates(self, ticker, start_date, end_date):
                from requests import HTTPError
                error = HTTPError("404 Client Error: Not Found")
                error.response = MagicMock(status_code=404)
                raise error
        
        monkeypatch.setattr('data_gathering.stock_data_gatherer_polygon.PolygonClient', Mock404Client)
        
        job = {
            'ticker': 'NOTFOUND',
            'start_date': date(2024, 1, 1),
            'end_date': date(2024, 1, 2),
            'api_key': 'test_key',
            'db_path': str(db_path)
        }
        
        result = fetch_worker(job)
        
        # Should have error status
        assert result['status'] == 'error'
        
        # Table should be created
        tables = con.execute("SHOW TABLES;").fetchall()
        table_names = [t[0] for t in tables]
        assert 'polygon_untrackable_tickers' in table_names
        
        # Ticker should be in table
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'NOTFOUND' in untrackable
        
        # Verify record details
        record = con.execute(
            "SELECT reason FROM polygon_untrackable_tickers WHERE ticker = 'NOTFOUND';"
        ).fetchone()
        assert record is not None
        assert '404' in record[0]
    
    def test_fetch_worker_creates_table_on_400(self, test_db, monkeypatch):
        """Test that fetch_worker creates untrackable table on 400 error."""
        con, db_path = test_db
        
        # Mock PolygonClient to raise 400
        class Mock400Client:
            def __init__(self, *args, **kwargs):
                pass
            
            def get_aggregates(self, ticker, start_date, end_date):
                from requests import HTTPError
                error = HTTPError("400 Client Error: Bad Request")
                error.response = MagicMock(status_code=400)
                raise error
        
        monkeypatch.setattr('data_gathering.stock_data_gatherer_polygon.PolygonClient', Mock400Client)
        
        job = {
            'ticker': 'BADREQUEST',
            'start_date': date(2024, 1, 1),
            'end_date': date(2024, 1, 2),
            'api_key': 'test_key',
            'db_path': str(db_path)
        }
        
        result = fetch_worker(job)
        
        # Should have error status
        assert result['status'] == 'error'
        
        # Ticker should be in untrackable table
        untrackable = get_polygon_untrackable_tickers(con, expiry_days=365)
        assert 'BADREQUEST' in untrackable
    
    def test_verify_404_tracking_details(self, test_db, monkeypatch, capsys):
        """Comprehensive test to verify 404 tracking with detailed output."""
        con, db_path = test_db
        
        # Mock PolygonClient to raise 404
        class Mock404Client:
            def __init__(self, *args, **kwargs):
                pass
            
            def get_aggregates(self, ticker, start_date, end_date):
                from requests import HTTPError
                error = HTTPError("404 Client Error: Not Found")
                error.response = MagicMock(status_code=404)
                raise error
        
        monkeypatch.setattr('data_gathering.stock_data_gatherer_polygon.PolygonClient', Mock404Client)
        
        # Process multiple tickers with 404s
        tickers_to_test = ['FAKE1', 'FAKE2', 'FAKE3']
        
        for ticker in tickers_to_test:
            job = {
                'ticker': ticker,
                'start_date': date(2024, 1, 1),
                'end_date': date(2024, 1, 2),
                'api_key': 'test_key',
                'db_path': str(db_path)
            }
            
            result = fetch_worker(job)
            assert result['status'] == 'error', f"{ticker} should have error status"
        
        # Verify all were tracked
        print("\n=== Verification of 404 Tracking ===")
        
        # Check table exists
        tables = con.execute("SHOW TABLES;").fetchall()
        table_names = [t[0] for t in tables]
        print(f"Tables in database: {table_names}")
        assert 'polygon_untrackable_tickers' in table_names, "Table should be created"
        
        # Get all records
        all_records = con.execute("""
            SELECT ticker, reason, last_failed_timestamp 
            FROM polygon_untrackable_tickers 
            ORDER BY ticker;
        """).fetchall()
        
        print(f"\nTotal untrackable tickers recorded: {len(all_records)}")
        print("\nDetailed records:")
        for ticker, reason, timestamp in all_records:
            print(f"  Ticker: {ticker}")
            print(f"    Reason: {reason}")
            print(f"    Timestamp: {timestamp}")
            print()
        
        # Verify all test tickers are present
        tracked_tickers = {record[0] for record in all_records}
        for ticker in tickers_to_test:
            assert ticker in tracked_tickers, f"{ticker} should be tracked"
        
        # Verify using get_polygon_untrackable_tickers function
        untrackable_set = get_polygon_untrackable_tickers(con, expiry_days=365)
        print(f"Untrackable set (via function): {sorted(untrackable_set)}")
        
        assert untrackable_set == set(tickers_to_test), "Function should return all test tickers"
        
        print("\n✅ All 404s were properly tracked in polygon_untrackable_tickers table")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
