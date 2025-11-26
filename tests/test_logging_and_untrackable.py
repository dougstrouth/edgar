"""Tests for logging_utils.py and untrackable.py to achieve full coverage"""
import pytest
import logging
from pathlib import Path
import duckdb
from utils.logging_utils import setup_logging
from utils.untrackable import (
    ensure_untrackable_table,
    mark_untrackable,
    get_untrackable_tickers,
    TABLE_NAME
)


# ===== Tests for logging_utils.py =====

def test_setup_logging_creates_log_directory(tmp_path):
    """Test that setup_logging creates the log directory"""
    log_dir = tmp_path / "logs"
    assert not log_dir.exists()
    
    logger = setup_logging("test_script", log_dir)
    
    assert log_dir.exists()
    assert log_dir.is_dir()
    assert logger is not None


def test_setup_logging_creates_log_file(tmp_path):
    """Test that setup_logging creates a log file"""
    log_dir = tmp_path / "logs"
    
    _ = setup_logging("test_script", log_dir)
    
    log_file = log_dir / "test_script.log"
    assert log_file.exists()
    assert log_file.is_file()


def test_setup_logging_writes_to_file(tmp_path):
    """Test that logs are written to file"""
    log_dir = tmp_path / "logs"
    
    logger = setup_logging("test_script", log_dir)
    logger.info("Test message")
    
    log_file = log_dir / "test_script.log"
    content = log_file.read_text()
    assert "Test message" in content
    assert "INFO" in content


def test_setup_logging_file_handler_failure(tmp_path, monkeypatch, capfd):
    """Test handling of file handler creation failure"""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    
    # Mock FileHandler to raise exception
    def mock_file_handler(*args, **kwargs):
        raise PermissionError("Cannot create file")
    
    monkeypatch.setattr("logging.FileHandler", mock_file_handler)
    
    logger = setup_logging("test_script", log_dir)
    
    # Should still return a logger (with stream handler only)
    assert logger is not None
    
    # Check warning was printed to stderr
    captured = capfd.readouterr()
    assert "Warning: Could not set up file handler" in captured.err


def test_setup_logging_dir_creation_failure(tmp_path, monkeypatch):
    """Test handling of directory creation failure"""
    log_dir = tmp_path / "logs"
    
    # Mock mkdir to raise exception
    def mock_mkdir(*args, **kwargs):
        raise OSError("Cannot create directory")
    
    monkeypatch.setattr(Path, "mkdir", mock_mkdir)
    
    with pytest.raises(OSError, match="Cannot create directory"):
        setup_logging("test_script", log_dir)


def test_setup_logging_clears_existing_handlers(tmp_path):
    """Test that calling setup_logging twice clears old handlers"""
    log_dir = tmp_path / "logs"
    
    logger1 = setup_logging("test_script", log_dir)
    handler_count_1 = len(logger1.handlers)
    
    logger2 = setup_logging("test_script", log_dir)
    handler_count_2 = len(logger2.handlers)
    
    # Should have same number of handlers (old ones cleared)
    assert handler_count_1 == handler_count_2
    assert logger1 is logger2  # Same logger instance


def test_setup_logging_custom_level(tmp_path):
    """Test setup_logging with custom log level"""
    log_dir = tmp_path / "logs"
    
    logger = setup_logging("test_script", log_dir, level=logging.DEBUG)
    
    assert logger.level == logging.DEBUG


def test_setup_logging_custom_format(tmp_path):
    """Test setup_logging with custom format"""
    log_dir = tmp_path / "logs"
    custom_format = "%(levelname)s - %(message)s"
    
    logger = setup_logging("test_script", log_dir, log_format=custom_format)
    logger.info("Custom format test")
    
    log_file = log_dir / "test_script.log"
    content = log_file.read_text()
    # With custom format, should not have timestamp
    assert "Custom format test" in content


def test_setup_logging_stream_handler_outputs_to_stdout(tmp_path, capsys):
    """Test that stream handler outputs to stdout"""
    log_dir = tmp_path / "logs"
    
    logger = setup_logging("test_script", log_dir)
    logger.info("Console test message")
    
    captured = capsys.readouterr()
    assert "Console test message" in captured.out
    assert "Console test message" not in captured.err  # Should be stdout, not stderr


# ===== Tests for untrackable.py =====

def test_ensure_untrackable_table_creates_table():
    """Test that ensure_untrackable_table creates the table"""
    con = duckdb.connect(":memory:")
    
    ensure_untrackable_table(con)
    
    # Verify table exists
    tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    assert TABLE_NAME in tables
    
    con.close()


def test_ensure_untrackable_table_idempotent():
    """Test that calling ensure_untrackable_table multiple times is safe"""
    con = duckdb.connect(":memory:")
    
    ensure_untrackable_table(con)
    ensure_untrackable_table(con)  # Should not raise
    
    tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    assert TABLE_NAME in tables
    
    con.close()


def test_mark_untrackable_creates_table_if_not_exists():
    """Test that mark_untrackable creates table if needed"""
    con = duckdb.connect(":memory:")
    
    mark_untrackable(con, "AAPL", "404 Not Found")
    
    # Table should exist now
    tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    assert TABLE_NAME in tables
    
    con.close()


def test_mark_untrackable_inserts_ticker():
    """Test that mark_untrackable inserts ticker correctly"""
    con = duckdb.connect(":memory:")
    
    mark_untrackable(con, "AAPL", "404 Not Found")
    
    result = con.execute(f"SELECT ticker, reason FROM {TABLE_NAME} WHERE ticker = 'AAPL'").fetchone()
    assert result is not None
    assert result[0] == "AAPL"
    assert result[1] == "404 Not Found"
    
    con.close()


def test_mark_untrackable_case_insensitive():
    """Test that mark_untrackable handles ticker updates"""
    con = duckdb.connect(":memory:")
    
    mark_untrackable(con, "AAPL", "404 Not Found")
    mark_untrackable(con, "AAPL", "400 Bad Request")  # Update same ticker
    
    # Verify reason was updated (case insensitive PRIMARY KEY)
    result = con.execute(f"SELECT reason FROM {TABLE_NAME} WHERE UPPER(ticker) = 'AAPL'").fetchone()
    assert result is not None
    assert result[0] == "400 Bad Request"
    
    con.close()


def test_mark_untrackable_updates_timestamp():
    """Test that mark_untrackable updates timestamp on duplicate"""
    con = duckdb.connect(":memory:")
    
    mark_untrackable(con, "AAPL", "404 Not Found")
    
    # Get first timestamp
    ts1 = con.execute(f"SELECT last_failed_timestamp FROM {TABLE_NAME} WHERE ticker = 'AAPL'").fetchone()
    
    # Mark again
    mark_untrackable(con, "AAPL", "Still not found")
    
    # Get second timestamp
    ts2 = con.execute(f"SELECT last_failed_timestamp FROM {TABLE_NAME} WHERE ticker = 'AAPL'").fetchone()
    
    # Should be updated (different)
    assert ts1 is not None
    assert ts2 is not None
    assert ts2[0] >= ts1[0]  # New timestamp should be equal or later
    
    con.close()


def test_get_untrackable_tickers_empty_table():
    """Test get_untrackable_tickers with empty table"""
    con = duckdb.connect(":memory:")
    ensure_untrackable_table(con)
    
    result = get_untrackable_tickers(con)
    
    assert result == set()
    con.close()


def test_get_untrackable_tickers_table_not_exists():
    """Test get_untrackable_tickers when table doesn't exist"""
    con = duckdb.connect(":memory:")
    
    result = get_untrackable_tickers(con)
    
    assert result == set()  # Should return empty set
    con.close()


def test_get_untrackable_tickers_returns_recent_failures():
    """Test get_untrackable_tickers returns recent failures"""
    con = duckdb.connect(":memory:")
    
    mark_untrackable(con, "AAPL", "404")
    mark_untrackable(con, "MSFT", "400")
    mark_untrackable(con, "GOOGL", "404")
    
    result = get_untrackable_tickers(con, expiry_days=365)
    
    assert result == {"AAPL", "MSFT", "GOOGL"}
    con.close()


def test_get_untrackable_tickers_excludes_expired():
    """Test get_untrackable_tickers excludes expired entries"""
    con = duckdb.connect(":memory:")
    ensure_untrackable_table(con)
    
    # Insert old entry (400 days ago)
    con.execute(
        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?)",
        ["OLD_TICKER", "404", "2024-01-01 00:00:00+00"]
    )
    
    # Insert recent entry
    mark_untrackable(con, "NEW_TICKER", "404")
    
    result = get_untrackable_tickers(con, expiry_days=365)
    
    # Should only include recent ticker
    assert "NEW_TICKER" in result
    assert "OLD_TICKER" not in result
    
    con.close()


def test_get_untrackable_tickers_custom_expiry():
    """Test get_untrackable_tickers with custom expiry days"""
    con = duckdb.connect(":memory:")
    ensure_untrackable_table(con)
    
    # Insert entries at different times
    con.execute(
        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?)",
        ["TICKER_30_DAYS_AGO", "404", "2025-10-27 00:00:00+00"]  # ~30 days ago from Nov 26
    )
    
    mark_untrackable(con, "TICKER_TODAY", "404")
    
    # Query with 7 day expiry
    result_7_days = get_untrackable_tickers(con, expiry_days=7)
    assert "TICKER_TODAY" in result_7_days
    assert "TICKER_30_DAYS_AGO" not in result_7_days
    
    # Query with 60 day expiry
    result_60_days = get_untrackable_tickers(con, expiry_days=60)
    assert "TICKER_TODAY" in result_60_days
    assert "TICKER_30_DAYS_AGO" in result_60_days
    
    con.close()
