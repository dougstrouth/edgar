"""Extended tests for database_conn.py to improve coverage"""
import pytest
import duckdb
import logging
from unittest.mock import patch
from utils.database_conn import get_db_connection, ManagedDatabaseConnection


def test_get_db_connection_memory_database():
    """Test connection to :memory: database"""
    conn = get_db_connection(db_path_override=":memory:")
    assert conn is not None
    assert isinstance(conn, duckdb.DuckDBPyConnection)
    result = conn.execute("SELECT 42 AS answer").fetchone()
    assert result is not None
    assert result[0] == 42
    conn.close()


def test_get_db_connection_with_path_object(tmp_path):
    """Test db_path_override accepts Path object"""
    db_file = tmp_path / "test_path_object.duckdb"
    conn = get_db_connection(db_path_override=db_file)
    assert conn is not None
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.close()
    assert db_file.exists()


def test_get_db_connection_with_string_path(tmp_path):
    """Test db_path_override accepts string path"""
    db_file = tmp_path / "test_string.duckdb"
    conn = get_db_connection(db_path_override=str(db_file))
    assert conn is not None
    conn.close()
    assert db_file.exists()


def test_get_db_connection_creates_parent_directory(tmp_path):
    """Test that parent directory is created if it doesn't exist"""
    db_file = tmp_path / "nested" / "deeper" / "test.duckdb"
    assert not db_file.parent.exists()
    
    conn = get_db_connection(db_path_override=db_file)
    assert conn is not None
    assert db_file.parent.exists()
    conn.close()


def test_get_db_connection_read_only_missing_file(tmp_path):
    """Test read-only connection to non-existent file returns None"""
    db_file = tmp_path / "nonexistent.duckdb"
    conn = get_db_connection(db_path_override=db_file, read_only=True)
    assert conn is None


def test_get_db_connection_read_only_existing_file(tmp_path):
    """Test read-only connection to existing file succeeds"""
    db_file = tmp_path / "existing.duckdb"
    
    # Create the database first
    setup_conn = duckdb.connect(str(db_file))
    setup_conn.execute("CREATE TABLE test (id INTEGER)")
    setup_conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    setup_conn.close()
    
    # Connect read-only
    conn = get_db_connection(db_path_override=db_file, read_only=True)
    assert conn is not None
    
    # Should be able to read
    result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
    assert result is not None
    assert result[0] == 3
    
    # Should not be able to write
    with pytest.raises(Exception):  # DuckDB raises various exceptions for read-only violations
        conn.execute("INSERT INTO test VALUES (4)")
    
    conn.close()


def test_get_db_connection_with_pragma_settings(tmp_path):
    """Test applying PRAGMA settings on connection"""
    db_file = tmp_path / "pragma_test.duckdb"
    pragma_settings = {
        "enable_object_cache": "true"
    }
    
    conn = get_db_connection(db_path_override=db_file, pragma_settings=pragma_settings)
    assert conn is not None
    
    # Connection succeeded with PRAGMA settings applied
    conn.close()


def test_get_db_connection_with_string_pragma(tmp_path):
    """Test PRAGMA settings with string values"""
    db_file = tmp_path / "string_pragma.duckdb"
    pragma_settings = {
        "temp_directory": str(tmp_path / "temp")
    }
    
    conn = get_db_connection(db_path_override=db_file, pragma_settings=pragma_settings)
    assert conn is not None
    conn.close()


def test_get_db_connection_invalid_pragma_warning(tmp_path, caplog):
    """Test that invalid PRAGMA settings log warnings but don't fail"""
    db_file = tmp_path / "invalid_pragma.duckdb"
    pragma_settings = {
        "invalid_pragma_that_does_not_exist": "value"
    }
    
    with caplog.at_level(logging.WARNING):
        conn = get_db_connection(db_path_override=db_file, pragma_settings=pragma_settings)
        assert conn is not None  # Connection should still succeed
        conn.close()
    
    # Check that warning was logged
    assert any("Could not set PRAGMA" in record.message for record in caplog.records)


def test_get_db_connection_from_environment(tmp_path, monkeypatch):
    """Test reading DB_FILE from environment variable"""
    db_file = tmp_path / "env_test.duckdb"
    monkeypatch.setenv("DB_FILE", str(db_file))
    
    conn = get_db_connection()
    assert conn is not None
    conn.close()
    assert db_file.exists()


def test_get_db_connection_missing_env_var_raises_error(monkeypatch):
    """Test that missing DB_FILE env var raises KeyError"""
    monkeypatch.delenv("DB_FILE", raising=False)
    
    with pytest.raises(KeyError):
        get_db_connection()


def test_get_db_connection_directory_creation_failure(tmp_path, monkeypatch):
    """Test handling of directory creation failure"""
    db_file = tmp_path / "readonly_parent" / "test.duckdb"
    
    # Make parent directory read-only to prevent creation
    parent = tmp_path / "readonly_parent"
    parent.mkdir()
    
    # Mock mkdir to raise PermissionError
    with patch('pathlib.Path.mkdir', side_effect=PermissionError("Cannot create directory")):
        conn = get_db_connection(db_path_override=db_file)
        assert conn is None  # Should return None on directory creation failure


def test_managed_database_connection_success(tmp_path):
    """Test ManagedDatabaseConnection context manager"""
    db_file = tmp_path / "managed.duckdb"
    
    with ManagedDatabaseConnection(db_path_override=str(db_file)) as conn:
        assert conn is not None
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
    
    # Verify connection was closed and data persists
    new_conn = duckdb.connect(str(db_file))
    result = new_conn.execute("SELECT COUNT(*) FROM test").fetchone()
    assert result is not None
    assert result[0] == 1
    new_conn.close()


def test_managed_database_connection_read_only(tmp_path):
    """Test ManagedDatabaseConnection with read_only flag"""
    db_file = tmp_path / "readonly_managed.duckdb"
    
    # Create database first
    setup_conn = duckdb.connect(str(db_file))
    setup_conn.execute("CREATE TABLE test (id INTEGER)")
    setup_conn.close()
    
    with ManagedDatabaseConnection(db_path_override=str(db_file), read_only=True) as conn:
        assert conn is not None
        result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
        assert result is not None
        assert result[0] == 0


def test_managed_database_connection_with_pragma(tmp_path):
    """Test ManagedDatabaseConnection with PRAGMA settings"""
    db_file = tmp_path / "pragma_managed.duckdb"
    pragma_settings = {"enable_object_cache": "true"}
    
    with ManagedDatabaseConnection(
        db_path_override=str(db_file),
        pragma_settings=pragma_settings
    ) as conn:
        assert conn is not None
        # Connection succeeded with PRAGMA


def test_managed_database_connection_failure():
    """Test ManagedDatabaseConnection when connection fails"""
    # Try to connect read-only to non-existent file
    with ManagedDatabaseConnection(
        db_path_override="/nonexistent/path/to/db.duckdb",
        read_only=True
    ) as conn:
        assert conn is None  # Connection should fail


def test_managed_database_connection_exception_propagation(tmp_path):
    """Test that exceptions inside context manager are propagated"""
    db_file = tmp_path / "exception_test.duckdb"
    
    with pytest.raises(ValueError, match="Test exception"):
        with ManagedDatabaseConnection(db_path_override=str(db_file)) as conn:
            assert conn is not None
            raise ValueError("Test exception")


def test_get_db_connection_empty_path_string():
    """Test that empty path string returns None"""
    # This shouldn't normally happen, but test defensive coding
    with patch.dict('os.environ', {'DB_FILE': ''}):
        conn = get_db_connection()
        assert conn is None


def test_get_db_connection_logs_info_messages(tmp_path, caplog):
    """Test that connection logs informational messages"""
    db_file = tmp_path / "logging_test.duckdb"
    
    with caplog.at_level(logging.INFO):
        conn = get_db_connection(db_path_override=db_file)
        assert conn is not None
        conn.close()
    
    # Check for expected log messages
    assert any("Using override DB path" in record.message for record in caplog.records)
    assert any("Connected:" in record.message for record in caplog.records)
