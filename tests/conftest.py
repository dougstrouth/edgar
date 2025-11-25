"""
Pytest configuration and shared fixtures for EDGAR Analytics tests.

This file is automatically loaded by pytest and provides:
- Shared fixtures available to all tests
- Common test configuration
- Test utilities
"""

import sys
from pathlib import Path
import pytest
import duckdb
import logging
from typing import Generator

# Add project root to Python path for all tests
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import after path is set
from utils.db_validation import DatabaseValidator, validate_edgar_database


@pytest.fixture(scope="session")
def project_root():
    """Provide the project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def tests_dir():
    """Provide the tests directory."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def fixtures_dir():
    """Provide the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_ticker():
    """Provide a well-known ticker for testing."""
    return "AAPL"


@pytest.fixture
def sample_tickers():
    """Provide a list of well-known tickers for testing."""
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


# Configure pytest to handle our custom test types
def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "requires_db: marks tests that require a database"
    )
    config.addinivalue_line(
        "markers", "requires_api: marks tests that require API access"
    )


@pytest.fixture
def in_memory_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Provide an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(database=':memory:')
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def db_validator(in_memory_db) -> DatabaseValidator:
    """Provide a database validator instance."""
    return DatabaseValidator(in_memory_db)


@pytest.fixture
def sample_edgar_db(in_memory_db) -> duckdb.DuckDBPyConnection:
    """Create a sample EDGAR database with test data."""
    # Create schema
    in_memory_db.execute("""
        CREATE TABLE companies (
            cik VARCHAR(10) PRIMARY KEY,
            primary_name VARCHAR,
            entity_name_cf VARCHAR,
            entity_type VARCHAR,
            sic VARCHAR,
            sic_description VARCHAR
        )
    """)
    
    in_memory_db.execute("""
        CREATE TABLE filings (
            accession_number VARCHAR PRIMARY KEY,
            cik VARCHAR(10) NOT NULL,
            filing_date DATE,
            form VARCHAR NOT NULL
        )
    """)
    
    in_memory_db.execute("""
        CREATE TABLE tickers (
            cik VARCHAR(10) NOT NULL,
            ticker VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL
        )
    """)
    
    # Insert sample data
    in_memory_db.execute("""
        INSERT INTO companies (cik, primary_name, entity_type, sic)
        VALUES 
            ('0000320193', 'Apple Inc.', 'corporation', '3571'),
            ('0000789019', 'Microsoft Corporation', 'corporation', '7372')
    """)
    
    in_memory_db.execute("""
        INSERT INTO filings (accession_number, cik, filing_date, form)
        VALUES 
            ('0000320193-23-000077', '0000320193', '2023-11-02', '10-Q'),
            ('0000789019-23-000123', '0000789019', '2023-10-25', '10-K')
    """)
    
    in_memory_db.execute("""
        INSERT INTO tickers (cik, ticker, exchange)
        VALUES 
            ('0000320193', 'AAPL', 'NASDAQ'),
            ('0000789019', 'MSFT', 'NASDAQ')
    """)
    
    return in_memory_db

