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

# Add project root to Python path for all tests
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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
