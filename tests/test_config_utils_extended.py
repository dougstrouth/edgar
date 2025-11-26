"""Additional tests for config_utils.py to improve coverage"""
import pytest
from pathlib import Path
from utils.config_utils import AppConfig


# Helper to create valid .env with required vars
def _create_valid_env(tmp_path, extra_content=""):
    """Create .env file with required vars + optional extra content"""
    env_file = tmp_path / ".env"
    required = "DB_FILE=test.db\nDOWNLOAD_DIR=/tmp/downloads\nSEC_USER_AGENT=test-agent\n"
    env_file.write_text(required + extra_content)
    return env_file


def test_config_optional_int_with_default(tmp_path, monkeypatch):
    """Test get_optional_int returns default when var missing"""
    _create_valid_env(tmp_path)
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_int("NONEXISTENT_VAR", 42)
    assert result == 42


def test_config_optional_int_invalid_value(tmp_path, monkeypatch):
    """Test get_optional_int with non-numeric value returns default"""
    _create_valid_env(tmp_path, "INVALID_INT=notanumber\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_int("INVALID_INT", 99)
    assert result == 99


def test_config_optional_float_with_default(tmp_path, monkeypatch):
    """Test get_optional_float returns default when var missing"""
    _create_valid_env(tmp_path)
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_float("NONEXISTENT_VAR", 3.14)
    assert result == 3.14


def test_config_optional_float_invalid_value(tmp_path, monkeypatch):
    """Test get_optional_float with non-numeric value returns default"""
    _create_valid_env(tmp_path, "INVALID_FLOAT=not_a_float\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_float("INVALID_FLOAT", 1.5)
    assert result == 1.5


def test_config_optional_var_returns_none(tmp_path, monkeypatch):
    """Test get_optional_var returns None when var missing"""
    _create_valid_env(tmp_path)
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_var("MISSING_VAR")
    assert result is None


def test_config_optional_var_returns_value(tmp_path, monkeypatch):
    """Test get_optional_var returns value when var exists"""
    _create_valid_env(tmp_path, "OPTIONAL_KEY=optional_value\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_var("OPTIONAL_KEY")
    assert result == "optional_value"


def test_config_missing_db_file_raises_error(tmp_path, monkeypatch):
    """Test missing required env vars raises SystemExit"""
    env_file = tmp_path / ".env"
    env_file.write_text("# No DB_FILE defined\n")
    
    # Clear environment variables
    monkeypatch.delenv("DB_FILE", raising=False)
    monkeypatch.delenv("DOWNLOAD_DIR", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.chdir(tmp_path)
    
    with pytest.raises(SystemExit):
        AppConfig(calling_script_path=tmp_path / "test.py")


def test_config_relative_db_path(tmp_path, monkeypatch):
    """Test DB_FILE with relative path is resolved correctly"""
    _create_valid_env(tmp_path, "DB_FILE=../data/test.db\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    # Should resolve to absolute path
    assert config.DB_FILE.is_absolute()
    assert str(config.DB_FILE).endswith("test.db")


def test_config_parquet_dir_created(tmp_path, monkeypatch):
    """Test PARQUET_DIR path is derived from DOWNLOAD_DIR correctly"""
    parquet_path = tmp_path / "new_parquet_dir"
    _create_valid_env(tmp_path, f"DOWNLOAD_DIR={tmp_path / 'downloads'}\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    # PARQUET_DIR should be derived as DOWNLOAD_DIR/parquet_data
    assert config.PARQUET_DIR == tmp_path / "downloads" / "parquet_data"
    assert isinstance(config.PARQUET_DIR, Path)


def test_config_env_file_not_found_raises_error(tmp_path, monkeypatch):
    """Test missing .env file logs warning and raises SystemExit on missing required vars"""
    # Clear environment variables
    monkeypatch.delenv("DB_FILE", raising=False)
    monkeypatch.delenv("DOWNLOAD_DIR", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.chdir(tmp_path)
    
    # Missing .env means required vars won't be found
    with pytest.raises(SystemExit):
        AppConfig(calling_script_path=tmp_path / "test.py")


def test_config_int_conversion_success(tmp_path, monkeypatch):
    """Test successful integer conversion"""
    _create_valid_env(tmp_path, "INT_VALUE=42\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_int("INT_VALUE", 0)
    assert result == 42
    assert isinstance(result, int)


def test_config_float_conversion_success(tmp_path, monkeypatch):
    """Test successful float conversion"""
    _create_valid_env(tmp_path, "FLOAT_VALUE=3.14159\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_float("FLOAT_VALUE", 0.0)
    assert result is not None
    assert abs(result - 3.14159) < 0.00001
    assert isinstance(result, float)


def test_config_db_file_str_property(tmp_path, monkeypatch):
    """Test DB_FILE_STR property returns string path"""
    env_file = tmp_path / ".env"
    env_file.write_text("DB_FILE=test.db\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    assert isinstance(config.DB_FILE_STR, str)
    assert config.DB_FILE_STR.endswith("test.db")


def test_config_with_tilde_expansion(tmp_path, monkeypatch):
    """Test paths with ~ are expanded correctly"""
    env_file = tmp_path / ".env"
    # Use a path that won't actually exist but tests expansion
    env_file.write_text("DB_FILE=~/test_db/test.db\nDOWNLOAD_DIR=~/downloads\nSEC_USER_AGENT=test\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    # Should expand ~ to home directory
    assert '~' not in str(config.DB_FILE)
    assert config.DB_FILE.is_absolute()
    assert '~' not in str(config.DOWNLOAD_DIR)


def test_config_empty_optional_var(tmp_path, monkeypatch):
    """Test get_optional_var with empty string value"""
    _create_valid_env(tmp_path, "EMPTY_VAR=\n")
    
    monkeypatch.chdir(tmp_path)
    config = AppConfig(calling_script_path=tmp_path / "test.py")
    
    result = config.get_optional_var("EMPTY_VAR")
    # Empty string should be returned as-is or treated as missing
    assert result == "" or result is None
