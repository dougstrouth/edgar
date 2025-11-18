from pathlib import Path
import duckdb
import pandas as pd  # type: ignore
import pytest

from scripts.downloads_scanner import summarize


class MinimalConfig:
    def __init__(self, base: Path):
        self.DOWNLOAD_DIR = base / 'downloads'
        self.EXTRACT_BASE_DIR = base / 'extracted'
        self.PARQUET_DIR = base / 'parquet'
        self.DB_FILE = base / 'test.duckdb'
        # For compatibility with code expecting string path
        self.DB_FILE_STR = str(self.DB_FILE)
        # Ensure directories exist
        self.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        self.EXTRACT_BASE_DIR.mkdir(parents=True, exist_ok=True)
        self.PARQUET_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def tmp_config(tmp_path: Path) -> MinimalConfig:
    return MinimalConfig(tmp_path)


def test_scanner_recommends_extract_when_zip_not_extracted(tmp_config: MinimalConfig):
    # Create a dummy zip file and no corresponding extraction folder
    zip_path = tmp_config.DOWNLOAD_DIR / 'archive_20200101.zip'
    zip_path.write_bytes(b'PK\x03\x04')

    result = summarize(tmp_config, apply_changes=False, dry_run=True)  # type: ignore[arg-type]

    assert 'extract_missing_zips' in result['recommendations']


def test_scanner_recommends_parse_when_extracted_json_present(tmp_config: MinimalConfig):
    # Create extracted json under a specific archive folder
    archive_dir = tmp_config.EXTRACT_BASE_DIR / 'archive_20200101'
    archive_dir.mkdir(parents=True, exist_ok=True)
    (archive_dir / 'sample.json').write_text('{"ok": true}')

    # Ensure parquet dir is empty
    result = summarize(tmp_config, apply_changes=False, dry_run=True)  # type: ignore[arg-type]

    assert 'parse_to_parquet' in result['recommendations']


def test_scanner_recommends_load_when_new_parquet_unprocessed(tmp_config: MinimalConfig):
    # Create a simple parquet file for a table
    table_dir = tmp_config.PARQUET_DIR / 'some_table'
    table_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    parquet_file = table_dir / 'batch_0001.parquet'
    df.to_parquet(parquet_file)

    # Create a DuckDB without parquet_file_log records
    with duckdb.connect(str(tmp_config.DB_FILE)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS parquet_file_log (
                table_name VARCHAR,
                file_name VARCHAR,
                processed_ts TIMESTAMPTZ
            );
            """
        )

    result = summarize(tmp_config, apply_changes=False, dry_run=True)  # type: ignore[arg-type]

    assert 'load_parquet' in result['recommendations']
