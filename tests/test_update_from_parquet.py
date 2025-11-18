# -*- coding: utf-8 -*-
"""
Unit test for incremental parquet loader `update_from_parquet.py`.
"""
from pathlib import Path
import os
import pandas as pd
import pytest

# Ensure project imports work
import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from update_from_parquet import main as run_incremental
from utils.database_conn import ManagedDatabaseConnection


def write_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow', index=False)


def test_incremental_load_creates_tables_and_logs(tmp_path, monkeypatch):
    # Setup environment variables expected by AppConfig
    download_dir = tmp_path / 'downloads'
    download_dir.mkdir()
    db_file = tmp_path / 'test.duckdb'

    monkeypatch.setenv('DOWNLOAD_DIR', str(download_dir))
    monkeypatch.setenv('DB_FILE', str(db_file))
    monkeypatch.setenv('SEC_USER_AGENT', 'TestAgent test@example.com')

    # Prepare parquet files
    parquet_root = download_dir / 'parquet_data'
    companies_dir = parquet_root / 'companies'
    filings_dir = parquet_root / 'filings'

    # Minimal companies DF
    df_comp = pd.DataFrame([{
        'cik': '0000000001',
        'primary_name': 'Test Co',
        'last_parsed_timestamp': pd.Timestamp('2025-01-01T00:00:00Z')
    }])
    write_parquet(df_comp, companies_dir / 'batch_comp.parquet')

    # Minimal filings DF
    df_fil = pd.DataFrame([{
        'accession_number': 'ACC-1',
        'cik': '0000000001',
        'form': '10-K',
        'filing_date': '2025-01-01'
    }])
    write_parquet(df_fil, filings_dir / 'batch_fil.parquet')

    # Build a minimal config object to pass into the runner to avoid loading repo .env
    class MinimalConfig:
        def __init__(self, download_dir, db_file):
            self.PARQUET_DIR = download_dir / 'parquet_data'
            self.DB_FILE_STR = str(db_file)
            self.DB_FILE = db_file
            self.DUCKDB_TEMP_DIR = None
            self.DUCKDB_MEMORY_LIMIT = '1GB'

    cfg = MinimalConfig(download_dir, db_file)

    # Run incremental loader with provided config
    rc = run_incremental(config=cfg)
    assert rc == 0

    # Validate DB contents
    with ManagedDatabaseConnection(db_path_override=str(db_file), read_only=True) as conn:
        assert conn is not None
        res = conn.execute("SELECT cik, primary_name FROM companies").fetchall()
        assert len(res) == 1
        assert res[0][0] == '0000000001'

        res2 = conn.execute("SELECT accession_number FROM filings").fetchall()
        assert len(res2) == 1
        assert res2[0][0] == 'ACC-1'

        # Check parquet_file_log
        logs = conn.execute("SELECT table_name, file_name FROM parquet_file_log").fetchall()
        assert ('companies', 'batch_comp.parquet') in logs or any(l[0]=='companies' and l[1]=='batch_comp.parquet' for l in logs)
        assert ('filings', 'batch_fil.parquet') in logs or any(l[0]=='filings' and l[1]=='batch_fil.parquet' for l in logs)
