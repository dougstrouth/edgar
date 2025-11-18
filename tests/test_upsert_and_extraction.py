import logging
from pathlib import Path
import zipfile
import json

import duckdb
import pytest

from data_gathering.fetch_edgar_archives import (
    setup_database_and_table,
    upsert_archive_records,
    extract_zip_json_members,
    extract_and_sample_zip_archive,
)


logger = logging.getLogger("tests")
logger.setLevel(logging.DEBUG)


def test_upsert_archive_records_merge_behavior():
    # Use an in-memory DuckDB for testing
    conn = duckdb.connect(database=':memory:')

    # setup table
    setup_database_and_table(conn, Path(':memory:'), logger)

    records = [
        {
            'file_path': 'a.zip',
            'file_name': 'a.zip',
            'url': 'http://example/a.zip',
            'size_bytes': 100,
            'local_last_modified_utc': None,
            'download_timestamp_utc': None,
            'status': 'Downloaded',
        },
        {
            'file_path': 'b.zip',
            'file_name': 'b.zip',
            'url': 'http://example/b.zip',
            'size_bytes': 200,
            'local_last_modified_utc': None,
            'download_timestamp_utc': None,
            'status': 'Downloaded',
        },
    ]

    upsert_archive_records(conn, records, logger)

    res = conn.execute("SELECT file_path, status, size_bytes FROM downloaded_archives ORDER BY file_path").fetchall()
    assert len(res) == 2
    assert ('a.zip', 'Downloaded', 100) in res

    # Update record a.zip
    records_update = [
        {
            'file_path': 'a.zip',
            'file_name': 'a2.zip',
            'url': 'http://example/a2.zip',
            'size_bytes': 150,
            'local_last_modified_utc': None,
            'download_timestamp_utc': None,
            'status': 'Updated',
        }
    ]
    upsert_archive_records(conn, records_update, logger)

    updated = conn.execute("SELECT file_name, status, size_bytes FROM downloaded_archives WHERE file_path = 'a.zip'").fetchone()
    assert updated[0] == 'a2.zip'
    assert updated[1] == 'Updated'
    assert updated[2] == 150


def _create_test_zip(zip_path: Path, files: dict):
    with zipfile.ZipFile(zip_path, 'w') as z:
        for name, content in files.items():
            z.writestr(name, content)


def test_extract_zip_json_members_and_sample(tmp_path: Path):
    zip_path = tmp_path / 'sample.zip'
    files = {
        'one.json': json.dumps({'a': 1}),
        'two.json': json.dumps({'b': 2}),
    }
    _create_test_zip(zip_path, files)

    out_dir = tmp_path / 'out'
    extracted = extract_zip_json_members(zip_path, out_dir, max_workers=2, logger=logger)
    assert len(extracted) == 2
    for p in extracted:
        assert p.exists()

    # Test extract_and_sample_zip_archive returns sample
    status, sample = extract_and_sample_zip_archive(zip_path, out_dir, max_workers=2, logger=logger)
    assert status in ("Extracted", "No JSON Found")
    if status == "Extracted":
        assert sample is not None and isinstance(sample, str)
