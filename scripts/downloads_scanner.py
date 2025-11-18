# -*- coding: utf-8 -*-
"""
Downloads Scanner - Inspect current downloads, extractions, and parquet state
and recommend the next pipeline actions.

This script examines:
- DOWNLOAD_DIR for zip/json archives
- EXTRACT_BASE_DIR for extracted JSONs
- PARQUET_DIR for per-table parquet batches
- DuckDB catalog tables `downloaded_archives` and `parquet_file_log` (if available)

It prints a short plan of recommended next actions (fetch/extract, parse-to-parquet, load).
Optionally, with `--apply`, it will invoke `main.py` to run the suggested steps.
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Any

# Ensure repo root on path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / 'utils'))

from utils.config_utils import AppConfig  # noqa: E402
from utils.logging_utils import setup_logging  # noqa: E402
from utils.database_conn import ManagedDatabaseConnection  # noqa: E402


def find_parquet_files(parquet_dir: Path) -> Dict[str, List[Path]]:
    result: Dict[str, List[Path]] = {}
    if not parquet_dir.exists():
        return result
    for child in parquet_dir.iterdir():
        if child.is_dir():
            files = sorted(child.glob('*.parquet'))
            result[child.name] = files
    return result


def get_tables_in_db(conn) -> List[str]:
    try:
        rows = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_processed_parquet_files(conn) -> List[str]:
    try:
        rows = conn.execute("SELECT file_name FROM parquet_file_log").fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_downloaded_archives(conn) -> List[str]:
    try:
        rows = conn.execute("SELECT file_name FROM downloaded_archives").fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def summarize(config: AppConfig, apply_changes: bool = False, dry_run: bool = True, force: bool = False):
    logger = setup_logging('downloads_scanner', Path(__file__).resolve().parent / 'logs', level=logging.INFO)
    logger.info('Starting downloads scanner')

    download_dir = config.DOWNLOAD_DIR
    extract_dir = config.EXTRACT_BASE_DIR
    parquet_dir = config.PARQUET_DIR

    findings: Dict[str, Any] = {}

    # Files in download dir
    zips = sorted(list(download_dir.glob('*.zip')))
    jsons = sorted(list(download_dir.glob('*.json')))
    findings['zip_files'] = [p.name for p in zips]
    findings['download_json_files'] = [p.name for p in jsons]

    # Check extraction directories
    missing_extracts = []
    for z in zips:
        stem = z.stem
        specific = extract_dir / stem
        if not specific.exists() or not any(specific.iterdir()):
            missing_extracts.append(z.name)
    findings['zip_extracts_missing'] = missing_extracts

    # Count extracted JSONs
    extracted_jsons = []
    if extract_dir.exists():
        for sub in extract_dir.iterdir():
            if sub.is_dir():
                for js in sub.rglob('*.json'):
                    extracted_jsons.append(str(js.relative_to(extract_dir)))
    findings['extracted_jsons_count'] = len(extracted_jsons)

    # Parquet files
    parquet_files = find_parquet_files(parquet_dir)
    findings['parquet_files'] = {k: [p.name for p in v] for k, v in parquet_files.items()}

    # DB-inspected metadata
    db_tables = []
    processed_parquet = []
    downloaded_archives = []
    with ManagedDatabaseConnection(db_path_override=str(config.DB_FILE), read_only=True) as conn:
        if conn:
            db_tables = get_tables_in_db(conn)
            if 'parquet_file_log' in db_tables:
                processed_parquet = get_processed_parquet_files(conn)
            if 'downloaded_archives' in db_tables:
                downloaded_archives = get_downloaded_archives(conn)

    findings['db_tables'] = db_tables
    findings['processed_parquet_files'] = processed_parquet
    findings['downloaded_archives_in_db'] = downloaded_archives

    # Decide recommended actions
    recommendations: List[str] = []

    if findings['zip_extracts_missing']:
        recommendations.append('extract_missing_zips')

    if findings['extracted_jsons_count'] > 0:
        # crude check: if parquet dirs empty but extracted jsons exist, recommend parse
        parquet_empty = all(len(v) == 0 for v in parquet_files.values()) if parquet_files else True
        if parquet_empty:
            recommendations.append('parse_to_parquet')

    # If any parquet files are present and not in processed_parquet_files -> load
    any_new_parquet = False
    for table, files in parquet_files.items():
        for f in files:
            fname = f.name
            if fname not in processed_parquet:
                any_new_parquet = True
                break
        if any_new_parquet:
            break
    if any_new_parquet:
        recommendations.append('load_parquet')

    # Print summary
    print('\nDownloads Scanner Summary')
    print('-------------------------')
    print(f"Download dir: {download_dir}")
    print(f"Found {len(findings['zip_files'])} zip(s), {len(findings['download_json_files'])} json file(s) in DOWNLOAD_DIR")
    if findings['zip_extracts_missing']:
        print(f"Zips missing extraction: {findings['zip_extracts_missing']}")
    print(f"Extracted JSONs (total): {findings['extracted_jsons_count']}")
    print(f"Parquet directories: {list(findings['parquet_files'].keys())}")
    if findings['processed_parquet_files']:
        print(f"Parquet files already processed (in DB): {len(findings['processed_parquet_files'])}")
    print(f"DB tables detected: {findings['db_tables']}")

    if recommendations:
        print('\nRecommended Next Actions:')
        for r in recommendations:
            if r == 'extract_missing_zips':
                print('- Extract missing ZIP archives (run: `python main.py fetch`)')
            elif r == 'parse_to_parquet':
                print('- Parse extracted JSONs to Parquet (run: `python main.py parse-to-parquet`)')
            elif r == 'load_parquet':
                print('- Load new Parquet batches into DuckDB (run: `python main.py load`)')
    else:
        print('\nNo actions recommended. Pipeline appears up-to-date.')

    # If apply_changes requested, run main.py steps in order
    if apply_changes:
        print('\n-- Applying recommended actions --')
        # Map action to main steps
        action_to_cmd = {
            'extract_missing_zips': ['fetch'],
            'parse_to_parquet': ['parse-to-parquet'],
            'load_parquet': ['load']
        }
        for action in recommendations:
            steps = action_to_cmd.get(action, [])
            for step in steps:
                cmd = [sys.executable, str(PROJECT_ROOT / 'main.py'), step]
                print(f"Running: {' '.join(cmd)}")
                if dry_run:
                    print('(dry-run) Skipping execution')
                else:
                    try:
                        proc = subprocess.run(cmd, check=False)
                        print(f"Step '{step}' exited with code {proc.returncode}")
                    except Exception as e:
                        print(f"Failed to run step {step}: {e}")

    # Final return code behavior
    if recommendations and not dry_run and not apply_changes:
        print('\nNote: Recommendations available. Rerun with --apply --no-dry-run to execute.')

    # Return structured result for programmatic use/testing
    return {
        'findings': findings,
        'recommendations': recommendations
    }


def cli():
    parser = argparse.ArgumentParser(description='Scan downloads/extracts/parquet state and recommend next actions')
    parser.add_argument('--apply', action='store_true', help='Apply recommended actions by invoking main pipeline steps')
    parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=True, help='When applying, do not actually execute steps (default)')
    parser.add_argument('--no-dry-run', dest='dry_run', action='store_false', help='When applying, execute steps')
    parser.add_argument('--force', action='store_true', help='Force recommended actions even if checks are ambiguous')

    args = parser.parse_args()

    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit:
        print('Configuration failed. Ensure .env is present and valid.')
        sys.exit(1)

    summarize(config, apply_changes=args.apply, dry_run=args.dry_run, force=args.force)


if __name__ == '__main__':
    cli()
