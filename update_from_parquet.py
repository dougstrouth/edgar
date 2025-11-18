# -*- coding: utf-8 -*-
"""
Incremental Parquet → DuckDB loader

Scans the `PARQUET_DIR` for new Parquet batch files (per-table subdirectories),
loads only new files into a staging table, and merges/upserts into the
existing DuckDB tables. Tracks processed files in a `parquet_file_log`
meta table so subsequent runs skip already-applied files.

This runner is intended to be fast for daily incremental loads.
"""

import logging
import sys
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection

# Tables to process and their primary key columns (used for deduplication)
TABLE_PK_MAP = {
    'companies': ['cik'],
    'tickers': ['ticker', 'exchange'],
    'former_names': ['cik', 'former_name', 'date_from'],
    'filings': ['accession_number'],
    'filing_summaries': ['accession_number'],
    'xbrl_tags': ['taxonomy', 'tag_name'],
    'xbrl_facts': ['cik', 'accession_number', 'taxonomy', 'tag_name', 'unit', 'period_end_date', 'frame'],
}

# Optional ORDER BY preferences when deduplicating (helps pick latest record)
TABLE_ORDER_MAP = {
    'filings': 'filing_date DESC',
    'companies': 'last_parsed_timestamp DESC',
    'xbrl_facts': 'filed_date DESC',
}


def _ensure_parquet_log_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parquet_file_log (
            table_name VARCHAR NOT NULL,
            file_name VARCHAR NOT NULL,
            processed_ts TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (table_name, file_name)
        );
    """)


def _get_already_processed_files(conn, table_name: str) -> List[str]:
    res = conn.execute("SELECT file_name FROM parquet_file_log WHERE table_name = ?;", [table_name]).fetchall()
    return [r[0] for r in res] if res else []


def _mark_files_processed(conn, table_name: str, file_names: List[str]):
    for fn in file_names:
        conn.execute("INSERT OR REPLACE INTO parquet_file_log (table_name, file_name) VALUES (?, ?)", [table_name, fn])


def _read_parquet_files_to_staging(conn, parquet_files: List[Path], staging_table: str) -> None:
    # Create or replace staging table from the first file, then insert subsequent files
    if not parquet_files:
        return
    first = parquet_files[0]
    conn.execute(f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM read_parquet('{first}');")
    for p in parquet_files[1:]:
        conn.execute(f"INSERT INTO {staging_table} SELECT * FROM read_parquet('{p}');")


def _merge_staging_into_table(conn, table: str, staging_table: str, pk_cols: List[str], order_by: Optional[str] = None) -> None:
    # If target table doesn't exist, simply rename staging to table
    # Otherwise, create a merged table deduplicating by PK (prefer ORDER BY when provided)
    # Build partition expression
    pk_expr = ', '.join(pk_cols)
    order_clause = f"ORDER BY {order_by}" if order_by else ""

    # If table does not exist, create it from staging
    table_exists = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;", [table]).fetchall()
    # DuckDB doesn't have sqlite_master; use a robust check
    try:
        # Try a quick SELECT to determine existence
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1;")
        exists = True
    except Exception:
        exists = False

    if not exists:
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {staging_table};")
        return

    # Merge by creating a temp merged table; prefer staged rows when ordering suggests
    # Use UNION ALL of existing table and staging, then ROW_NUMBER OVER partition
    # Using order by when provided to choose which row wins
    order_sql = f"{order_by}," if order_by else ""
    # If order_by is present we will use it; else use a constant ordering
    order_by_sql = order_by if order_by else 'ROWID'

    # Build SQL
    merge_sql = f"""
        CREATE OR REPLACE TABLE {table}_merged AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {pk_expr} ORDER BY {order_by_sql}) as rn
            FROM (
                SELECT * FROM {table}
                UNION ALL
                SELECT * FROM {staging_table}
            )
        ) WHERE rn = 1;
    """
    conn.execute(merge_sql)
    # Replace original table
    conn.execute(f"DROP TABLE IF EXISTS {table};")
    conn.execute(f"ALTER TABLE {table}_merged RENAME TO {table};")


def main(config: Optional[AppConfig] = None, dry_run: bool = False, create_checkpoint: bool = True) -> int:
    # Allow tests to pass a pre-built AppConfig to avoid reading project .env
    if config is None:
        try:
            config = AppConfig(calling_script_path=Path(__file__))
        except SystemExit as e:
            print(f"Configuration failed: {e}")
            sys.exit(1)

    logger = setup_logging(Path(__file__).stem, Path(__file__).parent / 'logs')

    parquet_root = config.PARQUET_DIR
    if not parquet_root.exists():
        logger.info(f"Parquet directory {parquet_root} does not exist. Nothing to do.")
        return 0

    # Prepare pragmas for write-heavy operations
    pragmas = {'threads':  max(1, 1)}
    if config.DUCKDB_TEMP_DIR:
        pragmas['temp_directory'] = str(config.DUCKDB_TEMP_DIR)

    # Optionally create a checkpoint copy of the DB file before making changes
    db_path_str = getattr(config, 'DB_FILE_STR', None)
    if not dry_run and create_checkpoint and db_path_str and db_path_str != ':memory:':
        db_path_obj = Path(db_path_str)
        if db_path_obj.is_file():
            try:
                ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                checkpoint_path = db_path_obj.with_name(f"{db_path_obj.stem}.checkpoint_{ts}{db_path_obj.suffix}")
                shutil.copy2(db_path_obj, checkpoint_path)
                logger.info(f"Created DB checkpoint copy: {checkpoint_path}")
            except Exception as e:
                logger.warning(f"Failed to create DB checkpoint copy: {e}")

    with ManagedDatabaseConnection(db_path_override=str(config.DB_FILE_STR), read_only=False, pragma_settings=pragmas) as conn:
        if conn is None:
            logger.error("Failed to connect to DB")
            return 2

        _ensure_parquet_log_table(conn)

        for table_name, pk_cols in TABLE_PK_MAP.items():
            table_dir = parquet_root / table_name
            if not table_dir.is_dir():
                logger.debug(f"No parquet directory for {table_name}; skipping.")
                continue

            all_files = sorted([p for p in table_dir.glob('*.parquet') if p.is_file()])
            if not all_files:
                logger.debug(f"No parquet files found for {table_name}.")
                continue

            processed = set(_get_already_processed_files(conn, table_name))
            to_process = [p for p in all_files if p.name not in processed]
            if not to_process:
                logger.info(f"No new parquet files for {table_name}.")
                continue

            logger.info(f"Processing {len(to_process)} new parquet file(s) for table {table_name}...")
            staging_table = f"staging_{table_name}"

            try:
                # Dry-run: only report files that would be processed
                if dry_run:
                    logger.info(f"DRY RUN: would process files for {table_name}: {[p.name for p in to_process]}")
                else:
                    _read_parquet_files_to_staging(conn, to_process, staging_table)
                    order_by = TABLE_ORDER_MAP.get(table_name)
                    _merge_staging_into_table(conn, table_name, staging_table, pk_cols, order_by=order_by)
                    # Mark files processed
                    _mark_files_processed(conn, table_name, [p.name for p in to_process])
                    # Drop staging
                    conn.execute(f"DROP TABLE IF EXISTS {staging_table};")
            except Exception as e:
                logger.error(f"Error processing parquet files for {table_name}: {e}", exc_info=True)
                # attempt to cleanup staging
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {staging_table};")
                except Exception:
                    pass
                continue

    logger.info("Incremental load completed.")
    return 0


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Incremental Parquet → DuckDB loader')
    parser.add_argument('--dry-run', action='store_true', help='List parquet files that would be processed without writing to DB')
    parser.add_argument('--no-checkpoint', action='store_true', help='Do not create a DB checkpoint copy before running')
    args = parser.parse_args()

    rc = main(dry_run=args.dry_run, create_checkpoint=not args.no_checkpoint)
    raise SystemExit(rc)
