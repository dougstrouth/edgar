#!/usr/bin/env python3
"""Local runner to regenerate data dictionary, refresh test DB, populate AI tables, and run tests.

Usage examples:
  # Use DB_FILE/TEST_DB_FILE from environment (e.g., your project .env)
  python scripts/run_local_schema_tasks.py

  # Or specify paths explicitly
  python scripts/run_local_schema_tasks.py --db /path/to/edgar_analytics.duckdb --test-db /path/to/edgar_analytics_test.duckdb --force

This script is intended for local invocation (developer machine). It does NOT
modify CI configuration.
"""
from __future__ import annotations
import argparse
import datetime
import os
import subprocess
import sys
from typing import Optional

import duckdb

ROOT = os.path.dirname(os.path.dirname(__file__))
SYSROOT = os.path.abspath(os.path.join(ROOT, '..'))

def generate_data_dictionary(db_file: str, out_path: str = 'DATA_DICTIONARY_GENERATED.md') -> None:
    con = duckdb.connect(database=db_file, read_only=True)
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY table_schema, table_name;").fetchall()
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    lines = []
    lines.append('<!-- Programmatically generated data dictionary -->')
    lines.append('# EDGAR Analytics - Data Dictionary (Generated)')
    lines.append('')
    lines.append('_This file was programmatically generated from the DuckDB schema._')
    lines.append('')
    lines.append(f'- Generated at: {now}')
    lines.append(f'- Source DB: `{db_file}`')
    lines.append('\n---\n')
    # list tables grouped by schema
    current_schema = None
    for schema_name, table in tables:
        if schema_name != current_schema:
            current_schema = schema_name
            lines.append(f'\n## Schema `{schema_name}`\n')
        lines.append(f"\n### `{table}`\n")
        lines.append('\n| Column Name | Data Type |')
        lines.append('| :--- | :--- |')
        cols = con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position;", (schema_name, table)).fetchall()
        for col_name, data_type in cols:
            lines.append(f"| `{col_name}` | `{data_type}` |")

    with open(out_path, 'w') as f:
        f.write('\n'.join(lines))
    print(f'Wrote {out_path}')


def run_pytest() -> int:
    print('Running pytest...')
    res = subprocess.run([sys.executable, '-m', 'pytest', '-q'], cwd=ROOT)
    return res.returncode


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--db', help='Path to source DuckDB (defaults to DB_FILE env)')
    p.add_argument('--test-db', help='Path to test DuckDB (defaults to TEST_DB_FILE env)')
    p.add_argument('--no-tests', action='store_true', help='Skip running pytest')
    p.add_argument('--no-dict', action='store_true', help='Skip regenerating data dictionary')
    p.add_argument('--no-ai', action='store_true', help='Skip populating ai_unique_values in test DB')
    p.add_argument('--force', action='store_true', help='Force refresh of test DB')
    args = p.parse_args(argv)

    db_file = args.db or os.environ.get('DB_FILE')
    test_db = args.test_db or os.environ.get('TEST_DB_FILE')

    if not args.no_dict:
        if not db_file:
            print('No DB file provided (DB_FILE env or --db). Skipping data-dictionary generation.')
        else:
            generate_data_dictionary(db_file)

    # Populate ai_unique_values — prefer using an existing test DB, otherwise create one
    if not args.no_ai:
        if not test_db:
            print('No TEST_DB_FILE provided (env TEST_DB_FILE or --test-db). Skipping ai_unique_values population.')
        else:
            # import functions locally to avoid cross-module CLI invocation
            from data_processing.create_test_db import create_or_refresh_test_db
            from data_processing.unique_value_tables import create_ai_unique_value_table

            # Ensure test DB exists and is refreshed
            create_or_refresh_test_db(db_file or '', test_db, force=args.force)
            con = duckdb.connect(database=test_db, read_only=False)
            create_ai_unique_value_table(con)
            con.close()
            print('Populated ai_unique_values in test DB')

    if not args.no_tests:
        rc = run_pytest()
        return rc

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
