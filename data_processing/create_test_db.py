"""Create and refresh a testing DuckDB by sampling from the production DB.

This script attaches the source DuckDB and copies sampled subsets of configured
tables into a separate test DuckDB. It stores a `test_db_meta` entry with the
`last_refresh` timestamp and will skip refreshing if the last refresh is
younger than `--days` (default 30) unless `--force` is passed.

Usage:
  export DB_FILE=/path/to/edgar_analytics.duckdb
  export TEST_DB_FILE=/path/to/edgar_analytics_test.duckdb
  python data_processing/create_test_db.py --force

Suggested scheduling (run monthly via cron):
  0 3 1 * * DB_FILE=/... TEST_DB_FILE=/... /path/to/venv/bin/python /path/to/repo/data_processing/create_test_db.py
"""
from __future__ import annotations
import argparse
import datetime
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb


DEFAULT_TABLE_SAMPLE_SIZES: Dict[str, int] = {
    "xbrl_facts": 10000,
    "xbrl_tags": 2000,
    "updated_ticker_info": 5000,
    "tickers": 2000,
    # add other tables here with conservative defaults
}


def quote_path(p: str) -> str:
    return p.replace("'", "''")


def table_exists(con: duckdb.DuckDBPyConnection, table: str, schema: str = 'src') -> bool:
    # First try a direct qualified select against the attached schema (works
    # even if information_schema doesn't list the attached schema as 'src').
    try:
        con.execute(f"SELECT 1 FROM {schema}.\"{table}\" LIMIT 1;")
        return True
    except Exception:
        # Fallback: check information_schema for the table in any schema
        q = "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1;"
        r = con.execute(q, [table]).fetchone()
        return bool(r)


def find_date_column(con: duckdb.DuckDBPyConnection, table: str, schema: str = 'src') -> Optional[str]:
    q = "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND table_schema = ?;"
    cols = [r[0] for r in con.execute(q, [table, schema]).fetchall()]
    candidates = [c for c in cols if any(k in c.lower() for k in ('date', 'time', 'accepted', 'filing'))]
    return candidates[0] if candidates else None


def copy_table_sample(con: duckdb.DuckDBPyConnection, table: str, limit: int, schema: str = 'src') -> None:
    date_col = find_date_column(con, table, schema)
    src_quoted = f"{schema}.\"{table}\""
    if date_col:
        sel = f"SELECT * FROM {src_quoted} ORDER BY \"{date_col}\" DESC LIMIT {limit}"
    else:
        sel = f"SELECT * FROM {src_quoted} ORDER BY RANDOM() LIMIT {limit}"
    # Replace existing table in destination (test DB)
    con.execute(f"DROP TABLE IF EXISTS \"{table}\";")
    con.execute(f"CREATE TABLE \"{table}\" AS {sel};")


def ensure_meta_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS test_db_meta (key VARCHAR PRIMARY KEY, value VARCHAR);")


def get_last_refresh(con: duckdb.DuckDBPyConnection) -> Optional[datetime.datetime]:
    ensure_meta_table(con)
    r = con.execute("SELECT value FROM test_db_meta WHERE key='last_refresh' LIMIT 1;").fetchone()
    if not r:
        return None
    try:
        return datetime.datetime.fromisoformat(r[0])
    except Exception:
        return None


def set_last_refresh(con: duckdb.DuckDBPyConnection, when: datetime.datetime) -> None:
    ensure_meta_table(con)
    iso = when.isoformat()
    con.execute("DELETE FROM test_db_meta WHERE key='last_refresh';")
    con.execute("INSERT INTO test_db_meta VALUES ('last_refresh', ?);", [iso])


def create_or_refresh_test_db(src_db: str, test_db: str, tables: Optional[Iterable[str]] = None, sample_sizes: Optional[Dict[str, int]] = None, days: int = 30, force: bool = False) -> None:
    src_db_q = quote_path(src_db)
    # operate on destination connection, attach source as `src`
    dst = duckdb.connect(database=test_db, read_only=False)
    try:
        # Attach source db
        dst.execute(f"ATTACH '{src_db_q}' AS src;")

        last = get_last_refresh(dst)
        now = datetime.datetime.utcnow()
        if not force and last and (now - last).days < days:
            print(f"Test DB recently refreshed ({last.isoformat()}); skipping (use --force to override).")
            return

        all_tables = list(tables) if tables else list(DEFAULT_TABLE_SAMPLE_SIZES.keys())
        for t in all_tables:
            if not table_exists(dst, t, schema='src'):
                print(f"  - source table '{t}' does not exist in source DB; skipping")
                continue
            limit = (sample_sizes or {}).get(t, DEFAULT_TABLE_SAMPLE_SIZES.get(t, 2000))
            print(f"Copying sample for table {t} (limit={limit})...")
            try:
                copy_table_sample(dst, t, limit, schema='src')
            except Exception as e:
                print(f"  Error copying table {t}: {e}")
        # record refresh
        set_last_refresh(dst, now)
        print("Test DB refresh complete.")
    finally:
        try:
            dst.execute("DETACH src;")
        except Exception:
            pass
        dst.close()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create/refresh a sampled test DuckDB from production DB")
    p.add_argument("--db-file", default=os.environ.get('DB_FILE'), help="Path to source DuckDB (or set DB_FILE env)")
    p.add_argument("--test-db", default=os.environ.get('TEST_DB_FILE', os.environ.get('DB_FILE', '') + '_test.duckdb'), help="Path to test DuckDB (or set TEST_DB_FILE env)")
    p.add_argument("--tables", default=None, help="Comma-separated list of tables to copy (default: configured list)")
    p.add_argument("--sample-size", type=int, default=None, help="Default sample size to use when a table-specific size isn't configured")
    p.add_argument("--days", type=int, default=30, help="Minimum age in days before auto-refreshing")
    p.add_argument("--force", action='store_true', help="Force refresh regardless of last_refresh timestamp")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if not args.db_file:
        print("Missing source DB file. Set --db-file or DB_FILE environment variable.")
        return 2
    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(',') if t.strip()]
    sample_sizes = None
    if args.sample_size:
        # apply uniform sample size
        sample_sizes = {t: args.sample_size for t in (tables or DEFAULT_TABLE_SAMPLE_SIZES.keys())}

    create_or_refresh_test_db(args.db_file, args.test_db, tables=tables, sample_sizes=sample_sizes, days=args.days, force=args.force)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
