#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stage Stock Fetch Plan

Generates a table `stock_fetch_plan` using the previously created
`prioritized_tickers_stock_backlog` table. It selects tickers needing
attention (no history, stale >7 days, or incomplete <365 records) and
computes fetch start/end dates and a reason classification.

Logic:
  status classification (SQL):
    - no_history   : last_date IS NULL
    - stale        : date_diff('day', last_date, CURRENT_DATE) > :stale_days
    - incomplete   : record_count < :min_records
    - fresh        : else

  start_date:
    CASE
      WHEN last_date IS NULL THEN DATE '1990-01-01'
      ELSE last_date + INTERVAL 1 DAY
    END

  end_date: CURRENT_DATE (can be overridden via --end-date)

Usage:
  python scripts/stage_stock_fetch_plan.py \
      --limit 400 \
      --stale-days 7 \
      --min-records 365 \
      --end-date 2025-11-18 \
      --db-path /path/to/edgar_analytics.duckdb

Will overwrite `stock_fetch_plan` table.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from datetime import date, datetime
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / 'utils'))

from utils.config_utils import AppConfig  # type: ignore
from utils.logging_utils import setup_logging  # type: ignore

SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = PROJECT_ROOT / 'logs'
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)


def parse_date(s: str | None) -> date:
    if not s:
        return date.today()
    return datetime.strptime(s, '%Y-%m-%d').date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage stock fetch plan from prioritized backlog table.")
    parser.add_argument('--limit', type=int, default=400, help='Maximum number of tickers to stage.')
    parser.add_argument('--stale-days', type=int, default=7, help='Staleness threshold in days.')
    parser.add_argument('--min-records', type=int, default=365, help='Minimum record count considered complete.')
    parser.add_argument('--end-date', type=str, default=None, help='Override end date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--db-path', type=str, default=None, help='Override DuckDB path (default from config).')
    args = parser.parse_args()

    try:
        config = AppConfig(calling_script_path=Path(__file__))
        db_path = args.db_path or config.DB_FILE_STR
    except SystemExit:
        if not args.db_path:
            logger.critical('Config load failed and no --db-path provided.')
            sys.exit(1)
        db_path = args.db_path

    end_date = parse_date(args.end_date)
    logger.info(f"DB: {db_path}")
    logger.info(f"Limit: {args.limit} stale_days: {args.stale_days} min_records: {args.min_records} end_date: {end_date}")

    with duckdb.connect(db_path, read_only=False) as con:
        # Ensure source table exists
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if 'prioritized_tickers_stock_backlog' not in tables:
            logger.error('Source table prioritized_tickers_stock_backlog not found. Run generate_prioritized_backlog first.')
            sys.exit(2)

        # Prepare aggregated stock history view for more current snapshot
        con.execute("CREATE OR REPLACE TEMP VIEW stock_hist AS SELECT ticker, MAX(date) AS last_date, COUNT(*) AS record_count FROM stock_history GROUP BY ticker")

        # Parameterize values via Python (DuckDB substitution is simpler inline f-string for clarity)
        stale_days = args.stale_days
        min_records = args.min_records
        end_date_sql = end_date.strftime('%Y-%m-%d')

        query = f"""
WITH base AS (
    SELECT p.ticker, p.rank, p.score, p.cik,
           p.unique_tag_count, p.key_metric_count,
           h.last_date, h.record_count,
           p.staleness_days AS backlog_staleness_days
    FROM prioritized_tickers_stock_backlog p
    LEFT JOIN stock_hist h USING(ticker)
), classified AS (
    SELECT *,
        CASE
          WHEN last_date IS NULL THEN 'no_history'
          WHEN date_diff('day', last_date, CURRENT_DATE) > {stale_days} THEN 'stale'
          WHEN COALESCE(record_count,0) < {min_records} THEN 'incomplete'
          ELSE 'fresh'
        END AS status,
        CASE
          WHEN last_date IS NULL THEN DATE '1990-01-01'
          ELSE last_date + INTERVAL 1 DAY
        END AS start_date,
        DATE '{end_date_sql}' AS end_date
    FROM base
), filtered AS (
    SELECT * FROM classified WHERE status IN ('no_history','stale','incomplete')
    ORDER BY rank
    LIMIT {args.limit}
)
SELECT *, CURRENT_TIMESTAMP AS generated_at
FROM filtered
ORDER BY rank;
"""

        logger.info('Executing fetch plan creation query...')
        plan_df = con.execute(query).df()
        logger.info(f"Plan rows prepared: {len(plan_df)}")

        con.execute("DROP TABLE IF EXISTS stock_fetch_plan")
        con.register('plan_df', plan_df)
        con.execute("CREATE TABLE stock_fetch_plan AS SELECT * FROM plan_df")

        # Summary counts
        summary = con.execute("SELECT status, COUNT(*) FROM stock_fetch_plan GROUP BY status ORDER BY COUNT(*) DESC").fetchall()
        for status, cnt in summary:
            logger.info(f"{status}: {cnt}")

        sample = con.execute("SELECT ticker, status, start_date, end_date, record_count, last_date, score FROM stock_fetch_plan ORDER BY rank LIMIT 15").fetchall()
        logger.info('Sample top 15:')
        for r in sample:
            logger.info(r)

    logger.info('Stock fetch plan staging complete.')


if __name__ == '__main__':
    main()
