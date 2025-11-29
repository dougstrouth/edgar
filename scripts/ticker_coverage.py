#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility: Ticker Info Coverage Summary

Prints counts and percentages for:
 - Total tickers in `tickers` table
 - How many have `updated_ticker_info` (collected)
 - How many are marked untrackable (errors)
 - How many remain to fetch (neither collected nor untrackable)

Usage:
    python scripts/ticker_coverage.py [--expiry-days N]

The script uses the project's `AppConfig` to locate the DuckDB file.
"""

from __future__ import annotations

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

# Ensure project root is on sys.path so `utils` package is importable when
# running this script directly from `scripts/`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection


SCRIPT_NAME = Path(__file__).stem
logger = setup_logging(SCRIPT_NAME, Path(__file__).resolve().parent / "logs", level=logging.INFO)


def compute_coverage(con, expiry_days: int = 365) -> dict:
    """Compute counts for total, collected, untrackable, remaining.

    Strategy:
      - total: distinct tickers in `tickers` table
      - collected: count of distinct tickers from `tickers` that match
        entries in `updated_ticker_info`. To account for Polygon normalization
        (hyphen -> period), the join checks both exact and normalized form.
      - untrackable: tickers listed in `polygon_untrackable_tickers` within expiry window
      - remaining: total - covered - untrackable_not_covered
    """
    # Total distinct tickers
    total_row = con.execute("SELECT COUNT(DISTINCT ticker) FROM tickers WHERE ticker IS NOT NULL;").fetchone()
    total = total_row[0] if total_row else 0

    # Count of tickers considered covered by updated_ticker_info
    # Check for exact match OR normalized (replace '-' with '.') in updated_ticker_info
    covered_q = (
        "SELECT COUNT(DISTINCT t.ticker) FROM tickers t "
        "WHERE t.ticker IS NOT NULL AND EXISTS ("
        "  SELECT 1 FROM updated_ticker_info u "
        "  WHERE u.ticker = t.ticker OR u.ticker = REPLACE(t.ticker, '-', '.')"
        ");"
    )
    try:
        covered_row = con.execute(covered_q).fetchone()
        covered = covered_row[0] if covered_row else 0
    except Exception:
        # If updated_ticker_info doesn't exist, covered is zero
        covered = 0

    # Untrackable tickers within expiry window
    try:
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if 'polygon_untrackable_tickers' in tables:
            q_un = (
                "SELECT COUNT(DISTINCT t.ticker) FROM tickers t "
                "JOIN polygon_untrackable_tickers p ON LOWER(p.ticker)=LOWER(t.ticker) "
                "WHERE p.last_failed_timestamp >= (now() - INTERVAL '{d} days')"
            ).format(d=expiry_days)
            un_row = con.execute(q_un).fetchone()
            untrackable = un_row[0] if un_row else 0
        else:
            untrackable = 0
    except Exception:
        untrackable = 0

    # Some untrackable may already be covered (edge-case). Compute untrackable_not_covered
    try:
        q_un_not_cov = (
            "SELECT COUNT(DISTINCT t.ticker) FROM tickers t "
            "JOIN polygon_untrackable_tickers p ON LOWER(p.ticker)=LOWER(t.ticker) "
            "WHERE p.last_failed_timestamp >= (now() - INTERVAL '{d} days') "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM updated_ticker_info u WHERE u.ticker = t.ticker OR u.ticker = REPLACE(t.ticker, '-', '.')"
            ")"
        ).format(d=expiry_days)
        un_not_cov_row = con.execute(q_un_not_cov).fetchone()
        untrackable_not_covered = un_not_cov_row[0] if un_not_cov_row else 0
    except Exception:
        untrackable_not_covered = untrackable

    remaining = max(0, total - covered - untrackable_not_covered)

    return {
        'total': total,
        'covered': covered,
        'untrackable': untrackable,
        'untrackable_not_covered': untrackable_not_covered,
        'remaining': remaining,
    }


def fmt_pct(part: int, whole: int) -> str:
    if whole <= 0:
        return '0.0%'
    return f"{part / whole * 100:.1f}%"


def main(argv: Optional[list] = None):
    parser = argparse.ArgumentParser(description="Show ticker info coverage summary")
    parser.add_argument('--expiry-days', type=int, default=365, help='Expiry window (days) for untrackable tickers')
    args = parser.parse_args(argv)

    config = AppConfig()

    with ManagedDatabaseConnection(str(config.DB_FILE)) as con:
        if con is None:
            logger.critical("Failed to connect to database; ensure DB exists in AppConfig.DB_FILE")
            return

        stats = compute_coverage(con, expiry_days=args.expiry_days)

    total = stats['total']
    covered = stats['covered']
    untrackable = stats['untrackable']
    untrackable_not_covered = stats['untrackable_not_covered']
    remaining = stats['remaining']

    print("Ticker Info Coverage Summary")
    print("----------------------------------------")
    print(f"Total tickers:        {total}")
    print(f"Collected (covered):  {covered}   ({fmt_pct(covered, total)})")
    print(f"Errors (untrackable): {untrackable}   ({fmt_pct(untrackable, total)})")
    print(f" - Untrackable not yet covered: {untrackable_not_covered}")
    print(f"Remaining to fetch:   {remaining}   ({fmt_pct(remaining, total)})")


if __name__ == '__main__':
    main()
