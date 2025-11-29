#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility: Aggregate (stock history) Coverage Summary

Prints counts and percentages for:
 - Total tickers in `tickers` table
 - How many have `stock_history` rows (collected)
 - How many are marked untrackable (errors)
 - How many remain to fetch
 - Average / median time period collected per ticker (days and years)

Usage:
    python scripts/agg_coverage.py [--expiry-days N] [--lookback-years Y]

The script uses the project's `AppConfig` to locate the DuckDB file.
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path
from typing import Optional, List
from statistics import mean, median
from datetime import date, datetime

# Make project imports work when invoked from `scripts/`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection


SCRIPT_NAME = Path(__file__).stem
logger = setup_logging(SCRIPT_NAME, Path(__file__).resolve().parent / "logs")


def query_basic_counts(con, expiry_days: int = 365) -> dict:
    total_row = con.execute("SELECT COUNT(DISTINCT ticker) FROM tickers WHERE ticker IS NOT NULL;").fetchone()
    total = total_row[0] if total_row else 0

    # Collected: tickers that have at least one stock_history row (check normalized form)
    collected_q = (
        "SELECT COUNT(DISTINCT t.ticker) FROM tickers t "
        "WHERE t.ticker IS NOT NULL AND EXISTS ("
        "  SELECT 1 FROM stock_history s "
        "  WHERE s.ticker = t.ticker OR s.ticker = REPLACE(t.ticker, '-', '.')"
        ");"
    )
    try:
        collected_row = con.execute(collected_q).fetchone()
        collected = collected_row[0] if collected_row else 0
    except Exception:
        collected = 0

    # Untrackable counts
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

    # Untrackable not covered
    try:
        q_un_not_cov = (
            "SELECT COUNT(DISTINCT t.ticker) FROM tickers t "
            "JOIN polygon_untrackable_tickers p ON LOWER(p.ticker)=LOWER(t.ticker) "
            "WHERE p.last_failed_timestamp >= (now() - INTERVAL '{d} days') "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM stock_history s WHERE s.ticker = t.ticker OR s.ticker = REPLACE(t.ticker, '-', '.')"
            ")"
        ).format(d=expiry_days)
        un_not_cov_row = con.execute(q_un_not_cov).fetchone()
        untrackable_not_covered = un_not_cov_row[0] if un_not_cov_row else 0
    except Exception:
        untrackable_not_covered = untrackable

    remaining = max(0, total - collected - untrackable_not_covered)

    return {
        'total': total,
        'collected': collected,
        'untrackable': untrackable,
        'untrackable_not_covered': untrackable_not_covered,
        'remaining': remaining,
    }


def compute_time_spans(con) -> List[int]:
    """Return list of span lengths in days for tickers that have any stock_history rows.

    Uses a LEFT JOIN from `tickers` to `stock_history` with normalization to account
    for Polygon's ticker normalization (hyphen -> period).
    """
    q = (
        "SELECT t.ticker, MIN(s.date) AS min_date, MAX(s.date) AS max_date "
        "FROM tickers t "
        "LEFT JOIN stock_history s ON s.ticker = t.ticker OR s.ticker = REPLACE(t.ticker, '-', '.') "
        "GROUP BY t.ticker"
    )

    rows = con.execute(q).fetchall()
    spans: List[int] = []
    for ticker, min_d, max_d in rows:
        if min_d is None or max_d is None:
            continue
        # Convert to date
        def to_date_obj(v):
            if isinstance(v, date):
                return v
            if isinstance(v, datetime):
                return v.date()
            try:
                return datetime.fromisoformat(str(v)).date()
            except Exception:
                try:
                    import pandas as pd
                    return pd.to_datetime(v).date()
                except Exception:
                    return None

        dmin = to_date_obj(min_d)
        dmax = to_date_obj(max_d)
        if not dmin or not dmax:
            continue
        span_days = (dmax - dmin).days + 1
        if span_days >= 0:
            spans.append(span_days)

    return spans


def fmt_pct(part: int, whole: int) -> str:
    if whole <= 0:
        return '0.0%'
    return f"{part / whole * 100:.1f}%"


def main(argv: Optional[list] = None):
    parser = argparse.ArgumentParser(description="Show aggregate (stock_history) coverage and average time period collected")
    parser.add_argument('--expiry-days', type=int, default=365, help='Expiry window (days) for untrackable tickers')
    parser.add_argument('--lookback-years', type=int, default=5, help='Reference lookback window in years for coverage percent calculations')
    args = parser.parse_args(argv)

    config = AppConfig()

    with ManagedDatabaseConnection(str(config.DB_FILE)) as con:
        if con is None:
            print("Failed to connect to database; ensure DB is reachable via AppConfig.DB_FILE")
            return

        stats = query_basic_counts(con, expiry_days=args.expiry_days)
        spans = compute_time_spans(con)

    total = stats['total']
    collected = stats['collected']
    untrackable = stats['untrackable']
    untrackable_not_covered = stats['untrackable_not_covered']
    remaining = stats['remaining']

    # Time span statistics
    avg_days = int(mean(spans)) if spans else 0
    med_days = int(median(spans)) if spans else 0
    avg_years = avg_days / 365.0
    med_years = med_days / 365.0

    # Coverage relative to lookback window
    lookback_days = args.lookback_years * 365
    # Average coverage % among collected tickers
    if spans:
        avg_cov_pct_collected = mean(min(s / lookback_days, 1.0) for s in spans) * 100
        avg_cov_pct_all = (mean(spans) * collected / lookback_days) / max(1, total) * 100
    else:
        avg_cov_pct_collected = 0.0
        avg_cov_pct_all = 0.0

    print("Aggregate (stock_history) Coverage Summary")
    print("----------------------------------------")
    print(f"Total tickers:                {total}")
    print(f"Tickers with any history:     {collected}   ({fmt_pct(collected, total)})")
    print(f"Errors (untrackable):         {untrackable}   ({fmt_pct(untrackable, total)})")
    print(f" - Untrackable not covered:   {untrackable_not_covered}")
    print(f"Remaining tickers to fetch:   {remaining}   ({fmt_pct(remaining, total)})")
    print("")
    print("Time-period collected (per ticker with data):")
    print(f"  Average span: {avg_days} days ({avg_years:.2f} years)")
    print(f"  Median  span: {med_days} days ({med_years:.2f} years)")
    print(f"  Avg coverage vs {args.lookback_years}y window: {avg_cov_pct_collected:.1f}% (collected tickers)")
    print(f"  Avg coverage vs {args.lookback_years}y window: {avg_cov_pct_all:.1f}% (across all tickers)")


if __name__ == '__main__':
    main()
