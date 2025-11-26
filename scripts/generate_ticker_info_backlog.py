#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate Ticker Info Backlog Table

Creates/refreshes a DuckDB table `prioritized_tickers_info_backlog` ranking
tickers that need reference data fetched from Polygon.io/Massive.com.

Prioritizes tickers based on:
  - xbrl_richness: XBRL tag count (important companies have rich financial data)
  - key_metrics: Presence of core financial metrics
  - has_stock_data: Has trading history (active tickers worth detailed info)
  - filing_activity: Recent filing count
  - exchange_priority: Major exchange bonus (NASDAQ/NYSE)

Excludes:
  - Tickers already in updated_ticker_info (unless stale/force-refresh)
  - Tickers in polygon_untrackable_tickers (failed API calls)

Usage:
  python scripts/generate_ticker_info_backlog.py --limit 1000
  python scripts/generate_ticker_info_backlog.py --refresh-days 30 --limit 500
  python scripts/generate_ticker_info_backlog.py --force-refresh --limit 100

Optional --db-path overrides the DB from the loaded AppConfig.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import duckdb
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / 'utils'))

from utils.config_utils import AppConfig  # type: ignore
from utils.logging_utils import setup_logging  # type: ignore

SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = PROJECT_ROOT / 'logs'
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

DEFAULT_WEIGHTS: Dict[str, float] = {
    'xbrl_richness': 0.30,
    'key_metrics': 0.25,
    'has_stock_data': 0.20,
    'filing_activity': 0.15,
    'exchange_priority': 0.10,
}


def parse_weights(raw: str | None) -> Dict[str, float]:
    if not raw:
        return DEFAULT_WEIGHTS.copy()
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    out: Dict[str, float] = {}
    for part in parts:
        if '=' not in part:
            raise ValueError(f"Invalid weight spec '{part}'. Use key=value format.")
        k, v = part.split('=', 1)
        k = k.strip()
        v = v.strip()
        if k not in DEFAULT_WEIGHTS:
            raise ValueError(f"Unknown weight key '{k}'. Allowed: {list(DEFAULT_WEIGHTS)}")
        try:
            out[k] = float(v)
        except ValueError:
            raise ValueError(f"Weight value for '{k}' must be numeric, got '{v}'.")
    # Normalize
    total = sum(out.values())
    if total <= 0:
        raise ValueError("Weights must sum to > 0")
    out = {k: v / total for k, v in out.items()}
    # Fill any missing with normalized defaults
    for k in DEFAULT_WEIGHTS:
        if k not in out:
            out[k] = DEFAULT_WEIGHTS[k] / sum(DEFAULT_WEIGHTS.values())
    return out


def build_query(
    w: Dict[str, float],
    refresh_days: int,
    force_refresh: bool,
    exclude_untrackable: bool = True
) -> str:
    """Build the ticker-info prioritization query.
    
    Args:
        w: Weight dictionary for scoring components
        refresh_days: Re-fetch info if older than this many days
        force_refresh: If True, include all tickers regardless of existing info
        exclude_untrackable: If True, exclude polygon_untrackable_tickers
    """
    # Untrackable filter
    untrackable_cte = ""
    untrackable_join = ""
    untrackable_filter = ""
    
    if exclude_untrackable:
        untrackable_cte = """, polygon_untrackable AS (
    SELECT ticker
    FROM polygon_untrackable_tickers
    WHERE last_failed_timestamp >= (CURRENT_DATE - INTERVAL 365 DAY)
)"""
        untrackable_join = """
    LEFT JOIN polygon_untrackable pu ON pu.ticker = t.ticker"""
        untrackable_filter = """
      AND pu.ticker IS NULL  -- Exclude untrackable tickers"""
    
    # Existing info filter (unless force refresh)
    existing_info_join = ""
    existing_info_filter = ""
    
    if not force_refresh:
        refresh_cutoff = f"CURRENT_DATE - INTERVAL {refresh_days} DAY"
        existing_info_join = """
    LEFT JOIN updated_ticker_info uti ON uti.ticker = t.ticker OR uti.ticker = REPLACE(t.ticker, '-', '.')"""
        existing_info_filter = f"""
      AND (uti.ticker IS NULL OR uti.fetch_timestamp < {refresh_cutoff})  -- Missing or stale info"""
    
    # Major exchanges
    major_exchanges = "'XNAS', 'XNYS', 'NASDAQ', 'NYSE', 'ARCX', 'BATS'"
    
    return f"""
WITH fact_metrics AS (
    SELECT cik,
           COUNT(DISTINCT tag_name) AS unique_tag_count,
           MAX(CASE WHEN tag_name LIKE '%Assets%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%Liabilities%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%StockholdersEquity%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%Revenue%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%NetIncome%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%OperatingIncome%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%CashAndCashEquivalents%' THEN 1 ELSE 0 END) +
           MAX(CASE WHEN tag_name LIKE '%EarningsPerShare%' THEN 1 ELSE 0 END) AS key_metric_count
    FROM xbrl_facts
    GROUP BY cik
), stock_presence AS (
    SELECT ticker, 1 AS has_stock_data
    FROM stock_history
    GROUP BY ticker
), recent_filings AS (
    SELECT cik, COUNT(*) AS recent_filings
    FROM filings
    WHERE filing_date >= (CURRENT_DATE - INTERVAL 365 DAY)
    GROUP BY cik
), existing_info AS (
    SELECT ticker,
           primary_exchange,
           fetch_timestamp
    FROM updated_ticker_info
){untrackable_cte}, base AS (
    SELECT t.ticker, t.cik,
           fm.unique_tag_count, fm.key_metric_count,
           COALESCE(sp.has_stock_data, 0) AS has_stock_data,
           COALESCE(rf.recent_filings, 0) AS recent_filings,
           CASE 
               WHEN ei.primary_exchange IN ({major_exchanges}) THEN 1.0
               WHEN ei.primary_exchange IS NOT NULL THEN 0.3
               ELSE 0.0
           END AS exchange_score
    FROM tickers t
    JOIN fact_metrics fm USING(cik)
    LEFT JOIN stock_presence sp ON sp.ticker = t.ticker
    LEFT JOIN recent_filings rf USING(cik)
    LEFT JOIN existing_info ei ON ei.ticker = t.ticker OR ei.ticker = REPLACE(t.ticker, '-', '.'){untrackable_join}{existing_info_join}
    WHERE t.ticker IS NOT NULL{untrackable_filter}{existing_info_filter}
), normalized AS (
    SELECT *,
        unique_tag_count / NULLIF(MAX(unique_tag_count) OVER (),0) AS xbrl_norm,
        COALESCE(key_metric_count / NULLIF(MAX(key_metric_count) OVER (),0), 0) AS key_norm,
        COALESCE(has_stock_data / NULLIF(MAX(has_stock_data) OVER (),0), 0) AS stock_norm,
        COALESCE(recent_filings / NULLIF(MAX(recent_filings) OVER (),0), 0) AS filing_norm,
        COALESCE(exchange_score / NULLIF(MAX(exchange_score) OVER (),0), 0) AS exchange_norm
    FROM base
), final AS (
    SELECT *,
        ({w['xbrl_richness']:.6f} * xbrl_norm +
         {w['key_metrics']:.6f} * key_norm +
         {w['has_stock_data']:.6f} * stock_norm +
         {w['filing_activity']:.6f} * filing_norm +
         {w['exchange_priority']:.6f} * exchange_norm) AS score
    FROM normalized
)
SELECT ticker, cik, unique_tag_count, key_metric_count, has_stock_data,
       recent_filings, exchange_score, score,
       ROW_NUMBER() OVER (ORDER BY score DESC) AS rank,
       CURRENT_TIMESTAMP AS generated_at,
       '{json.dumps(w)}' AS weights_json
FROM final
ORDER BY score DESC;
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate prioritized ticker-info backlog table.")
    parser.add_argument('--limit', type=int, default=None, help='Row limit to persist (post ranking). Default: save all.')
    parser.add_argument('--weights', type=str, default=None, help='Comma list e.g. xbrl_richness=0.4,key_metrics=0.3,...')
    parser.add_argument('--refresh-days', type=int, default=30, help='Re-fetch info older than N days (default: 30)')
    parser.add_argument('--force-refresh', action='store_true', help='Include all tickers even if info is fresh')
    parser.add_argument('--db-path', type=str, default=None, help='Override DuckDB path (otherwise from AppConfig).')
    args = parser.parse_args()

    try:
        config = AppConfig(calling_script_path=Path(__file__))
        db_path = args.db_path or config.DB_FILE_STR
    except SystemExit:
        logger.critical("Failed to load AppConfig; provide --db-path manually.")
        if not args.db_path:
            sys.exit(1)
        db_path = args.db_path

    weights = parse_weights(args.weights)
    logger.info(f"Using DB: {db_path}")
    logger.info(f"Weights: {weights}")
    logger.info(f"Refresh window: {args.refresh_days} days")
    logger.info(f"Force refresh: {args.force_refresh}")
    logger.info(f"Row limit: {args.limit if args.limit else 'ALL'}")

    con = duckdb.connect(db_path, read_only=False)
    
    # Check if polygon_untrackable_tickers table exists
    tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
    has_untrackable = 'polygon_untrackable_tickers' in tables
    
    if has_untrackable:
        result = con.execute("SELECT COUNT(*) FROM polygon_untrackable_tickers WHERE last_failed_timestamp >= (CURRENT_DATE - INTERVAL 365 DAY)").fetchone()
        untrackable_count = result[0] if result else 0
        logger.info(f"Found {untrackable_count} untrackable tickers (365d window) to exclude")
    else:
        logger.info("polygon_untrackable_tickers table not found - will include all tickers")
    
    query = build_query(weights, args.refresh_days, args.force_refresh, exclude_untrackable=has_untrackable)
    logger.info("Executing ticker-info prioritization query...")
    df = con.execute(query).df()
    logger.info(f"Scored {len(df)} candidate tickers for info gathering.")

    # Apply limit and persist
    limited_df = df.head(args.limit).copy() if args.limit else df.copy()
    con.execute("DROP TABLE IF EXISTS prioritized_tickers_info_backlog")
    con.register("limited_df", limited_df)
    con.execute("CREATE TABLE prioritized_tickers_info_backlog AS SELECT * FROM limited_df")
    saved_row = con.execute("SELECT COUNT(*) FROM prioritized_tickers_info_backlog").fetchone()
    saved = saved_row[0] if saved_row else 0
    logger.info(f"Persisted {saved} rows to prioritized_tickers_info_backlog.")

    sample = con.execute("SELECT ticker, score, rank, unique_tag_count, key_metric_count, has_stock_data FROM prioritized_tickers_info_backlog ORDER BY rank LIMIT 15").fetchall()
    logger.info("Top 15 sample:")
    for r in sample:
        logger.info(r)

    con.close()
    logger.info("Ticker-info backlog generation complete.")


if __name__ == '__main__':
    main()
