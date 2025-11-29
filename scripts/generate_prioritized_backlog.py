#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate Backlog-Focused Prioritized Ticker Table

Creates/refreshes a DuckDB table `prioritized_tickers_stock_backlog` ranking
tickers that both (a) have XBRL fact coverage and (b) still need stock data
work (missing, stale, or incomplete history). Scoring is performed fully in
SQL for performance and to avoid Python date type issues.

Score components (normalized across the working set):
  - xbrl_richness      : distinct XBRL tag count (data quality proxy)
  - key_metrics        : presence count of core financial metrics
  - stock_data_need    : heuristic need score (no data > stale > incomplete)
  - filing_activity    : recent filings (last 365 days)

Stock data need heuristic (computed in SQL):
  CASE
    WHEN last_date IS NULL THEN 1000                      -- no data at all
    WHEN date_diff('day', last_date, CURRENT_DATE) > 7 THEN 500 + staleness
    WHEN record_count < 365 THEN 250 + (365 - record_count)
    ELSE GREATEST(0, staleness)
  END

Usage:
  python scripts/generate_prioritized_backlog.py \
      --limit 1200 \
      --weights xbrl_richness=0.25,key_metrics=0.15,stock_data_need=0.45,filing_activity=0.15

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
    'xbrl_richness': 0.25,
    'key_metrics': 0.15,
    'stock_data_need': 0.45,
    'filing_activity': 0.15,
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
        k = k.strip(); v = v.strip()
        if k not in DEFAULT_WEIGHTS:
            raise ValueError(f"Unknown weight key '{k}'. Allowed: {list(DEFAULT_WEIGHTS)}")
        try:
            out[k] = float(v)
        except ValueError:
            raise ValueError(f"Weight value for '{k}' must be numeric, got '{v}'.")
    # Normalize defensively
    total = sum(out.values())
    if total <= 0:
        raise ValueError("Weights must sum to > 0")
    out = {k: v / total for k, v in out.items()}
    # Fill any missing with normalized defaults proportionally (rare case)
    for k in DEFAULT_WEIGHTS:
        if k not in out:
            out[k] = DEFAULT_WEIGHTS[k] / sum(DEFAULT_WEIGHTS.values())
    return out


def build_query(w: Dict[str, float], exclude_untrackable: bool = True) -> str:
    """Build the prioritization query.
    
    Args:
        w: Weight dictionary for scoring components
        exclude_untrackable: If True and polygon_untrackable_tickers exists, exclude those tickers
    """
    # Conditionally add untrackable CTE and filter
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
    
    # All SQL uses window functions for normalization; weights are injected as literals.
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
), stock_hist AS (
    SELECT ticker, MAX(date) AS last_date, COUNT(*) AS record_count
    FROM stock_history
    GROUP BY ticker
), recent_filings AS (
    SELECT cik, COUNT(*) AS recent_filings
    FROM filings
    WHERE filing_date >= (CURRENT_DATE - INTERVAL 365 DAY)
    GROUP BY cik
){untrackable_cte}, base AS (
    SELECT t.ticker, t.cik,
           fm.unique_tag_count, fm.key_metric_count,
           sh.last_date, sh.record_count,
           COALESCE(rf.recent_filings, 0) AS recent_filings
    FROM updated_ticker_info t
    JOIN fact_metrics fm USING(cik)
    LEFT JOIN stock_hist sh ON sh.ticker = t.ticker
    LEFT JOIN recent_filings rf USING(cik){untrackable_join}
    WHERE t.ticker IS NOT NULL{untrackable_filter}
), scored AS (
    SELECT *,
        CASE
          WHEN last_date IS NULL THEN 1000
          WHEN date_diff('day', last_date, CURRENT_DATE) > 7 THEN 500 + date_diff('day', last_date, CURRENT_DATE)
          WHEN COALESCE(record_count,0) < 365 THEN 250 + (365 - COALESCE(record_count,0))
          ELSE GREATEST(0, date_diff('day', last_date, CURRENT_DATE))
        END AS stock_need_score,
        date_diff('day', last_date, CURRENT_DATE) AS staleness_days
    FROM base
), normalized AS (
    SELECT *,
        unique_tag_count / NULLIF(MAX(unique_tag_count) OVER (),0) AS xbrl_norm,
        COALESCE(key_metric_count / NULLIF(MAX(key_metric_count) OVER (),0), 0) AS key_norm,
        stock_need_score / NULLIF(MAX(stock_need_score) OVER (),0) AS need_norm,
        COALESCE(recent_filings / NULLIF(MAX(recent_filings) OVER (),0), 0) AS filing_norm
    FROM scored
), final AS (
    SELECT *,
        ({w['xbrl_richness']:.6f} * xbrl_norm +
         {w['key_metrics']:.6f} * key_norm +
         {w['stock_data_need']:.6f} * need_norm +
         {w['filing_activity']:.6f} * filing_norm) AS score
    FROM normalized
    WHERE stock_need_score > 0
)
SELECT ticker, cik, unique_tag_count, key_metric_count, last_date, record_count,
       recent_filings, stock_need_score, staleness_days, score,
       ROW_NUMBER() OVER (ORDER BY score DESC) AS rank,
       CURRENT_TIMESTAMP AS generated_at,
       '{json.dumps(w)}' AS weights_json,
       -- Add suggested date ranges for data fetching
       CASE 
           WHEN last_date IS NULL THEN CURRENT_DATE - INTERVAL 5 YEAR
           WHEN date_diff('day', last_date, CURRENT_DATE) > 7 THEN last_date + INTERVAL 1 DAY
           ELSE CURRENT_DATE - INTERVAL 5 YEAR
       END AS start_date,
       CURRENT_DATE - INTERVAL 1 DAY AS end_date
FROM final
ORDER BY score DESC;
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate prioritized backlog table for stock data gathering.")
    parser.add_argument('--limit', type=int, default=None, help='Row limit to persist (post ranking). Default: save all tickers.')
    parser.add_argument('--weights', type=str, default=None, help='Comma list of weight assignments e.g. xbrl_richness=0.3,key_metrics=0.2,stock_data_need=0.35,filing_activity=0.15')
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
    logger.info(f"Row limit: {args.limit if args.limit else 'ALL'}")

    with duckdb.connect(db_path, read_only=False) as con:
        # Check if polygon_untrackable_tickers table exists
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        has_untrackable = 'polygon_untrackable_tickers' in tables
        
        if has_untrackable:
            result = con.execute("SELECT COUNT(*) FROM polygon_untrackable_tickers WHERE last_failed_timestamp >= (CURRENT_DATE - INTERVAL 365 DAY)").fetchone()
            untrackable_count = result[0] if result else 0
            logger.info(f"Found {untrackable_count} untrackable tickers (365d window) to exclude from prioritization")
        else:
            logger.info("polygon_untrackable_tickers table not found - will include all tickers")
        
        query = build_query(weights, exclude_untrackable=has_untrackable)
        logger.info("Executing scoring query (vectorized)...")
        df = con.execute(query).df()
        logger.info(f"Scored {len(df)} candidate backlog tickers.")

        # Apply limit and persist
        limited_df = df.head(args.limit).copy() if args.limit else df.copy()
        con.execute("DROP TABLE IF EXISTS prioritized_tickers_stock_backlog")
        con.register("limited_df", limited_df)
        con.execute("CREATE TABLE prioritized_tickers_stock_backlog AS SELECT * FROM limited_df")
        saved_row = con.execute("SELECT COUNT(*) FROM prioritized_tickers_stock_backlog").fetchone()
        saved = saved_row[0] if saved_row else 0
        logger.info(f"Persisted {saved} rows to prioritized_tickers_stock_backlog.")

        sample = con.execute("SELECT ticker, score, rank, staleness_days, record_count, unique_tag_count, key_metric_count FROM prioritized_tickers_stock_backlog ORDER BY rank LIMIT 15").fetchall()
        logger.info("Top 15 sample:")
        for r in sample:
            logger.info(r)

    logger.info("Backlog prioritization complete.")


if __name__ == '__main__':
    main()
