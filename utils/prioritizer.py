# -*- coding: utf-8 -*-
"""
Hybrid prioritizer utilities for YFinance ticker scoring.

Provides a conservative, testable scoring function (option C) that
combines filings activity and company recency into a single score.
This module is intentionally small and dependency-free beyond DuckDB.
"""
from __future__ import annotations

from typing import List, Dict, Tuple, Optional
from datetime import datetime, date, timezone, timedelta
import duckdb
import math


def _safe_max(values: List[float]) -> float:
    return max(values) if values else 1.0


def prioritize_tickers_for_stock_data(
    db_path: str,
    tickers: List[str],
    weights: Optional[Dict[str, float]] = None,
    lookback_days: int = 365,
) -> List[Tuple[str, float]]:
    """
    Score and rank tickers for stock data gathering based on data quality and completeness.

    The scoring prioritizes tickers that:
      1. Have rich XBRL financial data (more unique tags)
      2. Have key financial metrics (Assets, Revenue, NetIncome, etc.)
      3. Are missing stock price data or have stale/incomplete data
      4. Have recent filing activity

    Weights:
      - xbrl_richness: How many unique XBRL tags (data quality indicator)
      - key_metrics: Presence of essential financial metrics
      - stock_data_need: Inverse of stock data completeness (no data = highest priority)
      - filing_activity: Recent filing activity

    Returns a list of tuples `(ticker, score)` sorted by score descending.
    """
    if weights is None:
        weights = {
            'xbrl_richness': 0.35,      # Rich financial data = worth gathering stock data
            'key_metrics': 0.25,         # Core metrics present = important company
            'stock_data_need': 0.25,     # Missing/stale stock data = higher priority
            'filing_activity': 0.15      # Active filers = more relevant
        }

    # Defensive normalization: ensure weights sum to 1
    total_w = sum(weights.values())
    if total_w <= 0:
        raise ValueError("weights must sum to > 0")
    weights = {k: v / total_w for k, v in weights.items()}

    scores: Dict[str, float] = {t: 0.0 for t in tickers}
    if not tickers:
        return []

    try:
        con = duckdb.connect(database=db_path, read_only=True)
    except Exception:
        return [(t, 0.0) for t in tickers]

    try:
        # Key financial metrics to check for
        key_metric_patterns = [
            'Assets', 'Liabilities', 'StockholdersEquity',
            'Revenue', 'NetIncome', 'OperatingIncome',
            'CashAndCashEquivalents', 'EarningsPerShare'
        ]

        # Collect metrics for each ticker
        xbrl_tag_counts = {}
        key_metrics_counts = {}
        stock_data_staleness = {}
        stock_data_record_counts = {}
        filing_counts = {}

        lookback_threshold = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()

        for t in tickers:
            try:
                row = con.execute("SELECT cik FROM tickers WHERE ticker = ? LIMIT 1;", [t]).fetchone()
                cik = row[0] if row else None
            except Exception:
                cik = None

            # Default values
            xbrl_tag_counts[t] = 0
            key_metrics_counts[t] = 0
            stock_data_staleness[t] = float('inf')  # Infinity means no data
            stock_data_record_counts[t] = 0
            filing_counts[t] = 0

            if cik:
                # XBRL richness: count unique tags for this CIK
                try:
                    r = con.execute(
                        "SELECT COUNT(DISTINCT tag_name) FROM xbrl_facts WHERE cik = ?;",
                        [cik]
                    ).fetchone()
                    xbrl_tag_counts[t] = int(r[0]) if r and r[0] else 0
                except Exception:
                    pass

                # Key metrics presence: check for essential financial tags
                try:
                    key_count = 0
                    for metric in key_metric_patterns:
                        r = con.execute(
                            "SELECT COUNT(*) FROM xbrl_facts WHERE cik = ? AND tag_name LIKE ? LIMIT 1;",
                            [cik, f'%{metric}%']
                        ).fetchone()
                        if r and r[0] > 0:
                            key_count += 1
                    key_metrics_counts[t] = key_count
                except Exception:
                    pass

                # Filing activity: recent filings count
                try:
                    r = con.execute(
                        "SELECT COUNT(*) FROM filings WHERE cik = ? AND filing_date >= ?;",
                        [cik, lookback_threshold]
                    ).fetchone()
                    filing_counts[t] = int(r[0]) if r else 0
                except Exception:
                    pass

            # Stock data completeness: check staleness and record count
            try:
                r = con.execute(
                    "SELECT MAX(date), COUNT(*) FROM stock_history WHERE ticker = ?;",
                    [t]
                ).fetchone()
                if r and r[0]:
                    last_date = r[0]
                    if isinstance(last_date, str):
                        last_date = datetime.fromisoformat(last_date).date()
                    elif isinstance(last_date, datetime):
                        last_date = last_date.date()
                    stock_data_staleness[t] = (datetime.now(timezone.utc).date() - last_date).days
                    stock_data_record_counts[t] = int(r[1]) if r[1] else 0
            except Exception:
                pass

        # Normalize metrics
        xbrl_vals = [float(v) for v in xbrl_tag_counts.values()]
        key_vals = [float(v) for v in key_metrics_counts.values()]
        filing_vals = [float(v) for v in filing_counts.values()]
        
        # For stock data need: higher staleness or no data = higher score
        # Also consider record count (fewer records = needs more data)
        stock_need_vals = []
        for t in tickers:
            staleness = stock_data_staleness[t]
            record_count = stock_data_record_counts[t]
            
            if math.isinf(staleness):
                # No data at all = maximum priority
                need_score = 1000.0
            elif staleness > 7:
                # Stale data (>1 week old) = high priority, scaled by staleness
                need_score = 500.0 + staleness
            elif record_count < 365:
                # Has some recent data but incomplete (less than 1 year) = medium priority
                need_score = 250.0 + (365 - record_count)
            else:
                # Has complete recent data = low priority
                need_score = max(0, staleness)
            
            stock_need_vals.append(need_score)

        max_xbrl = _safe_max(xbrl_vals)
        max_key = _safe_max(key_vals)
        max_filing = _safe_max(filing_vals)
        max_need = _safe_max(stock_need_vals)

        for i, t in enumerate(tickers):
            xbrl_norm = (xbrl_tag_counts[t] / max_xbrl) if max_xbrl else 0.0
            key_norm = (key_metrics_counts[t] / max_key) if max_key else 0.0
            filing_norm = (filing_counts[t] / max_filing) if max_filing else 0.0
            need_norm = (stock_need_vals[i] / max_need) if max_need else 0.0

            score = (
                weights['xbrl_richness'] * xbrl_norm +
                weights['key_metrics'] * key_norm +
                weights['stock_data_need'] * need_norm +
                weights['filing_activity'] * filing_norm
            )
            scores[t] = float(score)

        # Build sorted list
        sorted_list = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return sorted_list
    finally:
        try:
            con.close()
        except Exception:
            pass


def prioritize_tickers_for_info(
    db_path: str,
    tickers: List[str],
    weights: Optional[Dict[str, float]] = None,
    lookback_days: int = 365,
) -> List[Tuple[str, float]]:
    """
    Score and rank tickers for ticker-info gathering from Polygon.
    
    Prioritizes tickers that:
      1. Have rich XBRL data (many unique tags) - worth getting reference data
      2. Have key financial metrics (important companies)
      3. Have stock price data (trading actively, worth detailed info)
      4. Have recent filing activity (active/important companies)
      5. Are on major exchanges (US stocks, NASDAQ/NYSE)
    
    Weights:
      - xbrl_richness: Unique XBRL tag count (data quality proxy)
      - key_metrics: Presence of core financial metrics
      - has_stock_data: Has any stock price history (trading activity)
      - filing_activity: Recent filing count
      - exchange_priority: Major exchange bonus (NASDAQ/NYSE/XNYS/XNAS)
    
    Returns a list of tuples `(ticker, score)` sorted by score descending.
    """
    if weights is None:
        weights = {
            'xbrl_richness': 0.30,       # Rich XBRL = important company
            'key_metrics': 0.25,         # Core metrics = worth detailed info
            'has_stock_data': 0.20,      # Has trading data = active ticker
            'filing_activity': 0.15,     # Recent filers = important
            'exchange_priority': 0.10    # Major exchange = higher quality
        }
    
    # Defensive normalization
    total_w = sum(weights.values())
    if total_w <= 0:
        raise ValueError("weights must sum to > 0")
    weights = {k: v / total_w for k, v in weights.items()}
    
    scores: Dict[str, float] = {t: 0.0 for t in tickers}
    if not tickers:
        return []
    
    try:
        con = duckdb.connect(database=db_path, read_only=True)
    except Exception:
        return [(t, 0.0) for t in tickers]
    
    try:
        # Key financial metrics to check for
        key_metric_patterns = [
            'Assets', 'Liabilities', 'StockholdersEquity',
            'Revenue', 'NetIncome', 'OperatingIncome',
            'CashAndCashEquivalents', 'EarningsPerShare'
        ]
        
        # Major US exchanges (Polygon uses these codes)
        major_exchanges = {'XNAS', 'XNYS', 'NASDAQ', 'NYSE', 'ARCX', 'BATS'}
        
        # Collect metrics for each ticker
        xbrl_tag_counts = {}
        key_metrics_counts = {}
        has_stock_data_flags = {}
        filing_counts = {}
        exchange_scores = {}
        
        lookback_threshold = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
        
        for t in tickers:
            try:
                row = con.execute("SELECT cik FROM tickers WHERE ticker = ? LIMIT 1;", [t]).fetchone()
                cik = row[0] if row else None
            except Exception:
                cik = None
            
            # Default values
            xbrl_tag_counts[t] = 0
            key_metrics_counts[t] = 0
            has_stock_data_flags[t] = 0
            filing_counts[t] = 0
            exchange_scores[t] = 0.0
            
            if cik:
                # XBRL richness: count unique tags
                try:
                    r = con.execute(
                        "SELECT COUNT(DISTINCT tag_name) FROM xbrl_facts WHERE cik = ?;",
                        [cik]
                    ).fetchone()
                    xbrl_tag_counts[t] = int(r[0]) if r and r[0] else 0
                except Exception:
                    pass
                
                # Key metrics presence
                try:
                    key_count = 0
                    for metric in key_metric_patterns:
                        r = con.execute(
                            "SELECT COUNT(*) FROM xbrl_facts WHERE cik = ? AND tag_name LIKE ? LIMIT 1;",
                            [cik, f'%{metric}%']
                        ).fetchone()
                        if r and r[0] > 0:
                            key_count += 1
                    key_metrics_counts[t] = key_count
                except Exception:
                    pass
                
                # Filing activity
                try:
                    r = con.execute(
                        "SELECT COUNT(*) FROM filings WHERE cik = ? AND filing_date >= ?;",
                        [cik, lookback_threshold]
                    ).fetchone()
                    filing_counts[t] = int(r[0]) if r else 0
                except Exception:
                    pass
            
            # Has stock data (any history at all)
            try:
                r = con.execute(
                    "SELECT COUNT(*) FROM stock_history WHERE ticker = ? LIMIT 1;",
                    [t]
                ).fetchone()
                has_stock_data_flags[t] = 1 if (r and r[0] and r[0] > 0) else 0
            except Exception:
                pass
            
            # Exchange priority (check updated_ticker_info if exists)
            try:
                # Also check normalized ticker (hyphen -> period)
                normalized_ticker = t.replace('-', '.')
                r = con.execute(
                    "SELECT primary_exchange FROM updated_ticker_info WHERE ticker IN (?, ?) LIMIT 1;",
                    [t, normalized_ticker]
                ).fetchone()
                if r and r[0]:
                    exchange = str(r[0]).upper()
                    if exchange in major_exchanges:
                        exchange_scores[t] = 1.0
                    else:
                        exchange_scores[t] = 0.3  # Other exchanges get partial credit
            except Exception:
                pass
        
        # Normalize metrics
        xbrl_vals = [float(v) for v in xbrl_tag_counts.values()]
        key_vals = [float(v) for v in key_metrics_counts.values()]
        filing_vals = [float(v) for v in filing_counts.values()]
        stock_vals = [float(v) for v in has_stock_data_flags.values()]
        exchange_vals = [float(v) for v in exchange_scores.values()]
        
        max_xbrl = _safe_max(xbrl_vals)
        max_key = _safe_max(key_vals)
        max_filing = _safe_max(filing_vals)
        max_stock = _safe_max(stock_vals)
        max_exchange = _safe_max(exchange_vals)
        
        for t in tickers:
            xbrl_norm = (xbrl_tag_counts[t] / max_xbrl) if max_xbrl else 0.0
            key_norm = (key_metrics_counts[t] / max_key) if max_key else 0.0
            filing_norm = (filing_counts[t] / max_filing) if max_filing else 0.0
            stock_norm = (has_stock_data_flags[t] / max_stock) if max_stock else 0.0
            exchange_norm = (exchange_scores[t] / max_exchange) if max_exchange else 0.0
            
            score = (
                weights['xbrl_richness'] * xbrl_norm +
                weights['key_metrics'] * key_norm +
                weights['has_stock_data'] * stock_norm +
                weights['filing_activity'] * filing_norm +
                weights['exchange_priority'] * exchange_norm
            )
            scores[t] = float(score)
        
        # Build sorted list
        sorted_list = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return sorted_list
    finally:
        try:
            con.close()
        except Exception:
            pass


def prioritize_tickers_hybrid(
    db_path: str,
    tickers: List[str],
    weights: Optional[Dict[str, float]] = None,
    lookback_days: int = 365,
) -> List[Tuple[str, float]]:
    """
    Score and rank tickers using a hybrid scoring strategy (Option C).

    The default hybrid score combines:
      - filings activity in the last `lookback_days` (weight: filings_recent)
      - total filings count (weight: filings_total)
      - recency of company parse/update (weight: company_recency)

    Returns a list of tuples `(ticker, score)` sorted by score descending.
    Missing tables or missing data are handled gracefully; tickers with no
    metadata receive low (but non-zero) scores.
    """
    if weights is None:
        weights = {
            'filings_recent': 0.6,
            'filings_total': 0.25,
            'company_recency': 0.15
        }

    # Defensive normalization: ensure weights sum to 1
    total_w = sum(weights.values())
    if total_w <= 0:
        raise ValueError("weights must sum to > 0")
    weights = {k: v / total_w for k, v in weights.items()}

    scores: Dict[str, float] = {t: 0.0 for t in tickers}
    if not tickers:
        return []

    try:
        con = duckdb.connect(database=db_path, read_only=True)
    except Exception:
        # If the DB can't be opened, return equal low scores so callers can still proceed
        return [(t, 0.0) for t in tickers]

    try:
        # Prepare buckets
        recent_counts = {}
        total_counts = {}
        recency_days = {}

        lookback_threshold = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()

        # Gather metrics per ticker by mapping to CIK
        for t in tickers:
            try:
                row = con.execute("SELECT cik FROM tickers WHERE ticker = ? LIMIT 1;", [t]).fetchone()
                cik = row[0] if row else None
            except Exception:
                cik = None

            # Default values
            recent_counts[t] = 0
            total_counts[t] = 0
            recency_days[t] = float('inf')

            if cik:
                try:
                    r = con.execute(
                        "SELECT COUNT(*) FROM filings WHERE cik = ? AND filing_date >= ?;",
                        [cik, lookback_threshold]
                    ).fetchone()
                    recent_counts[t] = int(r[0]) if r else 0
                except Exception:
                    recent_counts[t] = 0

                try:
                    r = con.execute("SELECT COUNT(*) FROM filings WHERE cik = ?;", [cik]).fetchone()
                    total_counts[t] = int(r[0]) if r else 0
                except Exception:
                    total_counts[t] = 0

                try:
                    r = con.execute("SELECT MAX(filing_date) FROM filings WHERE cik = ?;", [cik]).fetchone()
                    last_date = r[0] if r else None
                    if last_date:
                        # duckdb may return a datetime.date or a string
                        if isinstance(last_date, str):
                            last_date = datetime.fromisoformat(last_date).date()
                        if isinstance(last_date, date):
                            recency_days[t] = (datetime.now(timezone.utc).date() - last_date).days
                except Exception:
                    recency_days[t] = float('inf')

        # Normalize metrics
        recent_vals = [float(v) for v in recent_counts.values()]
        total_vals = [float(v) for v in total_counts.values()]
        recency_vals = [float(v if math.isfinite(v) else 1e6) for v in recency_days.values()]

        max_recent = _safe_max(recent_vals)
        max_total = _safe_max(total_vals)
        max_recency = _safe_max(recency_vals)

        for t in tickers:
            recent_norm = (recent_counts[t] / max_recent) if max_recent else 0.0
            total_norm = (total_counts[t] / max_total) if max_total else 0.0
            # recency: smaller is better, so invert and normalize
            recency_norm = (1.0 - (recency_days[t] / max_recency)) if max_recency else 0.0
            if recency_norm < 0:
                recency_norm = 0.0

            score = (
                weights['filings_recent'] * recent_norm +
                weights['filings_total'] * total_norm +
                weights['company_recency'] * recency_norm
            )
            scores[t] = float(score)

        # Build sorted list
        sorted_list = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return sorted_list
    finally:
        try:
            con.close()
        except Exception:
            pass
