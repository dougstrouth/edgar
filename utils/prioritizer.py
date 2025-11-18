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
