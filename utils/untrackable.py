# -*- coding: utf-8 -*-
"""Shared utilities for tracking untrackable (permanent failure) Polygon/Massive tickers.

Centralizes table schema creation, insertion, and querying logic so gatherers
(ticker info & stock aggregates) do not duplicate implementations.

Table schema:
    polygon_untrackable_tickers (
        ticker VARCHAR COLLATE NOCASE PRIMARY KEY,
        reason VARCHAR,
        last_failed_timestamp TIMESTAMPTZ
    )

Functions:
    ensure_untrackable_table(con)
    mark_untrackable(con, ticker, reason)
    get_untrackable_tickers(con, expiry_days=365)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Set
import logging

import duckdb  # type: ignore

logger = logging.getLogger(__name__)

TABLE_NAME = "polygon_untrackable_tickers"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
    reason VARCHAR,
    last_failed_timestamp TIMESTAMPTZ
);"""

INSERT_SQL = f"INSERT OR REPLACE INTO {TABLE_NAME} VALUES (?, ?, ?);"

QUERY_SQL_TEMPLATE = (
    "SELECT ticker FROM {table} "
    "WHERE last_failed_timestamp >= (now() - INTERVAL '{{expiry_days}} days');"
)


def ensure_untrackable_table(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure the untrackable tickers table exists."""
    try:
        con.execute(CREATE_SQL)
    except Exception as e:
        logger.error(f"Failed to ensure {TABLE_NAME} exists: {e}")
        raise


def mark_untrackable(con: duckdb.DuckDBPyConnection, ticker: str, reason: str) -> None:
    """Insert or update a ticker as untrackable with current timestamp."""
    try:
        ensure_untrackable_table(con)
        con.execute(INSERT_SQL, [ticker, reason, datetime.now(timezone.utc)])
        logger.info(f"🚫 Marked {ticker} as Polygon-untrackable (client error) in database")
    except Exception as e:
        logger.error(f"Failed to mark {ticker} as untrackable: {e}")
        raise


def get_untrackable_tickers(con: duckdb.DuckDBPyConnection, expiry_days: int = 365) -> Set[str]:
    """Return set of tickers marked untrackable within expiry window."""
    logger.info(f"Querying for Polygon untrackable tickers (expiry: {expiry_days} days) to exclude...")
    try:
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if TABLE_NAME not in tables:
            logger.info(f"{TABLE_NAME} table doesn't exist yet")
            return set()
        query = QUERY_SQL_TEMPLATE.format(table=TABLE_NAME).replace("{expiry_days}", str(expiry_days))
        results = con.execute(query).fetchall()
        out = {row[0] for row in results}
        logger.info(f"Found {len(out)} Polygon untrackable tickers to skip")
        return out
    except Exception as e:
        logger.warning(f"Could not query {TABLE_NAME}: {e}")
        return set()
