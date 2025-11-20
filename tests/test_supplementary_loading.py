"""Tests for supplementary loading strategies.

Focus:
 - Incremental upsert behavior (stock_history)
 - Blue-green swap behavior (macro_economic_data)
 - Ticker info incremental vs full refresh

Uses temporary parquet directories and an ephemeral DuckDB file.
"""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import date
from typing import cast

import duckdb
import pandas as pd  # type: ignore
import pytest

from data_processing.load_supplementary_data import load_data, INCREMENTAL_TABLES
from data_processing.load_ticker_info import load_ticker_info_from_parquet


class StubConfig:
    """Minimal config stub providing attributes consumed by loaders."""
    def __init__(self, parquet_dir: Path, db_file: Path):
        self.PARQUET_DIR = parquet_dir
        self.DB_FILE_STR = str(db_file)
        self.DB_FILE = db_file


@pytest.fixture()
def temp_env(tmp_path: Path):
    parquet_root = tmp_path / "parquet"
    parquet_root.mkdir()
    db_file = tmp_path / "test.duckdb"
    return StubConfig(parquet_root, db_file)


@pytest.fixture()
def logger():
    lg = logging.getLogger("supp_loader_test")
    if not lg.handlers:
        lg.addHandler(logging.NullHandler())
    lg.setLevel(logging.INFO)
    return lg


def test_incremental_stock_history_upsert(temp_env: StubConfig, logger):
    assert "stock_history" in INCREMENTAL_TABLES
    parquet_dir = temp_env.PARQUET_DIR / "stock_history"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Initial batch
    df1 = pd.DataFrame([
        {"ticker": "AAA", "date": date(2024, 1, 1), "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "adj_close": 10.5, "volume": 1000},
        {"ticker": "AAA", "date": date(2024, 1, 2), "open": 11.0, "high": 12.0, "low": 10.5, "close": 11.5, "adj_close": 11.5, "volume": 1500},
    ])
    df1.to_parquet(parquet_dir / "batch1.parquet", index=False)
    load_data(cast(object, temp_env), logger, "stock_history", full_refresh=False)  # type: ignore[arg-type]

    # Second batch with updated close on Jan 2
    df2 = pd.DataFrame([
        {"ticker": "AAA", "date": date(2024, 1, 1), "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "adj_close": 10.5, "volume": 1000},
        {"ticker": "AAA", "date": date(2024, 1, 2), "open": 11.0, "high": 12.0, "low": 10.5, "close": 12.0, "adj_close": 12.0, "volume": 1500},
    ])
    df2.to_parquet(parquet_dir / "batch2.parquet", index=False)
    load_data(cast(object, temp_env), logger, "stock_history", full_refresh=False)  # type: ignore[arg-type]

    con = duckdb.connect(temp_env.DB_FILE_STR)
    row = con.execute("SELECT close FROM stock_history WHERE ticker='AAA' AND date='2024-01-02';").fetchone()
    assert row is not None, "Query returned no row for updated close"
    updated_close = row[0]
    con.close()
    assert updated_close == 12.0, "Incremental upsert failed to replace existing row value"


def test_blue_green_macro_swap_guard(temp_env: StubConfig, logger):
    parquet_dir = temp_env.PARQUET_DIR / "macro_economic_data"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    df_initial = pd.DataFrame([
        {"series_id": "CPI", "date": date(2024, 1, 1), "value": 100.0},
        {"series_id": "CPI", "date": date(2024, 1, 2), "value": 101.0},
    ])
    df_initial.to_parquet(parquet_dir / "macro_batch1.parquet", index=False)
    load_data(cast(object, temp_env), logger, "macro_economic_data", full_refresh=False)  # type: ignore[arg-type]

    # Attempt to swap with reduced dataset (should be guarded and retain original)
    for f in parquet_dir.glob("*.parquet"):
        f.unlink()
    df_smaller = pd.DataFrame([
        {"series_id": "CPI", "date": date(2024, 1, 2), "value": 999.0},
    ])
    df_smaller.to_parquet(parquet_dir / "macro_batch_reduced.parquet", index=False)
    load_data(cast(object, temp_env), logger, "macro_economic_data", full_refresh=False)  # type: ignore[arg-type]  # should abort swap

    con = duckdb.connect(temp_env.DB_FILE_STR)
    rows = con.execute("SELECT COUNT(*) FROM macro_economic_data;").fetchone()
    assert rows is not None and rows[0] == 2, "Swap guard failed; dataset was reduced"
    con.close()


def test_blue_green_macro_empty_guard(temp_env: StubConfig, logger):
    parquet_dir = temp_env.PARQUET_DIR / "macro_economic_data"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    # Initial load
    df_initial = pd.DataFrame([
        {"series_id": "GDP", "date": date(2024, 1, 1), "value": 200.0},
    ])
    df_initial.to_parquet(parquet_dir / "macro_batch_initial.parquet", index=False)
    load_data(cast(object, temp_env), logger, "macro_economic_data", full_refresh=False)  # type: ignore[arg-type]
    # Remove parquet files to force empty staging
    for f in parquet_dir.glob("*.parquet"):
        f.unlink()
    # Create an empty parquet
    pd.DataFrame(columns=["series_id", "date", "value"]).to_parquet(parquet_dir / "macro_batch_empty.parquet", index=False)
    load_data(cast(object, temp_env), logger, "macro_economic_data", full_refresh=False)  # type: ignore[arg-type]
    con = duckdb.connect(temp_env.DB_FILE_STR)
    remaining = con.execute("SELECT COUNT(*) FROM macro_economic_data;").fetchone()
    assert remaining is not None and remaining[0] == 1, "Empty staging swap should have been aborted"
    con.close()


def test_ticker_info_incremental_and_full_refresh(temp_env: StubConfig):
    parquet_dir = temp_env.PARQUET_DIR / "updated_ticker_info"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    db_con = duckdb.connect(temp_env.DB_FILE_STR)

    # Initial incremental load
    df_inc1 = pd.DataFrame([
        {"ticker": "ABC", "cik": "0000000001", "name": "ABC Corp", "market": "stocks", "locale": "US", "primary_exchange": "XNYS", "type": "CS", "active": True,
         "currency_name": "US Dollar", "currency_symbol": "USD", "base_currency_name": "US Dollar", "base_currency_symbol": "USD", "composite_figi": None, "share_class_figi": None,
         "description": "First batch", "homepage_url": None, "total_employees": 100, "list_date": "2020-01-01", "sic_code": None, "sic_description": None, "ticker_root": "ABC", "source_feed": None,
         "market_cap": 100.0, "weighted_shares_outstanding": 1000000, "round_lot": 100, "fetch_timestamp": pd.Timestamp.utcnow(), "last_updated_utc": pd.Timestamp.utcnow(), "delisted_utc": None,
         "address_1": None, "city": None, "state": None, "postal_code": None, "logo_url": None, "icon_url": None}
    ])
    df_inc1.to_parquet(parquet_dir / "info_batch1.parquet", index=False)
    load_ticker_info_from_parquet(db_con, parquet_dir, full_refresh=False)

    # Second incremental update modifies market_cap
    df_inc2 = df_inc1.copy()
    df_inc2.loc[0, "market_cap"] = 150.0
    df_inc2.to_parquet(parquet_dir / "info_batch2.parquet", index=False)
    load_ticker_info_from_parquet(db_con, parquet_dir, full_refresh=False)
    row_cap = db_con.execute("SELECT market_cap FROM updated_ticker_info WHERE ticker='ABC';").fetchone()
    assert row_cap is not None, "Ticker ABC not found after incremental update"
    cap_inc = row_cap[0]
    assert cap_inc == 150.0, "Incremental upsert failed for ticker info"

    # Full refresh attempt with smaller dataset (guard should abort replacement)
    # First add another distinct ticker incrementally to establish baseline of 2 distinct tickers
    df_second = df_inc1.copy()
    df_second.loc[0, "ticker"] = "XYZ"
    df_second.loc[0, "market_cap"] = 300.0
    df_second.to_parquet(parquet_dir / "info_batch3.parquet", index=False)
    load_ticker_info_from_parquet(db_con, parquet_dir, full_refresh=False)
    
    # Verify we now have 2 distinct tickers
    current_distinct = db_con.execute("SELECT COUNT(DISTINCT ticker) FROM updated_ticker_info;").fetchone()
    assert current_distinct is not None and current_distinct[0] == 2, "Setup failed; should have 2 distinct tickers"
    
    # Now attempt full refresh with only 1 ticker (should be blocked)
    for f in parquet_dir.glob("*.parquet"):
        f.unlink()
    df_single = df_inc1.copy()
    df_single.to_parquet(parquet_dir / "info_full_smaller.parquet", index=False)
    load_ticker_info_from_parquet(db_con, parquet_dir, full_refresh=True)
    
    # Expect both original tickers retained (guard active)
    rows_full_guard = db_con.execute("SELECT ticker FROM updated_ticker_info ORDER BY ticker;").fetchall()
    assert len(rows_full_guard) == 2 and rows_full_guard[0][0] == "ABC" and rows_full_guard[1][0] == "XYZ", "Full refresh guard failed; ticker universe reduced"
    db_con.close()
