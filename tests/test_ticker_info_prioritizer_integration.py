from datetime import datetime, timezone
from typing import List, Tuple
from pathlib import Path

import types

from data_gathering.ticker_info_gatherer_polygon import run_polygon_ticker_info_pipeline
from utils.config_utils import AppConfig


class DummyConfig(AppConfig):
    def __init__(self):  # override to avoid filesystem lookups
        self.DB_FILE_STR = ':memory:'
        self.PARQUET_DIR = Path('.').resolve()
        self.DB_FILE = Path(':memory:')


def fake_prioritizer(db_path: str, tickers: List[str], lookback_days: int) -> List[Tuple[str, float]]:
    # Reverse order scoring for test determinism
    return [(t, i) for i, t in enumerate(reversed(tickers), 1)]


def test_prioritize_applies_order(monkeypatch, tmp_path):
    # Patch prioritizer (must patch the imported symbol in gatherer module)
    monkeypatch.setattr('data_gathering.ticker_info_gatherer_polygon.prioritize_tickers_for_stock_data', fake_prioritizer)

    # Minimal config pointing parquet dir to temp
    cfg = DummyConfig()
    cfg.PARQUET_DIR = tmp_path

    # Create a fake tickers table in an in-memory duckdb via temporary DB file
    import duckdb
    db_path = tmp_path / 'test.duckdb'
    conn = duckdb.connect(str(db_path))
    conn.execute('CREATE TABLE tickers (ticker VARCHAR);')
    conn.execute("INSERT INTO tickers VALUES ('AAA'), ('BBB'), ('CCC'), ('DDD')")
    conn.close()
    cfg.DB_FILE_STR = str(db_path)
    cfg.DB_FILE = db_path

    # Monkeypatch get_existing_ticker_info to return empty so all tickers selected
    monkeypatch.setattr('data_gathering.ticker_info_gatherer_polygon.get_existing_ticker_info', lambda con, t: {})
    # Monkeypatch filter_tickers_for_info to passthrough
    monkeypatch.setattr('data_gathering.ticker_info_gatherer_polygon.filter_tickers_for_info', lambda all_t, existing, days, force, current_time=None: all_t)

    # Spy: capture first prioritized ticker line
    capture = {}
    orig_info = run_polygon_ticker_info_pipeline.__globals__['logger'].info
    def spy(msg):
        if msg.strip().startswith('1.') and 'score=' in msg:
            capture['first_line'] = msg
        orig_info(msg)
    monkeypatch.setattr(run_polygon_ticker_info_pipeline.__globals__['logger'], 'info', spy)

    # Run (limit 3 to test ordering)
    run_polygon_ticker_info_pipeline(
        config=cfg,
        target_tickers=None,
        limit=3,
        force_refresh=False,
        prioritize=True,
        prioritizer_lookback_days=10,
    )

    # Verify ordering changed from original alphabetical (AAA would have been first)
    first_line = capture.get('first_line', '')
    assert first_line, 'Did not capture prioritized output line'
    assert 'AAA' not in first_line, f'Prioritization did not alter ordering: {first_line}'