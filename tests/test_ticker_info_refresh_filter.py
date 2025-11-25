from datetime import datetime, timezone, timedelta

from data_gathering.ticker_info_gatherer_polygon import filter_tickers_for_info


def test_filter_force_refresh_returns_all():
    all_tickers = ["AAA", "BBB", "CCC"]
    existing = {"AAA": datetime.now(timezone.utc)}
    result = filter_tickers_for_info(all_tickers, existing, refresh_days=30, force_refresh=True, current_time=datetime.now(timezone.utc))
    assert result == all_tickers


def test_filter_includes_missing_and_stale():
    now = datetime.now(timezone.utc)
    fresh = now - timedelta(days=5)
    stale = now - timedelta(days=60)
    existing = {
        "AAA": fresh,
        "BBB": stale,
    }
    all_tickers = ["AAA", "BBB", "CCC"]  # CCC missing
    now = datetime.now(timezone.utc)
    result = filter_tickers_for_info(all_tickers, existing, refresh_days=30, force_refresh=False, current_time=now)
    assert "BBB" in result  # stale
    assert "CCC" in result  # missing
    assert "AAA" not in result  # fresh


def test_filter_edge_exact_cutoff():
    now = datetime.now(timezone.utc)
    cutoff_days = 30
    exact_cutoff = now - timedelta(days=cutoff_days)
    existing = {"AAA": exact_cutoff}
    all_tickers = ["AAA"]
    # At exact cutoff, treat as fresh (ts < cutoff triggers fetch)
    result = filter_tickers_for_info(all_tickers, existing, refresh_days=cutoff_days, force_refresh=False, current_time=now)
    assert result == []