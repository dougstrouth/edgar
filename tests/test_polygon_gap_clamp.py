from datetime import date, timedelta

from data_gathering.stock_data_gatherer_polygon import _clamp_date_range


def test_clamp_date_range_no_change_for_small_span():
    start = date(2025, 11, 10)
    end = date(2025, 11, 18)
    clamp_days = 30
    assert _clamp_date_range(start, end, clamp_days) == start


def test_clamp_date_range_reduces_large_span():
    start = date(2020, 11, 19)
    end = date(2025, 11, 18)
    clamp_days = 365  # 1 year
    adjusted = _clamp_date_range(start, end, clamp_days)
    assert adjusted == end - timedelta(days=clamp_days)
    # Ensure span equals clamp_days
    assert (end - adjusted).days == clamp_days


def test_clamp_edge_case_exact_span():
    clamp_days = 100
    start = date(2025, 8, 10)
    end = start + timedelta(days=clamp_days)
    assert (end - start).days == clamp_days
    assert _clamp_date_range(start, end, clamp_days) == start