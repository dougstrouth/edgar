import duckdb
from datetime import date
import pandas as pd

from data_gathering.stock_data_gatherer_polygon import get_missing_intervals


def test_missing_intervals_with_gaps():
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE TABLE stock_history (ticker VARCHAR, date DATE);")
    # Insert dates 2025-01-01 and 2025-01-03 for ticker 'A'
    con.execute("INSERT INTO stock_history VALUES ('A', DATE '2025-01-01'), ('A', DATE '2025-01-03');")

    intervals = get_missing_intervals(con, 'A', date(2025, 1, 1), date(2025, 1, 5))

    # Expect gaps: 2025-01-02 and 2025-01-04 to 2025-01-05
    assert isinstance(intervals, list)
    assert len(intervals) == 2
    assert intervals[0]['start'] == date(2025, 1, 2) and intervals[0]['end'] == date(2025, 1, 2)
    assert intervals[1]['start'] == date(2025, 1, 4) and intervals[1]['end'] == date(2025, 1, 5)


def test_missing_intervals_no_existing_rows():
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE TABLE stock_history (ticker VARCHAR, date DATE);")

    intervals = get_missing_intervals(con, 'B', date(2024, 12, 25), date(2024, 12, 31))
    # No existing rows -> single interval covering full range
    assert isinstance(intervals, list)
    assert len(intervals) == 1
    assert intervals[0]['start'] == date(2024, 12, 25)
    assert intervals[0]['end'] == date(2024, 12, 31)
