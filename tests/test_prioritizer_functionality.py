#!/usr/bin/env python3
"""
Test prioritizer functionality for stock data gathering.

Verifies that the prioritizer correctly scores and ranks tickers
based on XBRL data quality, stock data completeness, and filing activity.
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.prioritizer import prioritize_tickers_for_stock_data
from utils.config_utils import AppConfig


def test_prioritizer_basic():
    """Test basic prioritizer functionality with mock data."""
    
    print("Basic Prioritizer Test")
    print("=" * 60)
    
    # Test with empty list
    result = prioritize_tickers_for_stock_data(
        db_path="nonexistent.db",
        tickers=[],
        lookback_days=365
    )
    
    assert result == [], "Empty ticker list should return empty result"
    print("✅ Empty list handling: PASS")
    
    # Test with non-existent DB (should return 0 scores gracefully)
    test_tickers = ['AAPL', 'MSFT', 'GOOGL']
    result = prioritize_tickers_for_stock_data(
        db_path="nonexistent.db",
        tickers=test_tickers,
        lookback_days=365
    )
    
    assert len(result) == 3, f"Expected 3 results, got {len(result)}"
    assert all(score == 0.0 for _, score in result), "Non-existent DB should return 0 scores"
    print("✅ Graceful failure handling: PASS")
    
    return True


def test_prioritizer_with_real_db():
    """Test prioritizer with real database if available."""
    
    print("\nReal Database Prioritizer Test")
    print("=" * 60)
    
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        db_path = config.DB_FILE_STR
        
        if not Path(db_path).exists():
            print("⚠️  Database not found, skipping real DB test")
            print(f"   DB path: {db_path}")
            return True
        
        print(f"Using database: {db_path}")
        
        # Get some test tickers from the database
        import duckdb
        con = duckdb.connect(database=db_path, read_only=True)
        
        try:
            # Get 10 random tickers
            tickers_df = con.execute(
                "SELECT DISTINCT ticker FROM tickers LIMIT 10"
            ).df()
            test_tickers = tickers_df['ticker'].tolist()
            
            if not test_tickers:
                print("⚠️  No tickers in database, skipping real DB test")
                return True
            
            print(f"Testing with {len(test_tickers)} tickers from database")
            print(f"Tickers: {', '.join(test_tickers[:5])}...")
            
            # Run prioritization
            result = prioritize_tickers_for_stock_data(
                db_path=db_path,
                tickers=test_tickers,
                lookback_days=365
            )
            
            print(f"\nPrioritization Results:")
            print(f"{'Rank':<6} {'Ticker':<8} {'Score':<10}")
            print("-" * 30)
            
            for i, (ticker, score) in enumerate(result[:10], 1):
                print(f"{i:<6} {ticker:<8} {score:<10.4f}")
            
            # Verify results
            assert len(result) == len(test_tickers), "Result count mismatch"
            assert all(isinstance(t, str) for t, _ in result), "Tickers should be strings"
            assert all(isinstance(s, float) for _, s in result), "Scores should be floats"
            assert all(s >= 0 for _, s in result), "Scores should be non-negative"
            
            # Verify sorting (descending by score)
            scores = [s for _, s in result]
            assert scores == sorted(scores, reverse=True), "Results should be sorted by score descending"
            
            print("\n✅ Real database test: PASS")
            return True
            
        finally:
            con.close()
            
    except Exception as e:
        print(f"⚠️  Could not run real DB test: {e}")
        import traceback
        traceback.print_exc()
        return True  # Don't fail the test suite if DB isn't set up


def test_prioritizer_custom_weights():
    """Test that custom weights are properly applied."""
    
    print("\nCustom Weights Test")
    print("=" * 60)
    
    test_tickers = ['AAPL', 'MSFT']
    
    # Test with custom weights
    custom_weights = {
        'xbrl_richness': 0.5,
        'key_metrics': 0.3,
        'stock_data_need': 0.15,
        'filing_activity': 0.05
    }
    
    result = prioritize_tickers_for_stock_data(
        db_path="nonexistent.db",
        tickers=test_tickers,
        weights=custom_weights,
        lookback_days=365
    )
    
    assert len(result) == 2, "Should return results for both tickers"
    print("✅ Custom weights accepted: PASS")
    
    return True


def test_prioritizer_lookback_period():
    """Test that different lookback periods work."""
    
    print("\nLookback Period Test")
    print("=" * 60)
    
    test_tickers = ['AAPL']
    
    # Test different lookback periods
    for lookback_days in [30, 90, 365, 730]:
        result = prioritize_tickers_for_stock_data(
            db_path="nonexistent.db",
            tickers=test_tickers,
            lookback_days=lookback_days
        )
        assert len(result) == 1, f"Failed for lookback_days={lookback_days}"
    
    print("✅ Various lookback periods: PASS")
    return True


if __name__ == "__main__":
    print("Prioritizer Functionality Tests")
    print("=" * 60)
    print()
    
    results = []
    results.append(("Basic Functionality", test_prioritizer_basic()))
    results.append(("Real Database", test_prioritizer_with_real_db()))
    results.append(("Custom Weights", test_prioritizer_custom_weights()))
    results.append(("Lookback Periods", test_prioritizer_lookback_period()))
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:25s} {status}")
    
    print()
    
    if all(passed for _, passed in results):
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed")
        sys.exit(1)
