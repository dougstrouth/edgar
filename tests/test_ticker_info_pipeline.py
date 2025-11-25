#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Test: Polygon Ticker Info Pipeline

Tests the ticker info gatherer and loader with a small sample of tickers.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_utils import AppConfig  # noqa: E402
from data_gathering.ticker_info_gatherer_polygon import run_polygon_ticker_info_pipeline  # noqa: E402
from data_processing.load_ticker_info import run_ticker_info_loader  # noqa: E402


def main():
    """Test the ticker info pipeline with a few well-known tickers."""
    print("=" * 80)
    print("Testing Polygon Ticker Info Pipeline")
    print("=" * 80)
    
    # Load config
    config = AppConfig()
    
    # Test tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    print(f"\nTest tickers: {', '.join(test_tickers)}")
    
    # Step 1: Fetch ticker info
    print("\n" + "=" * 80)
    print("Step 1: Fetching ticker info from Polygon.io")
    print("=" * 80)
    run_polygon_ticker_info_pipeline(
        config=config,
        target_tickers=test_tickers,
        force_refresh=True  # Force refresh for testing
    )
    
    # Step 2: Load into database
    print("\n" + "=" * 80)
    print("Step 2: Loading ticker info into database")
    print("=" * 80)
    run_ticker_info_loader(config)
    
    # Step 3: Query and verify
    print("\n" + "=" * 80)
    print("Step 3: Verifying data in database")
    print("=" * 80)
    
    from utils.database_conn import ManagedDatabaseConnection
    
    with ManagedDatabaseConnection(str(config.DB_FILE)) as con:
        if con is None:
            print("ERROR: Could not connect to database")
            return
        
        # Check if table exists
        tables = {row[0].lower() for row in con.execute("SHOW TABLES;").fetchall()}
        if "updated_ticker_info" not in tables:
            print("ERROR: Table 'updated_ticker_info' does not exist")
            return
        
        # Query our test tickers
        placeholders = ','.join(['?'] * len(test_tickers))
        query = f"""
            SELECT 
                ticker, 
                name, 
                active, 
                market, 
                primary_exchange,
                sic_description,
                total_employees,
                market_cap
            FROM updated_ticker_info
            WHERE ticker IN ({placeholders})
            ORDER BY ticker
        """
        
        df = con.execute(query, test_tickers).df()
        
        if df.empty:
            print("ERROR: No data found for test tickers")
        else:
            print(f"\nFound {len(df)} ticker(s) in database:\n")
            print(df.to_string(index=False))
            print("\n✅ Test completed successfully!")


if __name__ == "__main__":
    main()
