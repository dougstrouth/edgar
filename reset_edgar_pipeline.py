#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDGAR Pipeline Reset Script

Safely resets the EDGAR data pipeline to a clean state while preserving stock data.
This script will:
1. Clear EDGAR-related tables (companies, filings, xbrl_facts, etc.)
2. Remove parquet data files
3. Remove extracted JSON files
4. Keep stock data tables (stock_history, macro_economic_data, etc.)
5. Keep download archives (submissions.zip, companyfacts.zip)

Run this when you want to start fresh with EDGAR data but keep your stock data.
"""

import sys
from pathlib import Path
import duckdb
import shutil

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import after path setup
from utils.config_utils import AppConfig  # noqa: E402
from utils.logging_utils import setup_logging  # noqa: E402


def reset_edgar_pipeline(config: AppConfig, logger):
    """Reset EDGAR pipeline to clean state while preserving stock data."""
    
    logger.info("=" * 80)
    logger.info("EDGAR PIPELINE RESET")
    logger.info("=" * 80)
    
    # Tables to preserve (stock data and utility tables)
    preserve_tables = {
        'stock_history',
        'macro_economic_data', 
        'market_risk_factors',
        'prioritized_tickers_stock_backlog',
        'stock_fetch_plan',
        'updated_ticker_info',
        'yf_fetch_status',
        'downloaded_archives'  # Keep track of what we've downloaded
    }
    
    # Tables to clear (EDGAR data)
    edgar_tables = {
        'companies',
        'filings',
        'filing_summaries',
        'tickers',
        'former_names',
        'xbrl_facts',
        'xbrl_facts_orphaned',
        'xbrl_tags'
    }
    
    # Step 1: Clear EDGAR tables
    logger.info("\n--- Step 1: Clearing EDGAR tables ---")
    try:
        conn = duckdb.connect(str(config.DB_FILE))
        
        # Get all existing tables
        existing_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        existing_table_names = {t[0] for t in existing_tables}
        
        # Drop EDGAR tables
        for table in edgar_tables:
            if table in existing_table_names:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                row_count = result[0] if result else 0
                logger.info(f"Dropping table '{table}' ({row_count:,} rows)...")
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            else:
                logger.info(f"Table '{table}' does not exist, skipping...")
        
        # Show preserved tables
        logger.info("\n--- Preserved Tables (Stock Data) ---")
        for table in preserve_tables:
            if table in existing_table_names:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                row_count = result[0] if result else 0
                logger.info(f"✓ Keeping '{table}': {row_count:,} rows")
        
        conn.close()
        logger.info("✓ Database tables cleared successfully")
        
    except Exception as e:
        logger.error(f"✗ Error clearing database tables: {e}", exc_info=True)
        return False
    
    # Step 2: Remove parquet data
    logger.info("\n--- Step 2: Removing parquet data ---")
    parquet_dirs = [
        config.DOWNLOAD_DIR / "parquet_data",
        Path("/Users/dougstrouth/datasets_noicloud/edgar/parquet"),
        config.PROJECT_ROOT / "data" / "output" / "parquet"
    ]
    
    for parquet_dir in parquet_dirs:
        if parquet_dir.exists():
            try:
                logger.info(f"Removing {parquet_dir}...")
                shutil.rmtree(parquet_dir)
                logger.info(f"✓ Removed {parquet_dir}")
            except Exception as e:
                logger.warning(f"Could not remove {parquet_dir}: {e}")
        else:
            logger.info(f"Parquet directory {parquet_dir} does not exist, skipping...")
    
    # Step 3: Remove extracted JSON
    logger.info("\n--- Step 3: Removing extracted JSON files ---")
    extracted_dirs = [
        config.DOWNLOAD_DIR / "extracted_json",
        config.PROJECT_ROOT / "data" / "downloads" / "extracted_json"
    ]
    
    for extracted_dir in extracted_dirs:
        if extracted_dir.exists():
            try:
                logger.info(f"Removing {extracted_dir}...")
                shutil.rmtree(extracted_dir)
                logger.info(f"✓ Removed {extracted_dir}")
            except Exception as e:
                logger.warning(f"Could not remove {extracted_dir}: {e}")
        else:
            logger.info(f"Extracted directory {extracted_dir} does not exist, skipping...")
    
    # Step 4: Show what's preserved
    logger.info("\n--- Step 4: Preserved Files ---")
    preserved_files = [
        config.DOWNLOAD_DIR / "submissions.zip",
        config.DOWNLOAD_DIR / "companyfacts.zip",
        config.DOWNLOAD_DIR / "company_tickers.json"
    ]
    
    for file_path in preserved_files:
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"✓ Keeping {file_path.name}: {size_mb:.1f} MB")
        else:
            logger.info(f"  {file_path.name}: Not found")
    
    # Step 5: Summary
    logger.info("\n" + "=" * 80)
    logger.info("RESET COMPLETE")
    logger.info("=" * 80)
    logger.info("\nWhat was cleared:")
    logger.info("  ✓ EDGAR database tables (companies, filings, xbrl_facts, etc.)")
    logger.info("  ✓ Parquet data files")
    logger.info("  ✓ Extracted JSON files")
    logger.info("\nWhat was preserved:")
    logger.info("  ✓ Stock history data")
    logger.info("  ✓ Macro economic data")
    logger.info("  ✓ Market risk factors")
    logger.info("  ✓ Downloaded ZIP archives (submissions.zip, companyfacts.zip)")
    logger.info("\nNext steps:")
    logger.info("  1. Run: python main.py extract")
    logger.info("  2. Run: python main.py parse-to-parquet")
    logger.info("  3. Run: python main.py load")
    logger.info("=" * 80)
    
    return True


if __name__ == "__main__":
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        print(f"Configuration failed: {e}")
        sys.exit(1)
    
    # Set up logging
    logger = setup_logging("edgar_reset", Path(__file__).parent / "logs")
    
    # Confirm with user
    print("\n" + "!" * 80)
    print("WARNING: EDGAR PIPELINE RESET")
    print("!" * 80)
    print("\nThis will DELETE:")
    print("  - All EDGAR database tables (companies, filings, xbrl_facts, etc.)")
    print("  - All parquet data files")
    print("  - All extracted JSON files")
    print("\nThis will PRESERVE:")
    print("  - Stock history data (599,020 rows)")
    print("  - Macro economic data (47,368 rows)")
    print("  - Market risk factors (16,414 rows)")
    print("  - Downloaded ZIP archives (submissions.zip, companyfacts.zip)")
    print("!" * 80)
    
    response = input("\nAre you sure you want to continue? Type 'YES' to confirm: ")
    
    if response.strip() == "YES":
        logger.info("User confirmed reset operation")
        success = reset_edgar_pipeline(config, logger)
        sys.exit(0 if success else 1)
    else:
        logger.info("Reset operation cancelled by user")
        print("\nReset cancelled. No changes made.")
        sys.exit(0)
