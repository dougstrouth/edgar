"""
Script to identify and remove duplicate (ticker, date) rows in stock_history table.
Keeps the first row for each (ticker, date) pair.
"""
import sys
from pathlib import Path
# Add project root to sys.path for module resolution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
import duckdb
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
import logging

app_config = AppConfig()
db_path = app_config.DB_FILE_STR
logger = setup_logging("dedupe_stock_history", Path("logs"), level=logging.INFO)

with duckdb.connect(db_path) as con:
    # Find duplicates
    dupes = con.execute('''
        SELECT ticker, date, COUNT(*) as cnt
        FROM stock_history
        GROUP BY ticker, date
        HAVING cnt > 1
    ''').fetchdf()
    logger.info(f"Found {len(dupes)} duplicate (ticker, date) pairs.")
    if len(dupes) == 0:
        logger.info("No duplicates found. Exiting.")
        exit(0)

    # Remove all but the first row for each duplicate
    con.execute('''
        DELETE FROM stock_history
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM stock_history
            GROUP BY ticker, date
        );
    ''')
    logger.info("Duplicates removed. Only one row per (ticker, date) remains.")
