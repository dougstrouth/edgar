# -*- coding: utf-8 -*-
"""
Supplementary Data Loader (Parquet -> DuckDB)

Implements two loading strategies selected per table:

1. Snapshot Blue-Green Swap (e.g. `macro_economic_data`, `market_risk_factors`):
     - Entire historical dataset is regenerated each gather cycle.
     - Data loads into `<table>_new` staging; after successful insert a transaction
         atomically replaces the live table via `DROP` + `ALTER TABLE RENAME`.
     - Provides point-in-time consistency; partial failures never surface to readers.

2. Incremental Drip-Feed Upsert (e.g. `stock_history`, `stock_fetch_errors`,
     `yf_stock_actions`, `yf_recommendations`, `yf_info_fetch_errors`):
     - New batches are merged using a temporary `<table>_batch` staging table.
     - `INSERT OR REPLACE` semantics require PRIMARY KEYs to deterministically
         update existing rows while retaining untouched history.
     - Avoids cost and risk of full table rebuild for append-only / event logs.

Classification is defined in `INCREMENTAL_TABLES`. Tables absent from that set
default to snapshot mode unless `--full-refresh` overrides behavior.

See `SUPPLEMENTARY_LOADING_METHOD.md` for rationale, safety guarantees, and
future enhancement notes (audit lineage, empty-swap guards, etc.).
"""

import sys
import logging
import argparse
from pathlib import Path

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection

# Define the schemas for the supplementary tables.
SUPPLEMENTARY_SCHEMAS = {
    "stock_history": """
        CREATE TABLE IF NOT EXISTS stock_history (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            date DATE NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            adj_close DOUBLE, volume BIGINT,
            PRIMARY KEY (ticker, date)
        );""",
    "stock_fetch_errors": """
        CREATE TABLE IF NOT EXISTS stock_fetch_errors (
            cik VARCHAR,
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            error_timestamp TIMESTAMPTZ NOT NULL,
            error_type VARCHAR NOT NULL,
            error_message VARCHAR,
            start_date_req DATE,
            end_date_req DATE,
            PRIMARY KEY (ticker, error_timestamp, error_type)
        );""",
    "yf_profile_metrics": """
        CREATE TABLE IF NOT EXISTS yf_profile_metrics (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            fetch_timestamp TIMESTAMPTZ NOT NULL,
            cik VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            country VARCHAR,
            market_cap BIGINT,
            beta DOUBLE,
            trailing_pe DOUBLE,
            forward_pe DOUBLE,
            enterprise_value BIGINT,
            book_value DOUBLE,
            price_to_book DOUBLE,
            trailing_eps DOUBLE,
            forward_eps DOUBLE,
            peg_ratio DOUBLE,
            PRIMARY KEY (ticker)
        );""",
    "yf_recommendations": """
        CREATE TABLE IF NOT EXISTS yf_recommendations (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            recommendation_timestamp TIMESTAMPTZ NOT NULL,
            firm VARCHAR NOT NULL,
            grade_from VARCHAR,
            grade_to VARCHAR,
            action VARCHAR,
            PRIMARY KEY (ticker, recommendation_timestamp, firm)
        );""",
    "yf_major_holders": """
        CREATE TABLE IF NOT EXISTS yf_major_holders (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            fetch_timestamp TIMESTAMPTZ NOT NULL,
            pct_insiders DOUBLE,
            pct_institutions DOUBLE,
            PRIMARY KEY (ticker)
        );""",
    "yf_stock_actions": """
        CREATE TABLE IF NOT EXISTS yf_stock_actions (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            action_date DATE NOT NULL,
            action_type VARCHAR NOT NULL,
            value DOUBLE,
            PRIMARY KEY (ticker, action_date, action_type)
        );""",
    "yf_info_fetch_errors": """
        CREATE TABLE IF NOT EXISTS yf_info_fetch_errors (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            error_timestamp TIMESTAMPTZ NOT NULL,
            error_message VARCHAR,
            PRIMARY KEY (ticker, error_timestamp)
        );""",
    "yf_income_statement": """
        CREATE TABLE IF NOT EXISTS yf_income_statement (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
    "yf_balance_sheet": "CREATE TABLE IF NOT EXISTS yf_balance_sheet (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, item_name VARCHAR NOT NULL, item_value BIGINT, PRIMARY KEY (ticker, report_date, item_name));",
    "yf_cash_flow": "CREATE TABLE IF NOT EXISTS yf_cash_flow (ticker VARCHAR NOT NULL COLLATE NOCASE, report_date DATE NOT NULL, item_name VARCHAR NOT NULL, item_value BIGINT, PRIMARY KEY (ticker, report_date, item_name));",
    "yf_untrackable_tickers": """
        CREATE TABLE IF NOT EXISTS yf_untrackable_tickers (
            ticker VARCHAR NOT NULL COLLATE NOCASE PRIMARY KEY,
            reason VARCHAR,
            last_failed_timestamp TIMESTAMPTZ
        );
    """,
    "macro_economic_data": """
        CREATE TABLE IF NOT EXISTS macro_economic_data (
            series_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            value DOUBLE,
            PRIMARY KEY (series_id, date)
        );
    """,
    "market_risk_factors": """
        CREATE TABLE IF NOT EXISTS market_risk_factors (
            date DATE NOT NULL,
            factor_model VARCHAR NOT NULL,
            mkt_minus_rf DOUBLE,
            smb DOUBLE,
            hml DOUBLE,
            rmw DOUBLE,
            cma DOUBLE,
            rf DOUBLE,
            PRIMARY KEY (date, factor_model)
        );
    """,
    # Add schemas for yf_info tables here as they are created
}

# Tables that receive incremental drip-feed data; we upsert new batches into existing table
# rather than swapping out entire historical dataset.
INCREMENTAL_TABLES = {
    "stock_history",           # Daily bars appended over time
    "stock_fetch_errors",      # Error events log
    "yf_stock_actions",        # Corporate actions accumulate
    "yf_recommendations",      # Analyst recommendations accumulate
    "yf_info_fetch_errors"     # Info fetch errors log
}

def load_data(config: AppConfig, logger: logging.Logger, source_name: str, full_refresh: bool = False):
    """
    Loads data from a specific Parquet source into the DuckDB database.

    Args:
        config: The application configuration.
        logger: The logger instance.
        source_name: The name of the data source (e.g., 'stock_history'),
                     which corresponds to the Parquet subdirectory.
        full_refresh: If True, drops the table before loading.
    """
    parquet_path = config.PARQUET_DIR / source_name
    create_sql = SUPPLEMENTARY_SCHEMAS.get(source_name)

    if not create_sql:
        logger.error(f"No schema defined for source '{source_name}'. Aborting.")
        return

    if not parquet_path.exists() or not any(parquet_path.glob('*.parquet')):
        logger.warning(f"Parquet directory for source '{source_name}' not found or is empty. Nothing to load.")
        return

    logger.info(f"--- Starting data load for '{source_name}' ---")
    logger.info(f"Source Parquet Path: {parquet_path}")

    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False) as con:
            if not con:
                logger.critical("Database connection failed. Aborting load.")
                return

            if source_name in INCREMENTAL_TABLES and not full_refresh:
                # Incremental upsert path: maintain existing history, only merge new batch
                logger.info(f"INCREMENTAL: Ensuring base table '{source_name}' exists...")
                
                # Check if table exists and has proper constraints
                table_exists_row = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [source_name]
                ).fetchone()
                table_exists = (table_exists_row is not None and table_exists_row[0] == 1)
                
                if table_exists:
                    # Verify the table has the required primary key
                    # If not, we need to recreate it (this handles legacy tables without PKs)
                    try:
                        # Try a test INSERT OR REPLACE to see if PK exists
                        con.execute("BEGIN TRANSACTION;")
                        con.execute(f"CREATE TEMP TABLE _pk_test AS SELECT * FROM {source_name} LIMIT 0;")
                        # Try to get constraint info
                        constraint_check = con.execute(
                            f"SELECT constraint_name FROM information_schema.table_constraints "
                            f"WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
                            [source_name]
                        ).fetchall()
                        con.execute("ROLLBACK;")
                        
                        if not constraint_check:
                            logger.warning(f"Table '{source_name}' exists but lacks PRIMARY KEY. Recreating...")
                            # Save existing data, drop table, recreate with PK, restore data
                            temp_backup = f"{source_name}_backup_for_pk"
                            con.execute(f"CREATE TABLE {temp_backup} AS SELECT * FROM {source_name};")
                            count_row = con.execute(f"SELECT COUNT(*) FROM {temp_backup};").fetchone()
                            existing_count = count_row[0] if count_row else 0
                            logger.info(f"Backed up {existing_count} existing rows to {temp_backup}")
                            con.execute(f"DROP TABLE {source_name};")
                            con.execute(create_sql)
                            if existing_count > 0:
                                con.execute(f"INSERT OR REPLACE INTO {source_name} SELECT * FROM {temp_backup};")
                                logger.info(f"Restored {existing_count} rows with PRIMARY KEY constraint")
                            con.execute(f"DROP TABLE {temp_backup};")
                    except Exception as e:
                        logger.warning(f"Could not verify PRIMARY KEY (non-critical): {e}")
                        # Fallback: just create if not exists
                        con.execute(create_sql)
                else:
                    # Table doesn't exist, create it fresh
                    con.execute(create_sql)
                
                table_new = f"{source_name}_batch"
                logger.info(f"INCREMENTAL: Creating staging batch table '{table_new}'...")
                con.execute(create_sql.replace(f"{source_name}", table_new))
                insert_sql = f"INSERT OR REPLACE INTO {table_new} SELECT * FROM read_parquet('{str(parquet_path / '*.parquet')}');"
                con.execute(insert_sql)
                _row = con.execute(f"SELECT COUNT(*) FROM {table_new};").fetchone()
                batch_count = _row[0] if _row else 0
                if batch_count == 0:
                    logger.warning(f"Batch staging table '{table_new}' is empty; skipping merge.")
                else:
                    logger.info(f"INCREMENTAL: Merging {batch_count} rows into '{source_name}' via upsert...")
                    con.execute(f"INSERT OR REPLACE INTO {source_name} SELECT * FROM {table_new};")
                con.execute(f"DROP TABLE IF EXISTS {table_new};")
                logger.info(f"INCREMENTAL: Upsert complete for '{source_name}'.")
            else:
                # Blue-Green full replacement path (macro, market risk, or explicit full_refresh)
                table_new = f"{source_name}_new"
                mode_label = "FULL REFRESH" if full_refresh else "BLUE-GREEN"
                logger.info(f"{mode_label}: Creating staging table '{table_new}' with schema and loading Parquet data...")
                con.execute(create_sql.replace(f"{source_name}", table_new))
                insert_sql = f"INSERT OR REPLACE INTO {table_new} SELECT * FROM read_parquet('{str(parquet_path / '*.parquet')}');"
                con.execute(insert_sql)
                _row = con.execute(f"SELECT COUNT(*) FROM {table_new};").fetchone()
                count_new = _row[0] if _row else 0
                # Determine existing table row count (if present) for swap guard
                existing_count = 0
                _tbl_row = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [source_name]
                ).fetchone()
                table_exists = (_tbl_row is not None and _tbl_row[0] == 1)
                if table_exists:
                    existing_row = con.execute(f"SELECT COUNT(*) FROM {source_name};").fetchone()
                    existing_count = existing_row[0] if existing_row else 0

                # Guard: prevent swap if staging is empty OR smaller than existing
                if existing_count > 0 and count_new < existing_count:
                    logger.error(
                        f"SWAP GUARD: Aborting {mode_label.lower()} swap; staging count {count_new} < existing count {existing_count}."
                    )
                    con.execute(f"DROP TABLE IF EXISTS {table_new};")
                    logger.info(f"Retained original '{source_name}' ({existing_count} rows).")
                    return
                if count_new == 0 and existing_count == 0:
                    logger.warning(
                        f"Staging table '{table_new}' empty and no existing '{source_name}'. Creating empty table (initial load)."
                    )
                elif count_new == 0 and existing_count > 0:
                    logger.error(
                        f"SWAP GUARD: Aborting swap; staging '{table_new}' empty while existing '{source_name}' has {existing_count} rows."
                    )
                    con.execute(f"DROP TABLE IF EXISTS {table_new};")
                    logger.info(f"Retained original '{source_name}'.")
                    return
                logger.info(f"{mode_label}: Atomically swapping '{source_name}' with staging table '{table_new}'...")
                con.begin()
                try:
                    con.execute(f"DROP TABLE IF EXISTS {source_name};")
                    con.execute(f"ALTER TABLE {table_new} RENAME TO {source_name};")
                    con.commit()
                    logger.info(f"{mode_label}: Atomic swap for '{source_name}' complete.")
                except Exception as swap_e:
                    logger.error(f"ATOMIC SWAP FAILED for '{source_name}'. Rolling back.", exc_info=True)
                    con.rollback()
                    raise swap_e
                # Create supporting unique index AFTER swap to avoid rename dependency issues
                if source_name == 'macro_economic_data':
                    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS macro_economic_data_series_date_idx ON "
                                f"{source_name}(series_id, date);")
                if source_name == 'market_risk_factors':
                    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS market_risk_factors_date_model_idx ON "
                                f"{source_name}(date, factor_model);")
                logger.info(f"Successfully loaded data for '{source_name}'.")

    except Exception as e:
        logger.critical(f"An error occurred during the load process for '{source_name}': {e}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load supplementary data from Parquet to DuckDB.")
    # Allow multiple sources to be specified
    parser.add_argument(
        "source",
        nargs='+', # Accept one or more source names
        choices=SUPPLEMENTARY_SCHEMAS.keys(),
        help="The name of the data source to load (must match a key in SUPPLEMENTARY_SCHEMAS)."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop the target table before loading data, ensuring a clean slate. "
             "Use this after running a gatherer in full_refresh mode."
    )
    args = parser.parse_args()

    # Handle the case where 'all_yf' is passed
    sources_to_load = args.source
    if 'all_yf' in sources_to_load:
        # Replace 'all_yf' with the actual list of yf tables, excluding the stock history ones
        sources_to_load = [s for s in SUPPLEMENTARY_SCHEMAS.keys() if s.startswith('yf_')]

    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logging.getLogger().critical(f"Configuration failed: {e}")
        sys.exit(1)

    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    main_logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    for source_name in sources_to_load:
        load_data(app_config, main_logger, source_name, full_refresh=args.full_refresh)

    main_logger.info(f"--- Supplementary Data Loader Finished for source(s): {', '.join(sources_to_load)} ---")