# -*- coding: utf-8 -*-
"""
Yahoo Finance Company Information Gatherer.

Fetches and stores supplementary company data from yfinance, such as
profile metrics, recommendations, and major holders. This data complements
the historical stock prices and SEC filings.

Uses:
- config_utils.AppConfig for loading configuration.
- logging_utils.setup_logging for standardized logging.
- database_conn.ManagedDatabaseConnection for DB connection management.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# --- Import Utilities ---
from config_utils import AppConfig
from logging_utils import setup_logging
from database_conn import ManagedDatabaseConnection

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

# --- Constants ---
DEFAULT_REQUEST_DELAY = 0.05 # Default 50ms delay
DEFAULT_MAX_WORKERS = 10 # Default number of concurrent workers

# --- Database Schema ---
YF_TABLES_SCHEMA = {
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
            error_message VARCHAR
        );""",
    "yf_income_statement": """
        CREATE TABLE IF NOT EXISTS yf_income_statement (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
    "yf_balance_sheet": """
        CREATE TABLE IF NOT EXISTS yf_balance_sheet (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
    "yf_cash_flow": """
        CREATE TABLE IF NOT EXISTS yf_cash_flow (
            ticker VARCHAR NOT NULL COLLATE NOCASE,
            report_date DATE NOT NULL,
            item_name VARCHAR NOT NULL,
            item_value BIGINT,
            PRIMARY KEY (ticker, report_date, item_name)
        );""",
}

def setup_yf_tables(con):
    """Creates all yfinance info tables."""
    logger.info("Setting up yfinance info tables...")
    for table_name, sql in YF_TABLES_SCHEMA.items():
        try:
            con.execute(sql)
            logger.debug(f"Table '{table_name}' created or exists.")
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}", exc_info=True)
            raise
    logger.info("All yfinance info tables are set up.")

def get_tickers_to_process(con):
    """Gets a list of all unique tickers from the 'tickers' table."""
    logger.info("Querying for unique tickers to process...")
    try:
        tickers_df = con.sql("SELECT DISTINCT ticker FROM tickers ORDER BY ticker").df()
        tickers = tickers_df['ticker'].tolist()
        logger.info(f"Found {len(tickers)} unique tickers to process.")
        return tickers
    except Exception as e:
        logger.error(f"Failed to query tickers: {e}", exc_info=True)
        return []

def log_fetch_error(con, ticker: str, message: str):
    """Logs a fetch error to the database."""
    timestamp = datetime.now(timezone.utc)
    logger.warning(f"Logging error for ticker {ticker}: {message}")
    try:
        con.execute(
            "INSERT INTO yf_info_fetch_errors VALUES (?, ?, ?)",
            [ticker, timestamp, message]
        )
    except Exception as db_e:
        logger.error(f"Could not log error for ticker {ticker} to DB: {db_e}")

def _process_financial_statement(statement_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Helper function to transpose and melt a yfinance financial statement DataFrame."""
    if statement_df is None or statement_df.empty:
        return pd.DataFrame()
    try:
        statement_df = statement_df.transpose() # Dates become columns, items become rows
        statement_df.reset_index(inplace=True)
        statement_df.rename(columns={'index': 'item_name'}, inplace=True)
        melted_df = statement_df.melt(id_vars=['item_name'], var_name='report_date', value_name='item_value')
        melted_df['ticker'] = ticker
        melted_df['report_date'] = pd.to_datetime(melted_df['report_date']).dt.date
        melted_df.dropna(subset=['item_value'], inplace=True)
        return melted_df[['ticker', 'report_date', 'item_name', 'item_value']]
    except Exception as e:
        logger.warning(f"Could not process financial statement for {ticker}: {e}")
        return pd.DataFrame()

def fetch_ticker_data(ticker_str: str, delay: float) -> dict:
    """
    Fetches all info for a single ticker from yfinance.
    This function is designed to be run in a separate thread and does not touch the database.
    """
    fetch_timestamp = datetime.now(timezone.utc)
    if delay > 0:
        time.sleep(delay)

    try:
        ticker = yf.Ticker(ticker_str)
        
        # --- 1. Profile Metrics (ticker.info) ---
        info = ticker.info
        if info and info.get('trailingPE'): # Use a key that indicates valid data
            profile_data = {
                'ticker': ticker_str, 'fetch_timestamp': fetch_timestamp,
                'cik': info.get('cik'), 'sector': info.get('sector'), 'industry': info.get('industry'),
                'country': info.get('country'), 'market_cap': info.get('marketCap'), 'beta': info.get('beta'),
                'trailing_pe': info.get('trailingPE'), 'forward_pe': info.get('forwardPE'),
                'enterprise_value': info.get('enterpriseValue'), 'book_value': info.get('bookValue'),
                'price_to_book': info.get('priceToBook'), 'trailing_eps': info.get('trailingEps'),
                'forward_eps': info.get('forwardEps'), 'peg_ratio': info.get('pegRatio')
            }
            df_profile = pd.DataFrame([profile_data])
        else:
            df_profile = pd.DataFrame()

        # --- 2. Recommendations ---
        recs = ticker.recommendations
        if recs is not None and not recs.empty:
            recs.reset_index(inplace=True)
            recs['ticker'] = ticker_str
            recs.rename(columns={
                'Date': 'recommendation_timestamp', 'Firm': 'firm',
                'From Grade': 'grade_from', 'To Grade': 'grade_to', 'Action': 'action'
            }, inplace=True)
            df_recs = recs[['ticker', 'recommendation_timestamp', 'firm', 'grade_from', 'grade_to', 'action']]
        else:
            df_recs = pd.DataFrame()

        # --- 3. Major Holders ---
        holders = ticker.major_holders
        if holders is not None and not holders.empty:
            # Helper to safely parse percentage strings like '72.55%'
            def parse_pct(val):
                if isinstance(val, str) and '%' in val:
                    try:
                        return float(val.strip('%')) / 100.0
                    except (ValueError, TypeError):
                        return None
                return None
            pct_insiders = parse_pct(holders.iloc[0, 0]) if len(holders) > 0 else None
            pct_institutions = parse_pct(holders.iloc[1, 0]) if len(holders) > 1 else None
            holders_data = {
                'ticker': ticker_str, 'fetch_timestamp': fetch_timestamp,
                'pct_insiders': pct_insiders, 'pct_institutions': pct_institutions
            }
            df_holders = pd.DataFrame([holders_data])
        else:
            df_holders = pd.DataFrame()
        
        # --- 4. Stock Actions (Dividends & Splits) ---
        actions = ticker.actions
        if actions is not None and not actions.empty:
            actions.reset_index(inplace=True)
            actions['ticker'] = ticker_str
            # Melt to create action_type and value columns
            actions_melted = actions.melt(id_vars=['Date', 'ticker'], value_vars=['Dividends', 'Stock Splits'],
                                          var_name='action_type', value_name='value')
            actions_final = actions_melted[actions_melted['value'] != 0].copy()
            actions_final.rename(columns={'Date': 'action_date'}, inplace=True)
            if not actions_final.empty:
                df_actions = actions_final[['ticker', 'action_date', 'action_type', 'value']]
            else:
                df_actions = pd.DataFrame()
        else:
            df_actions = pd.DataFrame()

        # --- 5. Financial Statements ---
        df_income = _process_financial_statement(ticker.income_stmt, ticker_str)
        df_balance = _process_financial_statement(ticker.balance_sheet, ticker_str)
        df_cashflow = _process_financial_statement(ticker.cashflow, ticker_str)

        return {
            'status': 'success', 'ticker': ticker_str,
            'profile': df_profile, 'recs': df_recs,
            'holders': df_holders, 'actions': df_actions,
            'income': df_income, 'balance': df_balance, 'cashflow': df_cashflow
        }

    except Exception as e:
        e_str = str(e).lower()
        if "404" in e_str or "no data found" in e_str:
            error_msg = f"Ticker {ticker_str} not found on yfinance (404)."
            logger.debug(error_msg)
        elif "429" in e_str or "too many requests" in e_str:
            error_msg = f"Rate limit hit for {ticker_str} (429). Consider increasing YFINANCE_REQUEST_DELAY."
            logger.warning(error_msg)
        else:
            error_msg = f"yfinance info fetch failed for {ticker_str}: {type(e).__name__} - {e}"
            logger.warning(error_msg)
        return {'status': 'error', 'ticker': ticker_str, 'message': error_msg}

def bulk_load_data(con, all_results: list):
    """Concatenates and bulk-loads all fetched data into the database."""
    if not all_results:
        logger.info("No data to load.")
        return

    logger.info(f"Preparing to bulk-load data for {len(all_results)} successfully fetched tickers.")

    # Aggregate data from all results
    all_profiles = pd.concat([r['profile'] for r in all_results if not r['profile'].empty], ignore_index=True)
    all_recs = pd.concat([r['recs'] for r in all_results if not r['recs'].empty], ignore_index=True)
    all_holders = pd.concat([r['holders'] for r in all_results if not r['holders'].empty], ignore_index=True)
    all_actions = pd.concat([r['actions'] for r in all_results if not r['actions'].empty], ignore_index=True)
    all_income = pd.concat([r['income'] for r in all_results if not r['income'].empty], ignore_index=True)
    all_balance = pd.concat([r['balance'] for r in all_results if not r['balance'].empty], ignore_index=True)
    all_cashflow = pd.concat([r['cashflow'] for r in all_results if not r['cashflow'].empty], ignore_index=True)

    try:
        con.begin()
        logger.info("--- Starting Bulk Load Transaction ---")

        # Load Profiles
        if not all_profiles.empty:
            logger.info(f"Loading {len(all_profiles)} profile metrics records...")
            con.execute("""
                INSERT INTO yf_profile_metrics SELECT * FROM all_profiles
                ON CONFLICT (ticker) DO UPDATE SET
                fetch_timestamp = excluded.fetch_timestamp, cik = excluded.cik, sector = excluded.sector,
                industry = excluded.industry, country = excluded.country, market_cap = excluded.market_cap,
                beta = excluded.beta, trailing_pe = excluded.trailing_pe, forward_pe = excluded.forward_pe,
                enterprise_value = excluded.enterprise_value, book_value = excluded.book_value,
                price_to_book = excluded.price_to_book, trailing_eps = excluded.trailing_eps,
                forward_eps = excluded.forward_eps, peg_ratio = excluded.peg_ratio;
            """)

        # Load Recommendations
        if not all_recs.empty:
            logger.info(f"Loading {len(all_recs)} recommendation records...")
            con.execute("INSERT INTO yf_recommendations SELECT * FROM all_recs ON CONFLICT DO NOTHING")

        # Load Major Holders
        if not all_holders.empty:
            logger.info(f"Loading {len(all_holders)} major holder records...")
            con.execute("""
                INSERT INTO yf_major_holders SELECT * FROM all_holders
                ON CONFLICT (ticker) DO UPDATE SET
                fetch_timestamp = excluded.fetch_timestamp, pct_insiders = excluded.pct_insiders,
                pct_institutions = excluded.pct_institutions;
            """)

        # Load Stock Actions
        if not all_actions.empty:
            logger.info(f"Loading {len(all_actions)} stock action records...")
            con.execute("INSERT INTO yf_stock_actions SELECT * FROM all_actions ON CONFLICT DO NOTHING")

        # Load Financials
        if not all_income.empty:
            logger.info(f"Loading {len(all_income)} income statement records...")
            con.execute("INSERT OR REPLACE INTO yf_income_statement SELECT * FROM all_income")

        if not all_balance.empty:
            logger.info(f"Loading {len(all_balance)} balance sheet records...")
            con.execute("INSERT OR REPLACE INTO yf_balance_sheet SELECT * FROM all_balance")

        if not all_cashflow.empty:
            logger.info(f"Loading {len(all_cashflow)} cash flow statement records...")
            con.execute("INSERT OR REPLACE INTO yf_cash_flow SELECT * FROM all_cashflow")

        con.commit()
        logger.info("--- Bulk Load Transaction Committed Successfully ---")

    except Exception as e:
        logger.error(f"Error during bulk load transaction: {e}. Rolling back.", exc_info=True)
        try:
            con.rollback()
        except Exception as rb_e:
            logger.error(f"Failed to rollback transaction: {rb_e}")

def run_info_gathering_pipeline(config: AppConfig):
    """Main orchestration function for the info gathering pipeline."""
    logger.info("--- Starting Yahoo Finance Info Gathering Pipeline ---")
    start_time = time.time()
    max_workers = config.get_optional_int("YFINANCE_MAX_WORKERS", DEFAULT_MAX_WORKERS)
    request_delay = config.get_optional_float("YFINANCE_REQUEST_DELAY", DEFAULT_REQUEST_DELAY)
    logger.info(f"Using up to {max_workers} concurrent workers.")
    logger.info(f"Request Delay: {request_delay}s")

    # Define PRAGMA settings for write-heavy operations
    write_pragmas = {
        'threads': os.cpu_count(),
        'memory_limit': '4GB'
    }
    if config.DUCKDB_TEMP_DIR:
        write_pragmas['temp_directory'] = f"'{config.DUCKDB_TEMP_DIR}'"

    try:
        with ManagedDatabaseConnection(db_path_override=config.DB_FILE_STR, read_only=False, pragma_settings=write_pragmas) as con:
            if not con:
                logger.critical("Database connection failed. Exiting.")
                return

            # 1. Setup tables
            setup_yf_tables(con)

            # 2. Get tickers to process
            tickers = get_tickers_to_process(con)
            if not tickers:
                logger.warning("No tickers found in the database to process.")
                return

            # 3. Process tickers concurrently
            success_count = 0
            error_count = 0
            all_successful_results = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all jobs to the executor
                future_to_ticker = {executor.submit(fetch_ticker_data, ticker, request_delay): ticker for ticker in tickers}

                # Process results as they complete
                progress = tqdm(as_completed(future_to_ticker), total=len(tickers), desc="Gathering Company Info")
                for future in progress:
                    ticker_str = future_to_ticker[future]
                    try:
                        result = future.result()
                        if result['status'] == 'success':
                            all_successful_results.append(result)
                            success_count += 1
                        else:
                            log_fetch_error(con, result['ticker'], result['message'])
                            error_count += 1
                    except Exception as exc:
                        log_fetch_error(con, ticker_str, f"Unhandled exception in worker: {exc}")
                        error_count += 1
            
            logger.info(f"Finished concurrent fetching. Success: {success_count}, Errors: {error_count}")

            # 4. Bulk load all collected data
            if all_successful_results:
                bulk_load_data(con, all_successful_results)

    except Exception as pipeline_e:
        logger.critical(f"A critical error occurred in the pipeline: {pipeline_e}", exc_info=True)

    end_time = time.time()
    logger.info("--- Yahoo Finance Info Gathering Pipeline Finished ---")
    logger.info(f"Total Time: {end_time - start_time:.2f} seconds")
    logger.info(f"Successfully fetched: {success_count} tickers")
    logger.info(f"Errors logged: {error_count} tickers")
    logger.info("Check 'yf_info_fetch_errors' table for details on failures.")

if __name__ == "__main__":
    try:
        app_config = AppConfig(calling_script_path=Path(__file__))
        run_info_gathering_pipeline(app_config)
    except SystemExit as e:
        logger.critical(f"Configuration failed, cannot start: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred at startup: {e}", exc_info=True)
        sys.exit(1)