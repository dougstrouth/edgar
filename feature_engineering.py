# -*- coding: utf-8 -*-
"""
Feature Engineering for Financial Modeling

This script uses the AnalysisClient to retrieve clean, validated data and
then transforms it into features suitable for predictive modeling.
"""

import sys
import logging
from pathlib import Path
import pandas as pd

# --- Import Utilities ---
from config_utils import AppConfig
from logging_utils import setup_logging
from edgar_analysis_functions import AnalysisClient

# --- Setup Logging ---
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)


def create_financial_features(client: AnalysisClient, ticker: str) -> pd.DataFrame:
    """
    Creates a feature-rich DataFrame for a given ticker, suitable for modeling.

    Args:
        client: An initialized AnalysisClient instance.
        ticker: The stock ticker symbol to process.

    Returns:
        A pandas DataFrame with engineered features.
    """
    logger.info(f"--- Starting feature engineering for {ticker} ---")
    
    # 1. Get the base financial statements
    # The data is already pivoted with dates as the index.
    financials_df = client.get_financial_statements(ticker)
    if financials_df.empty:
        logger.warning(f"No financial data to process for {ticker}.")
        return pd.DataFrame()

    # Ensure data is sorted by date for time-series calculations
    financials_df.sort_index(ascending=True, inplace=True)

    # 2. Create Lagged Features
    # A lagged feature is the value from the previous period.
    # We will lag key items that might predict future cash or debt.
    items_to_lag = ['Net Income', 'Total Revenue', 'Operating Cash Flow', 'Long Term Debt']
    for item in items_to_lag:
        if item in financials_df.columns:
            financials_df[f'{item}_lag1'] = financials_df[item].shift(1)

    # 3. Create Rolling Averages
    # A rolling average smooths out short-term fluctuations.
    items_to_roll = ['Net Income', 'Total Revenue']
    for item in items_to_roll:
        if item in financials_df.columns:
            # 4-period (e.g., 4-quarter) rolling average
            financials_df[f'{item}_roll4_avg'] = financials_df[item].rolling(window=4).mean()

    # 4. Create Year-over-Year (YoY) Growth Features
    # For quarterly data, a 4-period shift represents the same quarter last year.
    items_for_yoy = ['Total Revenue', 'Net Income']
    for item in items_for_yoy:
        if item in financials_df.columns:
            financials_df[f'{item}_yoy_growth'] = financials_df[item].pct_change(periods=4)

    # 5. Create Financial Ratios
    # Example: Working Capital
    if 'Total Current Assets' in financials_df.columns and 'Total Current Liabilities' in financials_df.columns:
        financials_df['Working_Capital'] = financials_df['Total Current Assets'] - financials_df['Total Current Liabilities']

    logger.info(f"Successfully created {len(financials_df.columns)} total features for {ticker}.")
    
    # Drop rows with NaN values created by lagging/rolling operations
    final_df = financials_df.dropna()
    logger.info(f"Returning {len(final_df)} complete feature rows after dropping NaNs.")
    
    return final_df


if __name__ == "__main__":
    logger.info("--- Running Feature Engineering Script in Example Mode ---")
    client = None
    try:
        config = AppConfig(calling_script_path=Path(__file__))
        client = AnalysisClient(config.DB_FILE_STR)

        if not client.conn:
            raise ConnectionError("Failed to connect to the database.")

        # Example: Create features for Apple Inc.
        example_ticker = "AAPL"
        feature_df = create_financial_features(client, example_ticker)

        if not feature_df.empty:
            print(f"\n--- Sample Features for {example_ticker} ---")
            # Display a subset of original and new features
            display_cols = [col for col in ['Total Revenue', 'Total Revenue_lag1', 'Total Revenue_roll4_avg', 'Total Revenue_yoy_growth', 'Working_Capital'] if col in feature_df.columns]
            print(feature_df[display_cols].tail().to_string())

    except Exception as e:
        logger.critical(f"An error occurred in the example run: {e}", exc_info=True)
    finally:
        if client:
            client.close()