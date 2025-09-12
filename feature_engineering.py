# -*- coding: utf-8 -*-
"""
Feature Engineering Script

This script uses the AnalysisClient to connect to the EDGAR database,
extracts raw financial facts, and computes derived features (e.g., financial ratios)
suitable for machine learning models.
"""

import sys
import logging
from pathlib import Path
import pandas as pd

# --- Import Utilities ---
from config_utils import AppConfig
from logging_utils import setup_logging
from edgar_analysis_functions import AnalysisClient

def create_financial_ratios(client: AnalysisClient, cik: str) -> pd.DataFrame:
    """
    Example function to calculate financial ratios for a given company.

    Args:
        client: An instance of the AnalysisClient.
        cik: The CIK of the company to analyze.

    Returns:
        A pandas DataFrame with calculated ratios over time.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Calculating financial ratios for CIK: {cik}")

    # Example: Calculate Debt-to-Equity Ratio
    # D/E = Total Liabilities / Stockholders' Equity
    required_tags = [
        'Liabilities', # Often used for Total Liabilities, but can be ambiguous
        'StockholdersEquity',
    ]

    # Fetch the relevant financial facts from 10-K (annual) and 10-Q (quarterly) forms
    facts_df = client.get_financial_facts(cik, required_tags, forms=['10-K', '10-Q'])

    if facts_df.empty:
        logger.warning(f"No required financial facts found for CIK {cik}. Cannot calculate ratios.")
        return pd.DataFrame()

    # Pivot the table to have tags as columns for easier calculation
    pivoted_df = facts_df.pivot_table(
        index=['period_end_date', 'filed_date', 'form'],
        columns='tag_name',
        values='value_numeric'
    ).reset_index()

    # Calculate the ratio
    # Note: This is a simplified example. Real-world accounting requires more robust tag selection.
    if 'Liabilities' in pivoted_df.columns and 'StockholdersEquity' in pivoted_df.columns:
        pivoted_df['debt_to_equity'] = pivoted_df['Liabilities'] / pivoted_df['StockholdersEquity']
        logger.info(f"Successfully calculated Debt-to-Equity ratio for {len(pivoted_df)} periods.")
    else:
        logger.warning("Could not find 'Liabilities' or 'StockholdersEquity' in the pivoted data. Skipping D/E ratio.")
        pivoted_df['debt_to_equity'] = None

    return pivoted_df

def create_combined_features(client: AnalysisClient, cik: str) -> pd.DataFrame:
    """
    Demonstrates how to combine company financial facts with macro and market data.

    Args:
        client: An instance of the AnalysisClient.
        cik: The CIK of the company to analyze.

    Returns:
        A pandas DataFrame with company facts enriched with contextual data.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating combined feature set for CIK: {cik}")

    # 1. Get base company financial data (e.g., Revenue and Net Income for 10-K filings)
    company_facts = client.get_financial_facts(cik, ['Revenues', 'NetIncomeLoss'], forms=['10-K'])
    if company_facts.empty:
        logger.warning(f"No base financial facts found for CIK {cik}.")
        return pd.DataFrame()

    # Ensure date columns are in the correct format and sort
    company_facts['period_end_date'] = pd.to_datetime(company_facts['period_end_date'])
    company_facts = company_facts.sort_values('period_end_date')
    logger.info(f"Found {len(company_facts)} base financial facts.")

    # 2. Get Macro and Market data for the relevant time range
    min_date = company_facts['period_end_date'].min().strftime('%Y-%m-%d')
    max_date = company_facts['period_end_date'].max().strftime('%Y-%m-%d')

    macro_series = ['GDP', 'CPIAUCSL', 'UNRATE']
    macro_data = client.get_macro_data(macro_series, start_date=min_date, end_date=max_date)
    market_risk_data = client.get_market_risk_data(start_date=min_date, end_date=max_date)

    if macro_data.empty or market_risk_data.empty:
        logger.warning("Could not retrieve macro or market risk data for the required date range.")
        return company_facts # Return base facts if context is missing

    # 3. Combine the datasets using a merge_asof
    # This is perfect for joining time-series data. It finds the most recent
    # macro/market data point for each financial report date.

    # First, merge macro data onto the company facts
    logger.info("Merging macroeconomic data...")
    combined_df = pd.merge_asof(
        left=company_facts,
        right=macro_data,
        left_on='period_end_date',
        right_index=True,
        direction='backward' # Use the last available macro data point on or before the report date
    )

    # Next, merge market risk data onto the result
    logger.info("Merging market risk data...")
    combined_df = pd.merge_asof(left=combined_df, right=market_risk_data, left_on='period_end_date', right_index=True, direction='backward')

    return combined_df

def main():
    """Main execution function."""
    SCRIPT_NAME = Path(__file__).stem
    LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)

    try:
        config = AppConfig(calling_script_path=Path(__file__))
        client = AnalysisClient(config.DB_FILE_STR)

        if not client.conn:
            raise ConnectionError("Failed to connect to the database via AnalysisClient.")

        # --- Example Usage ---
        # Get ratios for a well-known company, e.g., Microsoft (CIK: 0000789019)
        example_cik = "0000789019" # Microsoft
        
        # --- Example 1: Simple Ratios ---
        # ratios_df = create_financial_ratios(client, example_cik)
        # if not ratios_df.empty:
        #     logger.info(f"\n--- Sample Ratios for CIK {example_cik} ---\n{ratios_df.tail().to_string()}")

        # --- Example 2: Combined Features ---
        combined_df = create_combined_features(client, example_cik)
        if not combined_df.empty:
            logger.info(f"\n--- Sample of Combined Features for CIK {example_cik} ---\n{combined_df.tail().to_string()}")
    except Exception as e:
        logger.critical(f"An error occurred in the feature engineering script: {e}", exc_info=True)
    finally:
        if 'client' in locals() and client: client.close()
        logger.info(f"--- {SCRIPT_NAME} Finished ---")

if __name__ == "__main__":
    main()