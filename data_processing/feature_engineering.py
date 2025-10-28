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

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

# --- Import Utilities ---
from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from analysis.edgar_analysis_functions import AnalysisClient

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
        msft_cik = "0000789019"
        ratios_df = create_financial_ratios(client, msft_cik)
        if not ratios_df.empty:
            logger.info(f"\n--- Sample Ratios for CIK {msft_cik} ---\n{ratios_df.tail().to_string()}")

    except Exception as e:
        logger.critical(f"An error occurred in the feature engineering script: {e}", exc_info=True)
    finally:
        if 'client' in locals() and client: client.close()
        logger.info(f"--- {SCRIPT_NAME} Finished ---")

if __name__ == "__main__":
    main()