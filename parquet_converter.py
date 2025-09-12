# -*- coding: utf-8 -*-
"""
Parquet Conversion Utility

Provides functions to convert aggregated data into Pandas DataFrames
and save them to the Parquet format, which is highly optimized for
columnar storage and fast loading into systems like DuckDB.
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List, Set, Tuple, Optional

# This module is a utility, so it will use the logger from the calling script.
logger = logging.getLogger(__name__)

def _prepare_df_for_storage(df: pd.DataFrame, str_cols: List[str], date_cols: List[str] = None, numeric_cols: List[str] = None, int_cols: List[str] = None) -> pd.DataFrame:
    """Prepares a DataFrame for Parquet serialization with correct types."""
    df_out = df.copy()
    if str_cols:
        for col in str_cols:
            if col in df_out.columns:
                df_out[col] = df_out[col].apply(lambda x: str(x) if pd.notna(x) else None)
    if date_cols:
        for col in date_cols:
            if col in df_out.columns:
                df_out[col] = pd.to_datetime(df_out[col], errors='coerce')
    if numeric_cols:
        for col in numeric_cols:
             if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
    if int_cols:
         for col in int_cols:
             if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce').astype('Int64')
    return df_out

def save_dataframe_to_parquet(df: pd.DataFrame, dir_path: Path):
    """
    Saves a DataFrame as a new, uniquely named Parquet file within a
    specified directory. This avoids read-modify-write cycles.
    """
    if df.empty:
        return

    try:
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"batch_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        df.to_parquet(file_path, engine='pyarrow', index=False)
        logger.info(f"Successfully saved batch of {len(df)} records to {file_path.name}")

    except Exception as e:
        logger.error(f"Failed to save data to Parquet file {file_path}: {e}", exc_info=True)
        raise

def process_batch_to_parquet(batch_data: Dict[str, List[Dict]], parquet_dir: Path):
    """
    Processes a batch of aggregated data and saves each component to a
    separate Parquet file in the specified directory.
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # --- Companies ---
    if batch_data.get("companies"):
        df_companies = pd.DataFrame(batch_data["companies"])
        str_cols = ['cik', 'primary_name', 'entity_name_cf', 'entity_type', 'sic', 'sic_description', 'ein', 'description', 'category', 'fiscal_year_end', 'state_of_incorporation', 'phone', 'flags', 'mailing_street1', 'mailing_street2', 'mailing_city', 'mailing_state_or_country', 'mailing_zip_code', 'business_street1', 'business_street2', 'business_city', 'business_state_or_country', 'business_zip_code']
        df_companies = _prepare_df_for_storage(df_companies, str_cols=str_cols, date_cols=['last_parsed_timestamp'])
        save_dataframe_to_parquet(df_companies, parquet_dir / "companies")

    # --- Tickers ---
    if batch_data.get("tickers"):
        unique_tickers = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['tickers'] if d.get('cik') and d.get('ticker') and d.get('exchange')}]
        df_tickers = pd.DataFrame(unique_tickers)
        df_tickers = _prepare_df_for_storage(df_tickers, str_cols=['cik', 'ticker', 'exchange', 'source'])
        save_dataframe_to_parquet(df_tickers, parquet_dir / "tickers")

    # --- Former Names ---
    if batch_data.get("former_names"):
        unique_fns = [dict(t) for t in {tuple(sorted(d.items())) for d in batch_data['former_names'] if d.get('cik') and d.get('former_name') and d.get('date_from')}]
        df_fns = pd.DataFrame(unique_fns)
        df_fns = _prepare_df_for_storage(df_fns, str_cols=['cik', 'former_name'], date_cols=['date_from', 'date_to'])
        save_dataframe_to_parquet(df_fns, parquet_dir / "former_names")

    # --- Filings ---
    if batch_data.get("filings"):
        unique_filings = {d['accession_number']: d for d in batch_data['filings'] if d.get('accession_number')}.values()
        df_filings = pd.DataFrame(list(unique_filings))
        str_cols = ['cik', 'accession_number', 'act', 'form', 'file_number', 'film_number', 'items', 'primary_document', 'primary_doc_description']
        date_cols = ['filing_date', 'report_date', 'acceptance_datetime']
        int_cols = ['size']
        df_filings = _prepare_df_for_storage(df_filings, str_cols=str_cols, date_cols=date_cols, int_cols=int_cols)
        save_dataframe_to_parquet(df_filings, parquet_dir / "filings")

    # --- XBRL Tags ---
    if batch_data.get("xbrl_tags"):
        unique_tag_data = { (d.get("taxonomy"), d.get("tag_name")): d for d in batch_data.get("xbrl_tags", []) if all((d.get("taxonomy"), d.get("tag_name"))) }
        df_tags = pd.DataFrame(list(unique_tag_data.values()))
        df_tags = _prepare_df_for_storage(df_tags, str_cols=['taxonomy', 'tag_name', 'label', 'description'])
        save_dataframe_to_parquet(df_tags, parquet_dir / "xbrl_tags")

    # --- XBRL Facts ---
    if batch_data.get("xbrl_facts"):
        df_facts_all = pd.DataFrame(batch_data["xbrl_facts"])
        if not df_facts_all.empty:
            str_cols = ['cik', 'accession_number', 'taxonomy', 'tag_name', 'unit', 'value_text', 'fp', 'form', 'frame']
            date_cols = ['period_end_date', 'filed_date']
            numeric_cols = ['value_numeric']
            int_cols = ['fy']
            df_facts_all = _prepare_df_for_storage(df_facts_all, str_cols=str_cols, date_cols=date_cols, numeric_cols=numeric_cols, int_cols=int_cols)
            df_facts_all.drop(columns=['period_start_date'], inplace=True, errors='ignore')
            save_dataframe_to_parquet(df_facts_all, parquet_dir / "xbrl_facts")