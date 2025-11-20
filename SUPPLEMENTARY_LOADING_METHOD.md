# Supplementary Data Loading Methodology

This document explains the unified approach used for loading supplementary tables (macro, market risk, yfinance, stock history, etc.) into DuckDB.

## Overview
Supplementary data falls into two categories:
1. Full-Swap (Snapshot) Tables – Replaced wholesale each run.
2. Incremental (Drip-Feed) Tables – New batches merged (upserted) into existing history.

## Classification
**Full-Swap Tables:**
- `macro_economic_data`
- `market_risk_factors`
- `yf_profile_metrics`
- `yf_major_holders`
- `yf_income_statement`
- `yf_balance_sheet`
- `yf_cash_flow`
- `yf_untrackable_tickers`

**Incremental Tables:**
- `stock_history` (daily bars appended)
- `stock_fetch_errors` (event log)
- `yf_stock_actions` (corporate actions accumulate)
- `yf_recommendations` (analyst events accumulate)
- `yf_info_fetch_errors` (event log)

## Loader Behavior
The loader (`load_supplementary_data.py`) applies two strategies:

### 1. Blue-Green Replacement (Full-Swap)
- Create staging table `<table>_new` using defined schema (preserves PKs).
- Bulk load Parquet batch with `INSERT OR REPLACE` (dedup within batch using PK).
- Apply supporting unique indexes (macro & market risk).
- Atomic swap: drop old table; rename staging to live.
- Safety warning if staging row count is zero.

### 2. Incremental Upsert
- Ensure base table exists.
- Create batch staging table `<table>_batch` from schema.
- Bulk load Parquet via `INSERT OR REPLACE` (dedup inside batch).
- Merge: `INSERT OR REPLACE INTO <table> SELECT * FROM <table>_batch`.
- Drop staging batch table.
- Does **not** remove existing historical records.

## Primary Keys & Uniqueness
Added PKs where previously missing to enable deterministic upsert:
- `stock_fetch_errors`: `PRIMARY KEY (ticker, error_timestamp, error_type)`
- `yf_info_fetch_errors`: `PRIMARY KEY (ticker, error_timestamp)`

Existing schemas already define PKs for other tables (e.g., `stock_history (ticker,date)`). These keys support DuckDB's `INSERT OR REPLACE` semantics.

## Rationale
- Prevent accidental data truncation for drip-feed tables.
- Maintain atomicity and consistency for snapshot-style tables.
- Support idempotent re-processing (same batch can be safely reloaded).

## Safety Guarantees

### Blue-Green Snapshot Swap Guards

**Row Count Protection**: Before executing atomic swap operations, loaders compare staging table row count with existing table. Swap is aborted if:
- Staging count < existing count (prevents data shrinkage)
- Staging is empty while existing table has rows (prevents total data loss)

Exception: When both staging and existing are empty (initial load scenario), swap proceeds with a warning.

**Ticker Universe Protection** (`updated_ticker_info` only): In addition to row count, the loader validates distinct ticker count. Full refresh is aborted if staging contains fewer distinct tickers than existing table, preventing accidental contraction of ticker universe coverage.

**Implementation**: Guards execute before transaction begins; failed validation drops staging table and retains original data intact. Error-level log messages provide clear diagnostic info (staging vs existing counts).

### Incremental Upsert Safety
Incremental tables merge new batches using `INSERT OR REPLACE` which updates matching PKs in-place while leaving unmatched historical rows untouched. No automatic deletion occurs; explicit gatherer logic must emit removal records if needed.

## Future Enhancements
- Optional change tracking: record batch timestamp & row counts in audit table.
- Incremental pruning / compaction for very large error or action logs.
- Extend swap guards to additional snapshot tables as new sources are integrated.

## Operational Notes
- Ensure gatherers produce all PK columns; otherwise upsert will fail.
- For incremental tables, Parquet batches should include only new or corrected rows; existing rows remain untouched unless explicitly replaced.
- For large historical rebuilds, pass `--full-refresh` to force snapshot logic even for normally incremental tables.

---
Updated: 2025-11-19
