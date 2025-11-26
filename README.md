# EDGAR Analytics Pipeline

This project provides a robust, end-to-end data engineering pipeline for downloading, parsing, and storing financial data from the U.S. Securities and Exchange Commission (SEC) EDGAR system and other supplementary sources. The data is structured and loaded into a DuckDB database, creating a powerful, local analytics engine suitable for financial analysis and machine learning applications.

## Key Features

*   **Automated Bulk Data Fetching**: Downloads the latest bulk archives from the SEC EDGAR system, including company submissions and XBRL facts.
*   **Efficient Two-Stage Loading**:
    1.  **Parse to Parquet**: Raw JSON data is first parsed into a highly efficient, columnar Parquet format. This intermediate step allows for fast, parallel processing and decouples parsing from database loading.
    2.  **Load to Database**: Parquet files are bulk-loaded into a DuckDB database, which is extremely fast for analytical queries.
*   **Atomic Database Updates**: The data loading process uses a "blue-green" deployment strategy. Data is loaded into temporary tables, and only upon successful completion are the live tables atomically swapped. This ensures the database is never left in a corrupted or incomplete state, even if a load fails midway.
*   **Data Ordering for Performance**: During the database load, the largest tables (`xbrl_facts` and `filings`) are pre-sorted on commonly queried columns (like `cik`, `form`, and `filing_date`). This significantly improves query performance by making DuckDB's automatic zonemap indexes more effective.
*   **Supplementary Data Integration**:
    *   **Massive.com (formerly Polygon.io) – Stocks**: Fetches historical stock prices (OHLCV) with a generous free tier (5 calls/minute, 1-day delay). Backfills years of daily data reliably.
    *   **Yahoo Finance**: Fetches company profile information, financial statements, and corporate actions.
    *   **Federal Reserve (FRED)**: Gathers key macroeconomic time-series data (e.g., GDP, CPI, interest rates) to provide economic context for financial models.
*   **Resilient API Interaction**:
    *   **Intelligent Retries**: Implements exponential backoff with jitter for handling API rate limits from external sources like Yahoo Finance.
    *   **Failure Tracking**: Automatically identifies and logs tickers that consistently fail (e.g., "No Data Found") to a dedicated table, preventing wasted API calls on subsequent runs. These entries expire after a configurable period (default: 365 days) to allow for re-checking.
*   **Modular & Extensible Architecture**: Each stage of the pipeline (fetch, parse, load, gather) is a separate, self-contained script, making it easy to maintain, debug, and extend with new data sources or processing steps.
*   **Centralized Orchestration**: A single `main.py` entry point provides a command-line interface (CLI) to run the entire pipeline or any specific step, simplifying execution.
*   **Integrated Utilities**: Includes dedicated scripts for data validation (`validate_edgar_db.py`) and cleanup of intermediate artifacts (`cleanup_artifacts.py`) to manage disk space and ensure data quality.

## Getting Started

### Prerequisites

*   Python 3.10 or higher
*   `uv` for package management (recommended)

### Installation

1.  **Clone the Repository**:
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Set up Virtual Environment and Install Dependencies**:
    This project uses `uv` for package management.
    ```bash
    # Create a virtual environment
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`

    # Install dependencies from the lock file
    uv pip sync
    ```

3.  **Configure Environment**:
    Copy the example environment file and fill in the required values.
    ```bash
    cp .env.example .env
    ```
    You will need to edit the `.env` file to provide:
    *   `DOWNLOAD_DIR`: The absolute path to a directory where all data will be stored.
    *   `DB_FILE`: The absolute path for the DuckDB database file (e.g., `C:\path\to\data\edgar_analytics.duckdb`).
    *   `SEC_USER_AGENT`: A descriptive User-Agent for making requests to the SEC (e.g., `YourName YourOrg your.email@example.com`). **This is required by the SEC.**
    *   `FRED_API_KEY`: Your API key for the FRED service (optional, if using `gather-macro`).
    *   `POLYGON_API_KEY`: Your API key from Massive.com (formerly Polygon.io) - free tier works.
    *   `POLYGON_MAX_WORKERS` (optional): Parallel workers for Massive/Polygon gatherers. For free tier, keep at `1`.
    *   `POLYGON_CALLS_PER_MINUTE` (optional): Effective client-side cap for Massive API calls per minute (default: 3; max: 5 on free tier). The client also spaces calls by ~20s to avoid burst 429s.

## Project Structure

The project is organized into the following directories:

*   `main.py`: The main orchestrator for running pipeline steps.
*   `data_gathering/`: Scripts for fetching data from external sources (SEC, Massive.com, Yahoo Finance, FRED).
*   `data_processing/`: Scripts for parsing, cleaning, and loading data into the database.
*   `analysis/`: Scripts and notebooks for analyzing the data.
*   `utils/`: Utility scripts for configuration, logging, and database connections.
*   `scripts/`: Standalone scripts for tasks like cleanup.
*   `logs/`: Directory for log files.
*   `DATA_DICTIONARY.md`: A detailed description of all database tables, columns, and their relationships.

## Usage

The pipeline is controlled via `main.py`.

### Running the Full Pipeline

To run the core data pipeline, which includes fetching SEC data, parsing it, loading it into the database, gathering macroeconomic data, and cleaning up, use:
```bash
python main.py all
```
The `all` command runs the following steps in sequence: `fetch`, `parse-to-parquet`, `load`, `validate`, `gather_macro`, `load_macro`, `gather_market_risk`, `load_market_risk`, and `cleanup`.

> **Note**: The supplementary data gathering steps for individual stocks from Yahoo Finance (`gather-stocks`, `gather-info`) are not included in the `all` command due to potential API rate-limiting issues. They should be run individually.

### Running Individual Steps

You can run any single step of the pipeline. This is useful for debugging or re-running a failed stage.

**Core EDGAR Pipeline:**
```bash
# 1. Download SEC bulk data
python main.py fetch

# 2. Parse downloaded JSON into Parquet
python main.py parse-to-parquet

# 3. Load the Parquet files into DuckDB
python main.py load
```

**Supplementary Data Gathering:**
```bash
# Gather company profile info from Yahoo Finance
python main.py gather_info
python main.py load_info

# Gather macroeconomic data from FRED
python main.py gather_macro
python main.py load_macro

# Gather market risk factors
python main.py gather_market_risk
python main.py load_market_risk

# Step 3: Gather supplementary stock data
# Gather daily OHLCV from Massive.com (formerly Polygon.io) - recommended for prices
# Start small to test (limit=50 means 50 tickers)
python data_gathering/stock_data_gatherer_polygon.py --mode initial_load --limit 50

# Option B: orchestrator alias
python main.py gather-stocks-polygon -- --mode initial_load --limit 50

# Load the gathered prices into DuckDB
python data_processing/load_supplementary_data.py stock_history
```

### Stocks: Loader and De-duplication
The `stock_history` table uses incremental upserts with a composite primary key on `(ticker, date)`. This ensures no duplicates and that re-runs replace existing rows for the same day:

- Table schema constraint: `PRIMARY KEY (ticker, date)`
- Merge behavior: `INSERT OR REPLACE` from a batch staging table
- De-duplication is automatic at the database level

Quick duplicate check (expects `duckdb` installed):
```bash
python - <<'PY'
import duckdb
con = duckdb.connect('/absolute/path/to/edgar_analytics.duckdb', read_only=True)
total = con.execute('SELECT COUNT(*) FROM stock_history').fetchone()[0]
unique = con.execute('SELECT COUNT(DISTINCT (ticker, date)) FROM stock_history').fetchone()[0]
print('Total:', total, 'Unique:', unique, 'Duplicates:', total-unique)
con.close()
PY
```

## Pipeline Steps (Quick Guide)

This section summarizes common orchestrator steps and how they differ.

- `gather-stocks-polygon`: Fetch OHLCV stock data from Massive/Polygon (writes Parquet only)
- `load_stocks`: Load `stock_history` Parquet into DuckDB (incremental upsert)
- `gather-ticker-info`: Fetch official ticker reference data from Massive/Polygon (writes Parquet)
- `load-ticker-info`: Load ticker reference Parquet into DuckDB
- `enrich-tickers`: Enrich local `massive_tickers` universe (active-only or include-delisted). Prepares/curates the ticker set and attributes; complements `gather-ticker-info` which fetches official reference data from the API.

Notes on naming and aliases:
- Launch configs may show friendly names, but the orchestrator recognizes hyphenated keys (e.g., `gather-stocks-polygon`, `enrich-tickers`). Aliases like `gather_stocks_polygon` exist in `main.py` for convenience; prefer the hyphenated form for consistency.

Validation additions:
- The database validator now includes `stock_history` checks: table/columns existence, `(ticker,date)` duplicates, non-null required fields, non-negative price/volume ranges, and a logical `high >= low` check.

**Other Commands:**
```bash
# Run feature engineering scripts
python main.py feature_eng

# Validate the data in the database
python main.py validate

# Clean up intermediate files
python main.py cleanup --all
```

## Database Schema

For a detailed description of all database tables, columns, and their relationships, see [DATA_DICTIONARY.md](DATA_DICTIONARY.md). For a concise overview of table-to-table relationships and validation implications, see [DATABASE_RELATIONSHIPS.md](DATABASE_RELATIONSHIPS.md).

## Massive.com Quick Check

Validate connectivity and estimate capacity:

```bash
python tests/test_polygon_capacity.py
```
This checks your `POLYGON_API_KEY`, fetches sample tickers, and estimates effective calls/min under rate limits.

### Massive.com Client & Rate Limiting
The built-in client targets `https://api.massive.com` (Polygon rebrand) and applies conservative rate limiting suitable for the free tier:

- Default cap is `POLYGON_CALLS_PER_MINUTE=3` with ~20s spacing between requests to avoid burst 429s; you can raise to 4–5 if stable.
- 404 responses (unknown tickers) still count against your quota; failed requests are tracked to avoid repeated waste.
- Tickers with dashes are normalized to dot notation (e.g., `AAM-UN` → `AAM.UN`).

Environment knobs:
- `POLYGON_MAX_WORKERS=1` (recommended for free tier)
- `POLYGON_CALLS_PER_MINUTE=3` (safe default; max 5 on free tier)
- `POLYGON_BATCH_SIZE=100` (optional; unified batch size for both Massive/Polygon gatherers. Controls how many rows are buffered before writing a parquet batch. Larger values reduce small-file churn; smaller values surface data sooner.)

**Recent Improvements (November 2025):**
- **Unified Configuration**: Single `POLYGON_BATCH_SIZE` variable replaces separate stock/info batch sizes. Deprecated variables (`POLYGON_STOCK_BATCH_SIZE`, `POLYGON_INFO_BATCH_SIZE`, `POLYGON_STOCK_DB_WRITE_INTERVAL_SECONDS`) removed.
- **Decoupled Database Loading**: Stock gatherers no longer perform automatic database writes. Gathering and loading are now separate, explicit steps for improved reliability and easier recovery. Use `python update_from_parquet.py` after gathering completes.
- **Rate Limiter Preservation**: Single-worker mode now reuses one `PolygonClient` instance across all jobs, preserving backoff state and preventing rate limiter resets.
- **Code Quality**: Removed duplicate function definitions from `stock_data_gatherer_polygon.py` (file size reduced by ~50%), added defensive `fetchone()` handling to prevent potential None indexing crashes.
- **Enhanced Diagnostics**: Improved error classification and proactive code quality monitoring.

Batching & Load Simplification:
- The stock price gatherer no longer performs periodic or automatic final database loads. It only writes parquet batches. Run `python update_from_parquet.py` (or the dedicated load scripts) when you want to ingest newly written parquet files.
- This decoupling improves reliability (DB load happens in an explicit, restartable step) and reduces contention with long-running gathers.

Migration Note:
- Deprecated variables removed: `POLYGON_STOCK_BATCH_SIZE`, `POLYGON_STOCK_DB_WRITE_INTERVAL_SECONDS`, `POLYGON_INFO_BATCH_SIZE`.
- Replace any prior references with the single `POLYGON_BATCH_SIZE` variable. Leaving old vars in `.env` has no effect.

## Local Schema Tasks

The repository includes a local runner to regenerate the programmatic data dictionary, refresh the test database, populate AI-friendly unique-value tables, and run unit tests. This is useful for developer workflows where CI does not have access to your production DuckDB file.

Quick usage (preferred, uses `.env`):
```bash
python scripts/run_local_schema_tasks.py
```

Explicit paths and forced refresh:
```bash
python scripts/run_local_schema_tasks.py --db /path/to/edgar_analytics.duckdb --test-db /path/to/edgar_analytics_test.duckdb --force
```

What it does:
- Regenerates `DATA_DICTIONARY_GENERATED.md` from the provided `DB_FILE`.
- Creates or refreshes the test DB and populates `ai_unique_values` (and per-source partition tables).
- Runs `pytest` to execute unit tests.

There is also a VS Code launch profile `Local: schema tasks runner` (in `.vscode/launch.json`) that invokes the same script using your workspace `.env`.

## VS Code Debugging

Launch profiles are included in `.vscode/launch.json`. Ensure your `.env` is present at the workspace root; VS Code will honor it via the project's config loader. Useful entries:
 - `MAIN: Run 'all' pipeline`
 - `STEP: fetch`, `parse-to-parquet`, `load`, `validate`
 - `STEP: gather-stocks-polygon (initial 50)` to exercise Polygon gathering

If you add new steps, consider updating `main.py` choices and `.vscode/launch.json` to keep them aligned.

## Incremental Parquet Loader

The project provides an incremental loader `update_from_parquet.py` which scans the `PARQUET_DIR` (per-table subdirectories) and only applies new Parquet batch files into the DuckDB database. Key features:

- Dry-run mode: run with `--dry-run` to list files that would be processed without writing to the DB.
- Checkpointing: before making destructive changes to the production DB, the script creates a timestamped checkpoint copy of the existing DB file. Use `--no-checkpoint` to disable this behavior.

Example usage:
```bash
# Dry-run: show what would be applied
python update_from_parquet.py --dry-run

# Apply new Parquet files with a checkpoint copy of the DB created first
python update_from_parquet.py

# Apply without creating a checkpoint (use with caution)
python update_from_parquet.py --no-checkpoint
```

## Supplementary Loading Methodology

Supplementary (non-core EDGAR) datasets are loaded using two distinct strategies, chosen per table based on data evolution patterns and risk tolerance for historical replacement:

**Snapshot (Blue-Green Atomic Swap)**
- Tables whose entire historical dataset is regenerated each gather run (e.g. macroeconomic time series, market risk factors) are loaded through a staging table (`<table>_new`).
- Data is bulk inserted into the staging table; uniqueness is enforced via PRIMARY KEYs and supporting indexes.
- After successful staging, an atomic swap (`DROP TABLE` + `ALTER TABLE RENAME`) replaces the live table. Failed swaps roll back cleanly.
- Guarantees: no partial updates; consistent point-in-time view.

**Incremental (Drip-Feed Upsert)**
- Tables that accumulate new events (e.g. `stock_history`, `stock_fetch_errors`, `yf_stock_actions`, `yf_recommendations`, `yf_info_fetch_errors`) ingest only new batches.
- A batch staging table (`<table>_batch`) is populated from all matching parquet files, then merged using `INSERT OR REPLACE` into the base table (PRIMARY KEY required).
- Old rows remain unless a batch includes an updated value for the same key.
- Guarantees: append-or-update semantics without full historical reload.

The authoritative classification and deeper rationale (including future enhancement notes) is documented in [`SUPPLEMENTARY_LOADING_METHOD.md`](SUPPLEMENTARY_LOADING_METHOD.md).

### DuckDB Primary Key Enforcement for Incremental Tables
Some legacy databases created before PKs were added may lack the required primary key on `stock_history`. The loader now auto-detects this scenario and performs a safe in-place migration:

1. Back up existing rows to a temporary table
2. Drop and recreate the base table with the proper `PRIMARY KEY`
3. Restore the data, then proceed with `INSERT OR REPLACE` upserts

If you encountered this error previously:

```
Binder Error: There are no UNIQUE/PRIMARY KEY Indexes that refer to this table, ON CONFLICT is a no-op
```

Re-run the loader and it will auto-correct the schema before the merge.

### Ticker Info Loader (Polygon / Massive)
`updated_ticker_info` uses both modes:
- Default execution performs an incremental upsert (existing ticker rows are updated in-place).
- Pass `--full-refresh` to perform a blue-green replacement (use after large schema or source changes).

Usage examples:
```bash
# Incremental (default)
python data_processing/load_ticker_info.py

# Full refresh atomic swap
python data_processing/load_ticker_info.py --full-refresh
```

### Massive Ticker Enrichment Parquet Backups
`ticker_enrichment_massive.py` writes a timestamped parquet backup for every enrichment batch under `PARQUET_DIR/massive_tickers/` (e.g. `massive_tickers_batch_20240131_235959.parquet`). These files:
- Provide an audit trail of enrichment inputs.
- Allow reconstruction or replays if the database must be restored.
- Are safe to prune by age once downstream validation is complete.

### Operational Safety Notes
- All snapshot tables have PRIMARY KEY constraints enabling deterministic `INSERT OR REPLACE` usage during staging (even if duplicates arise in inputs).
- Incremental tables avoid `DROP` operations—reduces accidental data loss risk during partial runs.
- **Swap Guards**: Blue-green snapshot loaders abort atomic swap operations when staging row count is less than existing table, preventing accidental data reduction or loss. Ticker info loader additionally validates distinct ticker count to prevent universe contraction.
- Empty staging detection: loaders refuse to swap when staging is empty but existing table contains data (logged as error with retention of original).
- Backups: parquet artifacts themselves are the provenance layer; consider offloading older batches to cold storage for long-term retention.

### Adding New Supplementary Tables
1. Define a schema with an appropriate PRIMARY KEY in `load_supplementary_data.py`.
2. Decide classification: snapshot vs incremental (see methodology doc).
3. Emit parquet via a gatherer script using deterministic column ordering.
4. Load with either incremental merge or blue-green swap; update documentation and tests accordingly.
5. Add validation queries (distribution checks, row counts) where appropriate.

### Quick Reference
| Strategy | When to Use | Mechanism | Risk Mitigated |
|----------|-------------|-----------|----------------|
| Blue-Green Swap | Regenerated full datasets | Staging + atomic rename | Partial/corrupted historical reloads |
| Incremental Upsert | Event-based growth | Batch staging + upsert | Unnecessary full reload & data churn |

For full details, including planned enhancements (audit table, empty-swap guards, batch lineage), see [`SUPPLEMENTARY_LOADING_METHOD.md`](SUPPLEMENTARY_LOADING_METHOD.md).

## Testing

The repository includes unit and integration tests under the `tests/` directory. Tests use a virtual environment and the project's test fixtures.

Run the full test suite locally:
```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

If you add or modify parsing/loader code, run tests frequently. Integration tests create temporary DuckDB instances and temporary Parquet files, so they are safe to run on development machines.

## Contributing

Contributions are welcome. Please open an issue to discuss any proposed changes or enhancements.
