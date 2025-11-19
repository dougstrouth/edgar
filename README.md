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
    *   **Polygon.io (Stocks)**: Fetches historical stock prices (OHLCV) with a generous free tier (5 calls/minute, 1-day delay). Backfills years of daily data reliably.
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
    *   `POLYGON_MAX_WORKERS` (optional): Parallel workers for the Polygon gatherer (default: 2; free tier is 5 req/min, so keep small).

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

For a detailed description of all database tables, columns, and their relationships, please see the [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Massive.com Quick Check

Validate connectivity and estimate capacity:

```bash
python tests/test_polygon_capacity.py
```
This checks your `POLYGON_API_KEY`, fetches sample tickers, and estimates effective calls/min under rate limits.

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
