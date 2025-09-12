﻿# EDGAR Analytics Pipeline

This project provides a robust, end-to-end data engineering pipeline for downloading, parsing, and storing financial data from the U.S. Securities and Exchange Commission (SEC) EDGAR system and other supplementary sources. The data is structured and loaded into a DuckDB database, creating a powerful, local analytics engine suitable for financial analysis and machine learning applications.

## Key Features

*   **Automated Bulk Data Fetching**: Downloads the latest bulk archives from the SEC EDGAR system, including company submissions and XBRL facts.
*   **Efficient Two-Stage Loading**:
    1.  **Parse to Parquet**: Raw JSON data is first parsed into a highly efficient, columnar Parquet format. This intermediate step allows for fast, parallel processing and decouples parsing from database loading.
    2.  **Load to Database**: Parquet files are bulk-loaded into a DuckDB database, which is extremely fast for analytical queries.
*   **Atomic Database Updates**: The data loading process uses a "blue-green" deployment strategy. Data is loaded into temporary tables, and only upon successful completion are the live tables atomically swapped. This ensures the database is never left in a corrupted or incomplete state, even if a load fails midway.
*   **Supplementary Data Integration**:
    *   **Yahoo Finance**: Fetches historical stock prices (OHLCV), company profile information, financial statements, and corporate actions.
    *   **Federal Reserve (FRED)**: Gathers key macroeconomic time-series data (e.g., GDP, CPI, interest rates) to provide economic context for financial models.
*   **Resilient API Interaction**:
    *   **Intelligent Retries**: Implements exponential backoff with jitter for handling API rate limits from external sources like Yahoo Finance.
    *   **Failure Tracking**: Automatically identifies and logs tickers that consistently fail (e.g., "No Data Found") to a dedicated table, preventing wasted API calls on subsequent runs. These entries expire after a configurable period (default: 365 days) to allow for re-checking.
*   **Modular & Extensible Architecture**: Each stage of the pipeline (fetch, parse, load, gather) is a separate, self-contained script, making it easy to maintain, debug, and extend with new data sources or processing steps.
*   **Centralized Orchestration**: A single `main.py` entry point provides a command-line interface (CLI) to run the entire pipeline or any specific step, simplifying execution.
*   **Integrated Utilities**: Includes dedicated scripts for data validation (`validate_edgar_db.py`) and cleanup of intermediate artifacts (`cleanup_artifacts.py`) to manage disk space and ensure data quality.

## Project Structure

The pipeline is composed of several key scripts:

*   `main.py`: The main orchestrator for running pipeline steps.
*   `config_utils.py`: Handles loading of configuration from the `.env` file.
*   `logging_utils.py`: Provides standardized logging across all scripts.
*   `database_conn.py`: Manages DuckDB database connections.

### Data Processing Stages

1.  **`fetch_edgar_archives.py`**: Downloads bulk data ZIP archives from the SEC.
2.  **`parse_to_parquet.py`**: Parses the raw JSON files from the archives into structured Parquet files.
3.  **`edgar_data_loader.py`**: Loads the core EDGAR data from Parquet into the DuckDB database.

### Supplementary Data Gathering

*   **`stock_data_gatherer.py`**: Fetches historical stock price data.
*   **`stock_info_gatherer.py`**: Fetches company financial statements from Yahoo Finance.
*   **`macro_data_gatherer.py`**: Fetches macroeconomic data from FRED.
*   **`load_supplementary_data.py`**: Loads all supplementary data from Parquet into DuckDB.

### Utilities

*   **`validate_edgar_db.py`**: Runs a series of checks to validate data integrity in the database.
*   **`cleanup_artifacts.py`**: Removes intermediate files (JSON, Parquet, cache) to free up disk space.

## Setup and Installation

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

## Usage

The pipeline is controlled via `main.py`.

### Running the Full Pipeline

To run all steps in the correct sequence (fetch, parse, load, validate, cleanup), use:
```bash
python main.py all
```

> **Note**: The supplementary data gathering steps (`gather-stocks`, `gather-info`, `gather-macro`) are currently disabled in the `all` command due to potential API rate-limiting issues. They can be run individually.

### Running Individual Steps

You can run any single step of the pipeline. This is useful for debugging or re-running a failed stage.

```bash
# Download SEC bulk data
python main.py fetch

# Parse downloaded JSON into Parquet
python main.py parse-to-parquet

# Load the Parquet files into DuckDB
python main.py load

# Validate the data in the database
python main.py validate

# Clean up intermediate files
python main.py cleanup --all
```

## Database Schema

For a detailed description of all database tables, columns, and their relationships, please see the **Data Dictionary**.

## Contributing

Contributions are welcome. Please open an issue to discuss any proposed changes or enhancements.