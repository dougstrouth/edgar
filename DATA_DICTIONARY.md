# EDGAR Analytics - Data Dictionary

This document provides a detailed description of the database schema used in the EDGAR Analytics project. The database stores structured data parsed from SEC EDGAR bulk files and supplementary stock market data from Yahoo Finance.
It also includes optional daily OHLCV price history gathered from Polygon.io.

## Database Engine

*   **Type**: DuckDB

## Table Definitions

### `companies`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `primary_name` | `VARCHAR` | |
| `entity_name_cf` | `VARCHAR` | |
| `entity_type` | `VARCHAR` | |
| `sic` | `VARCHAR` | |
| `sic_description` | `VARCHAR` | |
| `ein` | `VARCHAR` | |
| `description` | `INTEGER` | |
| `category` | `VARCHAR` | |
| `fiscal_year_end` | `VARCHAR` | |
| `state_of_incorporation` | `VARCHAR` | |
| `phone` | `VARCHAR` | |
| `flags` | `INTEGER` | |
| `first_added_timestamp` | `INTEGER` | |
| `last_parsed_timestamp` | `TIMESTAMP WITH TIME ZONE` | |
| `mailing_street1` | `VARCHAR` | |
| `mailing_street2` | `VARCHAR` | |
| `mailing_city` | `VARCHAR` | |
| `mailing_state_or_country` | `VARCHAR` | |
| `mailing_zip_code` | `VARCHAR` | |
| `business_street1` | `VARCHAR` | |
| `business_street2` | `VARCHAR` | |
| `business_city` | `VARCHAR` | |
| `business_state_or_country` | `VARCHAR` | |
| `business_zip_code` | `VARCHAR` | |
| `rn` | `BIGINT` | |

### `downloaded_archives`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `file_path` | `VARCHAR` | |
| `file_name` | `VARCHAR` | |
| `url` | `VARCHAR` | |
| `size_bytes` | `BIGINT` | |
| `local_last_modified_utc` | `TIMESTAMP WITH TIME ZONE` | |
| `download_timestamp_utc` | `TIMESTAMP WITH TIME ZONE` | |
| `status` | `VARCHAR` | |

### `filing_summaries`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `accession_number` | `VARCHAR` | |
| `summary_text` | `VARCHAR` | |
| `summary_model` | `VARCHAR` | |
| `summary_date` | `TIMESTAMP WITH TIME ZONE` | |

### `filings`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `accession_number` | `VARCHAR` | |
| `filing_date` | `TIMESTAMP_NS` | |
| `report_date` | `TIMESTAMP_NS` | |
| `acceptance_datetime` | `TIMESTAMP WITH TIME ZONE` | |
| `act` | `VARCHAR` | |
| `form` | `VARCHAR` | |
| `file_number` | `VARCHAR` | |
| `film_number` | `VARCHAR` | |
| `items` | `VARCHAR` | |
| `size` | `BIGINT` | |
| `is_xbrl` | `BOOLEAN` | |
| `is_inline_xbrl` | `BOOLEAN` | |
| `primary_document` | `VARCHAR` | |
| `primary_doc_description` | `VARCHAR` | |
| `rn` | `BIGINT` | |

### `former_names`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `date_from` | `TIMESTAMP WITH TIME ZONE` | |
| `date_to` | `TIMESTAMP WITH TIME ZONE` | |
| `former_name` | `VARCHAR` | |

### `macro_economic_data`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `series_id` | `VARCHAR` | |
| `date` | `TIMESTAMP_NS` | |
| `value` | `DOUBLE` | |

### `market_risk_factors`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `date` | `DATE` | |
| `factor_model` | `VARCHAR` | |
| `mkt_minus_rf` | `DOUBLE` | |
| `SMB` | `DOUBLE` | |
| `HML` | `DOUBLE` | |
| `RMW` | `DOUBLE` | |
| `CMA` | `DOUBLE` | |
| `RF` | `DOUBLE` | |

### `tickers`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR(10)` | SEC Central Index Key |
| `ticker` | `VARCHAR` | Stock ticker symbol (e.g., AAPL) |
| `exchange` | `VARCHAR` | Exchange symbol from SEC data |
| `source` | `VARCHAR` | Data source (e.g., 'sec_company_tickers.json') |
| `active` | `BOOLEAN` | Whether ticker is actively traded (Massive.com) |
| `composite_figi` | `VARCHAR` | Composite OpenFIGI identifier |
| `share_class_figi` | `VARCHAR` | Share class OpenFIGI identifier |
| `currency_name` | `VARCHAR` | Currency name (e.g., 'usd') |
| `currency_symbol` | `VARCHAR` | ISO 4217 currency code |
| `base_currency_name` | `VARCHAR` | Base currency name for forex/crypto |
| `base_currency_symbol` | `VARCHAR` | Base currency ISO code |
| `last_updated_utc` | `TIMESTAMP` | Last update timestamp from Massive.com |
| `delisted_utc` | `TIMESTAMP` | Delisting timestamp if applicable |
| `locale` | `VARCHAR` | Market locale (e.g., 'us', 'global') |
| `market` | `VARCHAR` | Market type (stocks, crypto, fx, otc, indices) |
| `name` | `VARCHAR` | Company/asset name |
| `primary_exchange` | `VARCHAR` | Primary listing exchange (ISO MIC code) |
| `type` | `VARCHAR` | Asset type (e.g., 'CS' for Common Stock) |
| `rn` | `BIGINT` | Row number for deduplication |

### `xbrl_facts`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `accession_number` | `VARCHAR` | |
| `taxonomy` | `VARCHAR` | |
| `tag_name` | `VARCHAR` | |
| `unit` | `VARCHAR` | |
| `period_end_date` | `TIMESTAMP_NS` | |
| `value_numeric` | `DOUBLE` | |
| `value_text` | `INTEGER` | |
| `fy` | `BIGINT` | |
| `fp` | `VARCHAR` | |
| `form` | `VARCHAR` | |
| `filed_date` | `TIMESTAMP_NS` | |
| `frame` | `VARCHAR` | |

### `xbrl_facts_orphaned`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `taxonomy` | `VARCHAR` | |
| `tag_name` | `VARCHAR` | |
| `accession_number` | `VARCHAR` | |
| `unit` | `VARCHAR` | |
| `period_end_date` | `TIMESTAMP_NS` | |
| `value_numeric` | `DOUBLE` | |
| `value_text` | `INTEGER` | |
| `fy` | `BIGINT` | |
| `fp` | `VARCHAR` | |
| `form` | `VARCHAR` | |
| `filed_date` | `TIMESTAMP_NS` | |
| `frame` | `VARCHAR` | |

### `xbrl_tags`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `taxonomy` | `VARCHAR` | |
| `tag_name` | `VARCHAR` | |
| `label` | `VARCHAR` | |
| `description` | `VARCHAR` | |
| `rn` | `BIGINT` | |

### `stock_history`

Daily price history for equities. Primary source is Polygon.io (free tier: 1-day delayed, 5 calls/min).

**Note on Data Adjustment**: Data fetched via REST API uses `adjusted=true` and is pre-adjusted for 
splits and dividends. Polygon.io Flat Files contain unadjusted data and require manual adjustment.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | Ticker symbol (case-insensitive). |
| `date` | `DATE` | Trading date (UTC). |
| `open` | `DOUBLE` | Open price (adjusted for splits). |
| `high` | `DOUBLE` | High price (adjusted). |
| `low` | `DOUBLE` | Low price (adjusted). |
| `close` | `DOUBLE` | Close price (adjusted). |
| `adj_close` | `DOUBLE` | Alias of adjusted close (same as `close` for Polygon daily aggregates). |
| `volume` | `BIGINT` | Volume. |

Primary Key: `(ticker, date)`

### `updated_ticker_info`

Comprehensive ticker details from Massive.com (formerly Polygon.io) v3 Reference API. Includes
company information, market data, financial identifiers, and sector classification.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | Ticker symbol (case-insensitive). Primary Key. |
| `cik` | `VARCHAR` | SEC Central Index Key. |
| `name` | `VARCHAR` | Company/asset name. |
| `market` | `VARCHAR` | Market type (stocks, crypto, fx, otc, indices). |
| `locale` | `VARCHAR` | Market locale (e.g., 'us', 'global'). |
| `primary_exchange` | `VARCHAR` | Primary listing exchange (ISO MIC code). |
| `type` | `VARCHAR` | Asset type (e.g., 'CS' for Common Stock, 'ADRC' for ADR). |
| `active` | `BOOLEAN` | Whether ticker is actively traded. |
| `currency_name` | `VARCHAR` | Currency name (e.g., 'usd'). |
| `currency_symbol` | `VARCHAR` | ISO 4217 currency code. |
| `base_currency_name` | `VARCHAR` | Base currency name for forex/crypto. |
| `base_currency_symbol` | `VARCHAR` | Base currency ISO code. |
| `composite_figi` | `VARCHAR` | Composite OpenFIGI identifier. |
| `share_class_figi` | `VARCHAR` | Share class OpenFIGI identifier. |
| `description` | `VARCHAR` | Company description/overview. |
| `homepage_url` | `VARCHAR` | Company homepage URL. |
| `total_employees` | `BIGINT` | Total number of employees. |
| `list_date` | `VARCHAR` | Date of initial listing. |
| `sic_code` | `VARCHAR` | Standard Industrial Classification code. |
| `sic_description` | `VARCHAR` | SIC code description (industry). |
| `ticker_root` | `VARCHAR` | Root ticker symbol. |
| `source_feed` | `VARCHAR` | Data source feed identifier. |
| `market_cap` | `DOUBLE` | Market capitalization. |
| `weighted_shares_outstanding` | `BIGINT` | Weighted average shares outstanding. |
| `round_lot` | `INTEGER` | Standard trading lot size. |
| `fetch_timestamp` | `TIMESTAMPTZ` | Timestamp when this data was fetched. |
| `last_updated_utc` | `TIMESTAMPTZ` | Last update timestamp from Polygon. |
| `delisted_utc` | `TIMESTAMPTZ` | Delisting timestamp if applicable. |
| `address_1` | `VARCHAR` | Company street address. |
| `city` | `VARCHAR` | Company city. |
| `state` | `VARCHAR` | Company state/province. |
| `postal_code` | `VARCHAR` | Company postal/zip code. |
| `logo_url` | `VARCHAR` | Company logo URL. |
| `icon_url` | `VARCHAR` | Company icon URL. |

Primary Key: `ticker`

Notes:
- Parquet files are written under `${PARQUET_DIR}/updated_ticker_info` by `data_gathering/ticker_info_gatherer_polygon.py`.
- Load into DuckDB using: `python main.py load-ticker-info` or `python data_processing/load_ticker_info.py`.


Notes:
- Parquet files are written under `${PARQUET_DIR}/stock_history` by `data_gathering/stock_data_gatherer_polygon.py`.
- Load into DuckDB using: `python data_processing/load_supplementary_data.py stock_history`.

### `stock_fetch_errors`

Tracks issues encountered when fetching price data.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | Optional CIK if mapped. |
| `ticker` | `VARCHAR` | Ticker symbol. |
| `error_timestamp` | `TIMESTAMPTZ` | Time the error was recorded. |
| `error_type` | `VARCHAR` | Short error category. |
| `error_message` | `VARCHAR` | Detailed message. |
| `start_date_req` | `DATE` | Start date requested. |
| `end_date_req` | `DATE` | End date requested. |

