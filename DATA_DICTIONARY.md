# EDGAR Analytics - Data Dictionary

This document provides a detailed description of the database schema used in the EDGAR Analytics project. The database stores structured data parsed from SEC EDGAR bulk files and supplementary stock market data from Yahoo Finance.

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

### `stock_fetch_errors`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `INTEGER` | |
| `ticker` | `VARCHAR` | |
| `error_timestamp` | `TIMESTAMP WITH TIME ZONE` | |
| `error_type` | `VARCHAR` | |
| `error_message` | `VARCHAR` | |
| `start_date_req` | `TIMESTAMP_NS` | |
| `end_date_req` | `TIMESTAMP_NS` | |

### `stock_history`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | |
| `date` | `TIMESTAMP_NS` | |
| `open` | `DOUBLE` | |
| `high` | `DOUBLE` | |
| `low` | `DOUBLE` | |
| `close` | `DOUBLE` | |
| `adj_close` | `DOUBLE` | |
| `volume` | `BIGINT` | |

### `tickers`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | |
| `exchange` | `VARCHAR` | |
| `source` | `VARCHAR` | |
| `ticker` | `VARCHAR` | |

### `xbrl_facts`

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

### `yf_untrackable_tickers`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | |
| `reason` | `VARCHAR` | |
| `last_failed_timestamp` | `TIMESTAMP WITH TIME ZONE` | |

