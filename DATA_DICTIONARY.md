# EDGAR Analytics - Data Dictionary

This document provides a detailed description of the database schema used in the EDGAR Analytics project. The database stores structured data parsed from SEC EDGAR bulk files and supplementary data from sources like Yahoo Finance and FRED.

## Database Engine

*   **Type**: DuckDB

## Table of Contents

1.  [Core EDGAR Tables](#core-edgar-tables)
    *   [`companies`](#companies)
    *   [`filings`](#filings)
    *   [`xbrl_facts`](#xbrl_facts)
    *   [`xbrl_tags`](#xbrl_tags)
    *   [`former_names`](#former_names)
    *   [`tickers`](#tickers)
    *   [`xbrl_facts_orphaned`](#xbrl_facts_orphaned)
2.  [Supplementary Data Tables](#supplementary-data-tables)
    *   [`stock_history`](#stock_history)
    *   [`yf_profile_metrics`](#yf_profile_metrics)
    *   [`yf_recommendations`](#yf_recommendations)
    *   [`yf_major_holders`](#yf_major_holders)
    *   [`yf_stock_actions`](#yf_stock_actions)
    *   [`yf_income_statement`](#yf_income_statement)
    *   [`yf_balance_sheet`](#yf_balance_sheet)
    *   [`yf_cash_flow`](#yf_cash_flow)
    *   [`macro_economic_data`](#macro_economic_data)
    *   [`market_risk_factors`](#market_risk_factors)
3.  [Operational & Logging Tables](#operational--logging-tables)
    *   [`stock_fetch_errors`](#stock_fetch_errors)
    *   [`yf_info_fetch_errors`](#yf_info_fetch_errors)
    *   [`yf_untrackable_tickers`](#yf_untrackable_tickers)

---

## Core EDGAR Tables

These tables contain data sourced directly from the SEC EDGAR system.

### `companies`

Stores identifying information for each entity that files with the SEC.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR(10)` | **Primary Key**. The Central Index Key, a unique 10-digit identifier for each filer. |
| `primary_name` | `VARCHAR` | The most recent legal name of the entity. |
| `entity_name_cf` | `VARCHAR` | The entity name as it appears in the Company Facts files. |
| `entity_type` | `VARCHAR` | The type of entity (e.g., 'operating', 'investment'). |
| `sic` | `VARCHAR` | The Standard Industrial Classification code. |
| `sic_description` | `VARCHAR` | The description of the SIC code. |
| `ein` | `VARCHAR` | The Employer Identification Number. |
| `description` | `VARCHAR` | A description of the company's business. |
| `category` | `VARCHAR` | The filer category (e.g., 'public-company'). |
| `fiscal_year_end` | `VARCHAR(4)` | The fiscal year end date (MMDD format). |
| `state_of_incorporation` | `VARCHAR` | The state or country of incorporation. |
| `phone` | `VARCHAR` | The company's phone number. |
| `flags` | `VARCHAR` | Internal flags used by the SEC. |
| `mailing_street1` | `VARCHAR` | Mailing address line 1. |
| `mailing_street2` | `VARCHAR` | Mailing address line 2. |
| `mailing_city` | `VARCHAR` | Mailing address city. |
| `mailing_state_or_country` | `VARCHAR` | Mailing address state or country. |
| `mailing_zip_code` | `VARCHAR` | Mailing address zip code. |
| `business_street1` | `VARCHAR` | Business address line 1. |
| `business_street2` | `VARCHAR` | Business address line 2. |
| `business_city` | `VARCHAR` | Business address city. |
| `business_state_or_country` | `VARCHAR` | Business address state or country. |
| `business_zip_code` | `VARCHAR` | Business address zip code. |
| `first_added_timestamp` | `TIMESTAMPTZ` | Timestamp when the company was first added to this database. |
| `last_parsed_timestamp` | `TIMESTAMPTZ` | Timestamp when the company's data was last updated. |

### `filings`

Stores metadata for each filing submitted to the SEC.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `accession_number` | `VARCHAR` | **Primary Key**. The unique identifier for a specific filing. |
| `cik` | `VARCHAR(10)` | The CIK of the filing entity. Foreign key to `companies.cik`. |
| `filing_date` | `DATE` | The date the filing was accepted by the SEC. |
| `report_date` | `DATE` | The end date of the reporting period covered by the filing. |
| `acceptance_datetime` | `TIMESTAMPTZ` | The exact date and time the filing was accepted. |
| `act` | `VARCHAR` | The Securities Act under which the filing was made (e.g., '34'). |
| `form` | `VARCHAR` | The type of form filed (e.g., '10-K', '8-K'). |
| `file_number` | `VARCHAR` | The SEC file number. |
| `film_number` | `VARCHAR` | The SEC film number. |
| `items` | `VARCHAR` | A description of the items covered in the filing. |
| `size` | `BIGINT` | The size of the filing in bytes. |
| `is_xbrl` | `BOOLEAN` | True if the filing contains XBRL data. |
| `is_inline_xbrl` | `BOOLEAN` | True if the filing is in Inline XBRL format. |
| `primary_document` | `VARCHAR` | The name of the primary document in the filing. |
| `primary_doc_description` | `VARCHAR` | The description of the primary document. |

### `xbrl_facts`

Stores individual XBRL data points from filings. This is the largest and most detailed table.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR(10)` | The CIK of the filing entity. |
| `accession_number` | `VARCHAR` | The accession number of the source filing. |
| `taxonomy` | `VARCHAR` | The XBRL taxonomy the tag belongs to (e.g., 'us-gaap'). |
| `tag_name` | `VARCHAR` | The name of the XBRL tag (e.g., 'Revenues', 'Assets'). |
| `unit` | `VARCHAR` | The unit of measurement (e.g., 'USD', 'shares'). |
| `period_end_date` | `DATE` | The end date of the period the fact pertains to. |
| `value_numeric` | `DOUBLE` | The numeric value of the fact, if applicable. |
| `value_text` | `VARCHAR` | The text value of the fact, if applicable. |
| `fy` | `INTEGER` | The fiscal year of the fact. |
| `fp` | `VARCHAR` | The fiscal period of the fact (e.g., 'Q1', 'FY'). |
| `form` | `VARCHAR` | The form type of the source filing. |
| `filed_date` | `DATE` | The date the source filing was accepted. |
| `frame` | `VARCHAR` | The time frame of the fact (e.g., 'CY2023Q1I'). |

*Composite Primary Key*: `(cik, accession_number, taxonomy, tag_name, unit, period_end_date, frame)`

### `xbrl_tags`

Stores definitions for XBRL tags.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `taxonomy` | `VARCHAR` | The XBRL taxonomy. |
| `tag_name` | `VARCHAR` | The name of the XBRL tag. |
| `label` | `VARCHAR` | The human-readable label for the tag. |
| `description` | `VARCHAR` | A detailed description of the tag's meaning. |

*Composite Primary Key*: `(taxonomy, tag_name)`

### `former_names`

Stores the history of company name changes.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR(10)` | The CIK of the company. |
| `former_name` | `VARCHAR` | The previous name of the company. |
| `date_from` | `TIMESTAMPTZ` | The date the name change became effective. |
| `date_to` | `TIMESTAMPTZ` | The date the name was no longer in use. |

*Composite Primary Key*: `(cik, former_name, date_from)`

### `tickers`

Maps CIKs to stock tickers and exchanges.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR(10)` | The CIK of the company. |
| `ticker` | `VARCHAR` | The stock ticker symbol. |
| `exchange` | `VARCHAR` | The stock exchange where the ticker is listed. |
| `source` | `VARCHAR` | The source of the ticker information. |

### `xbrl_facts_orphaned`

Stores XBRL facts that could not be associated with a valid filing in the `filings` table. The schema is identical to `xbrl_facts`.

---

## Supplementary Data Tables

These tables contain data gathered from external sources to supplement the EDGAR data.

### `stock_history`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key (part 1)**. The stock ticker. |
| `date` | `DATE` | **Primary Key (part 2)**. The date of the stock data. |
| `open` | `DOUBLE` | The opening price for the day. |
| `high` | `DOUBLE` | The highest price for the day. |
| `low` | `DOUBLE` | The lowest price for the day. |
| `close` | `DOUBLE` | The closing price for the day. |
| `adj_close` | `DOUBLE` | The closing price adjusted for dividends and stock splits. |
| `volume` | `BIGINT` | The number of shares traded during the day. |

### `yf_profile_metrics`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key**. The stock ticker. |
| `fetch_timestamp` | `TIMESTAMPTZ` | Timestamp when the profile data was fetched. |
| `cik` | `VARCHAR` | The CIK of the company, if available. |
| `sector` | `VARCHAR` | The company's business sector. |
| `industry` | `VARCHAR` | The company's specific industry. |
| `country` | `VARCHAR` | The country where the company is based. |
| `market_cap` | `BIGINT` | The company's market capitalization. |
| `beta` | `DOUBLE` | A measure of the stock's volatility in relation to the market. |
| `trailing_pe` | `DOUBLE` | The trailing Price-to-Earnings ratio. |
| `forward_pe` | `DOUBLE` | The forward Price-to-Earnings ratio. |
| `enterprise_value` | `BIGINT` | The company's total value. |
| `book_value` | `DOUBLE` | The company's book value per share. |
| `price_to_book` | `DOUBLE` | The Price-to-Book ratio. |
| `trailing_eps` | `DOUBLE` | The trailing Earnings Per Share. |
| `forward_eps` | `DOUBLE` | The forward Earnings Per Share. |
| `peg_ratio` | `DOUBLE` | The Price/Earnings to Growth ratio. |

### `yf_recommendations`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key (part 1)**. The stock ticker. |
| `recommendation_timestamp` | `TIMESTAMPTZ` | **Primary Key (part 2)**. Timestamp of the recommendation. |
| `firm` | `VARCHAR` | **Primary Key (part 3)**. The analyst firm making the recommendation. |
| `grade_from` | `VARCHAR` | The previous recommendation grade. |
| `grade_to` | `VARCHAR` | The new recommendation grade (e.g., 'Buy', 'Hold'). |
| `action` | `VARCHAR` | The action taken (e.g., 'up', 'down', 'init'). |

### `yf_major_holders`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key**. The stock ticker. |
| `fetch_timestamp` | `TIMESTAMPTZ` | Timestamp when the holder data was fetched. |
| `pct_insiders` | `DOUBLE` | Percentage of shares held by insiders. |
| `pct_institutions` | `DOUBLE` | Percentage of shares held by institutions. |

### `yf_stock_actions`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key (part 1)**. The stock ticker. |
| `action_date` | `DATE` | **Primary Key (part 2)**. The date of the action. |
| `action_type` | `VARCHAR` | **Primary Key (part 3)**. The type of action ('dividends' or 'splits'). |
| `value` | `DOUBLE` | The value of the dividend or split ratio. |

### `yf_income_statement`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key (part 1)**. The stock ticker. |
| `report_date` | `DATE` | **Primary Key (part 2)**. The date of the financial report. |
| `item_name` | `VARCHAR` | **Primary Key (part 3)**. The name of the line item (e.g., 'Total Revenue'). |
| `item_value` | `BIGINT` | The value of the line item. |

### `yf_balance_sheet`

Identical in structure to `yf_income_statement`, but for balance sheet items.

### `yf_cash_flow`

Identical in structure to `yf_income_statement`, but for cash flow items.

### `macro_economic_data`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `series_id` | `VARCHAR` | **Primary Key (part 1)**. The FRED series ID (e.g., 'GDP'). |
| `date` | `DATE` | **Primary Key (part 2)**. The date of the data point. |
| `value` | `DOUBLE` | The value of the economic indicator. |

### `market_risk_factors`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `date` | `DATE` | **Primary Key (part 1)**. The date of the data. |
| `factor_model` | `VARCHAR` | **Primary Key (part 2)**. The factor model used (e.g., 'Fama-French 5'). |
| `mkt_minus_rf` | `DOUBLE` | Market risk premium. |
| `smb` | `DOUBLE` | Small-Minus-Big factor. |
| `hml` | `DOUBLE` | High-Minus-Low factor. |
| `rmw` | `DOUBLE` | Robust-Minus-Weak factor. |
| `cma` | `DOUBLE` | Conservative-Minus-Aggressive factor. |
| `rf` | `DOUBLE` | The risk-free rate. |

---

## Operational & Logging Tables

These tables are used internally by the pipeline to track errors and manage data fetching.

### `stock_fetch_errors`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cik` | `VARCHAR` | The CIK of the company associated with the failed fetch. |
| `ticker` | `VARCHAR` | The ticker that failed to fetch. |
| `error_timestamp` | `TIMESTAMPTZ` | When the error occurred. |
| `error_type` | `VARCHAR` | The type of error (e.g., 'NoDataError'). |
| `error_message` | `VARCHAR` | The error message from the fetcher. |
| `start_date_req` | `DATE` | The requested start date for the failed fetch. |
| `end_date_req` | `DATE` | The requested end date for the failed fetch. |

### `yf_info_fetch_errors`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | The ticker that failed to fetch. |
| `error_timestamp` | `TIMESTAMPTZ` | When the error occurred. |
| `error_message` | `VARCHAR` | The error message from the fetcher. |

### `yf_untrackable_tickers`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `ticker` | `VARCHAR` | **Primary Key**. The ticker that is being temporarily ignored. |
| `reason` | `VARCHAR` | The reason for ignoring the ticker (e.g., 'NoDataFound'). |
| `last_failed_timestamp` | `TIMESTAMPTZ` | When the ticker last failed, used for expiration. |