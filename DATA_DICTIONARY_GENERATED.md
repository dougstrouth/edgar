<!-- Programmatically generated data dictionary -->
# EDGAR Analytics - Data Dictionary (Generated)

_This file was programmatically generated from the DuckDB schema._

- Generated at: 2025-11-26T00:00:00Z
- Source DB: `/Users/dougstrouth/datasets_noicloud/edgar/edgar_analytics.duckdb`

---

## Tables and Columns (schema: `main`)

### `companies`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `primary_name` | `VARCHAR` |
| `entity_name_cf` | `VARCHAR` |
| `entity_type` | `VARCHAR` |
| `sic` | `VARCHAR` |
| `sic_description` | `VARCHAR` |
| `ein` | `VARCHAR` |
| `description` | `INTEGER` |
| `category` | `VARCHAR` |
| `fiscal_year_end` | `VARCHAR` |
| `state_of_incorporation` | `VARCHAR` |
| `phone` | `VARCHAR` |
| `flags` | `INTEGER` |
| `first_added_timestamp` | `INTEGER` |
| `last_parsed_timestamp` | `TIMESTAMP WITH TIME ZONE` |
| `mailing_street1` | `VARCHAR` |
| `mailing_street2` | `VARCHAR` |
| `mailing_city` | `VARCHAR` |
| `mailing_state_or_country` | `VARCHAR` |
| `mailing_zip_code` | `VARCHAR` |
| `business_street1` | `VARCHAR` |
| `business_street2` | `VARCHAR` |
| `business_city` | `VARCHAR` |
| `business_state_or_country` | `VARCHAR` |
| `business_zip_code` | `VARCHAR` |
| `rn` | `BIGINT` |

### `downloaded_archives`

| Column Name | Data Type |
| :--- | :--- |
| `file_path` | `VARCHAR` |
| `file_name` | `VARCHAR` |
| `url` | `VARCHAR` |
| `size_bytes` | `BIGINT` |
| `local_last_modified_utc` | `TIMESTAMP WITH TIME ZONE` |
| `download_timestamp_utc` | `TIMESTAMP WITH TIME ZONE` |
| `status` | `VARCHAR` |

### `filing_summaries`

| Column Name | Data Type |
| :--- | :--- |
| `accession_number` | `VARCHAR` |
| `summary_text` | `VARCHAR` |
| `summary_model` | `VARCHAR` |
| `summary_date` | `TIMESTAMP WITH TIME ZONE` |

### `filings`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `accession_number` | `VARCHAR` |
| `filing_date` | `TIMESTAMP_NS` |
| `report_date` | `TIMESTAMP_NS` |
| `acceptance_datetime` | `TIMESTAMP WITH TIME ZONE` |
| `act` | `VARCHAR` |
| `form` | `VARCHAR` |
| `file_number` | `VARCHAR` |
| `film_number` | `VARCHAR` |
| `items` | `VARCHAR` |
| `size` | `BIGINT` |
| `is_xbrl` | `BOOLEAN` |
| `is_inline_xbrl` | `BOOLEAN` |
| `primary_document` | `VARCHAR` |
| `primary_doc_description` | `VARCHAR` |
| `rn` | `BIGINT` |

### `former_names`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `date_from` | `TIMESTAMP WITH TIME ZONE` |
| `date_to` | `TIMESTAMP WITH TIME ZONE` |
| `former_name` | `VARCHAR` |

### `macro_economic_data`

| Column Name | Data Type |
| :--- | :--- |
| `series_id` | `VARCHAR` |
| `date` | `DATE` |
| `value` | `DOUBLE` |

### `market_risk_factors`

| Column Name | Data Type |
| :--- | :--- |
| `date` | `DATE` |
| `factor_model` | `VARCHAR` |
| `mkt_minus_rf` | `DOUBLE` |
| `smb` | `DOUBLE` |
| `hml` | `DOUBLE` |
| `rmw` | `DOUBLE` |
| `cma` | `DOUBLE` |
| `rf` | `DOUBLE` |

### `massive_tickers`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `cik` | `VARCHAR` |
| `name` | `VARCHAR` |
| `market` | `VARCHAR` |
| `locale` | `VARCHAR` |
| `primary_exchange` | `VARCHAR` |
| `type` | `VARCHAR` |
| `active` | `BOOLEAN` |
| `currency_name` | `VARCHAR` |
| `currency_symbol` | `VARCHAR` |
| `base_currency_name` | `VARCHAR` |
| `base_currency_symbol` | `VARCHAR` |
| `composite_figi` | `VARCHAR` |
| `share_class_figi` | `VARCHAR` |
| `last_updated_utc` | `TIMESTAMP` |
| `delisted_utc` | `TIMESTAMP` |
| `source` | `VARCHAR` |
| `fetched_at` | `TIMESTAMP` |

### `polygon_untrackable_tickers`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `reason` | `VARCHAR` |
| `last_failed_timestamp` | `TIMESTAMP WITH TIME ZONE` |

### `prioritized_tickers_info_backlog`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `cik` | `VARCHAR` |
| `unique_tag_count` | `BIGINT` |
| `key_metric_count` | `INTEGER` |
| `has_stock_data` | `INTEGER` |
| `recent_filings` | `BIGINT` |
| `exchange_score` | `DOUBLE` |
| `score` | `DOUBLE` |
| `rank` | `BIGINT` |
| `generated_at` | `TIMESTAMP WITH TIME ZONE` |
| `weights_json` | `VARCHAR` |

### `prioritized_tickers_stock_backlog`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `cik` | `VARCHAR` |
| `unique_tag_count` | `BIGINT` |
| `key_metric_count` | `INTEGER` |
| `last_date` | `TIMESTAMP` |
| `record_count` | `BIGINT` |
| `recent_filings` | `BIGINT` |
| `stock_need_score` | `BIGINT` |
| `staleness_days` | `BIGINT` |
| `score` | `DOUBLE` |
| `rank` | `BIGINT` |
| `generated_at` | `TIMESTAMP WITH TIME ZONE` |
| `weights_json` | `VARCHAR` |
| `start_date` | `TIMESTAMP` |
| `end_date` | `TIMESTAMP` |

### `stock_fetch_plan`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `rank` | `BIGINT` |
| `score` | `DOUBLE` |
| `cik` | `VARCHAR` |
| `unique_tag_count` | `BIGINT` |
| `key_metric_count` | `INTEGER` |
| `last_date` | `TIMESTAMP` |
| `record_count` | `BIGINT` |
| `backlog_staleness_days` | `BIGINT` |
| `status` | `VARCHAR` |
| `start_date` | `TIMESTAMP` |
| `end_date` | `TIMESTAMP` |
| `generated_at` | `TIMESTAMP WITH TIME ZONE` |

### `stock_history`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `date` | `DATE` |
| `open` | `DOUBLE` |
| `high` | `DOUBLE` |
| `low` | `DOUBLE` |
| `close` | `DOUBLE` |
| `adj_close` | `DOUBLE` |
| `volume` | `BIGINT` |

### `tickers`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `ticker` | `VARCHAR` |
| `exchange` | `VARCHAR` |
| `source` | `VARCHAR` |

### `updated_ticker_info`

| Column Name | Data Type |
| :--- | :--- |
| `ticker` | `VARCHAR` |
| `cik` | `VARCHAR` |
| `name` | `VARCHAR` |
| `market` | `VARCHAR` |
| `locale` | `VARCHAR` |
| `primary_exchange` | `VARCHAR` |
| `type` | `VARCHAR` |
| `active` | `BOOLEAN` |
| `currency_name` | `VARCHAR` |
| `currency_symbol` | `VARCHAR` |
| `base_currency_name` | `VARCHAR` |
| `base_currency_symbol` | `VARCHAR` |
| `composite_figi` | `VARCHAR` |
| `share_class_figi` | `VARCHAR` |
| `description` | `VARCHAR` |
| `homepage_url` | `VARCHAR` |
| `total_employees` | `BIGINT` |
| `list_date` | `VARCHAR` |
| `sic_code` | `VARCHAR` |
| `sic_description` | `VARCHAR` |
| `ticker_root` | `VARCHAR` |
| `source_feed` | `VARCHAR` |
| `market_cap` | `DOUBLE` |
| `weighted_shares_outstanding` | `BIGINT` |
| `round_lot` | `INTEGER` |
| `last_updated_utc` | `TIMESTAMP WITH TIME ZONE` |
| `delisted_utc` | `TIMESTAMP WITH TIME ZONE` |
| `address_1` | `VARCHAR` |
| `city` | `VARCHAR` |
| `state` | `VARCHAR` |
| `postal_code` | `VARCHAR` |
| `logo_url` | `VARCHAR` |
| `icon_url` | `VARCHAR` |
| `fetch_timestamp` | `TIMESTAMP WITH TIME ZONE` |

### `xbrl_facts`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `accession_number` | `VARCHAR` |
| `taxonomy` | `VARCHAR` |
| `tag_name` | `VARCHAR` |
| `unit` | `VARCHAR` |
| `period_end_date` | `TIMESTAMP_NS` |
| `value_numeric` | `DOUBLE` |
| `value_text` | `INTEGER` |
| `fy` | `BIGINT` |
| `fp` | `VARCHAR` |
| `form` | `VARCHAR` |
| `filed_date` | `TIMESTAMP_NS` |
| `frame` | `VARCHAR` |

### `xbrl_facts_orphaned`

| Column Name | Data Type |
| :--- | :--- |
| `cik` | `VARCHAR` |
| `taxonomy` | `VARCHAR` |
| `tag_name` | `VARCHAR` |
| `accession_number` | `VARCHAR` |
| `unit` | `VARCHAR` |
| `period_end_date` | `TIMESTAMP_NS` |
| `value_numeric` | `DOUBLE` |
| `value_text` | `INTEGER` |
| `fy` | `BIGINT` |
| `fp` | `VARCHAR` |
| `form` | `VARCHAR` |
| `filed_date` | `TIMESTAMP_NS` |
| `frame` | `VARCHAR` |

### `xbrl_tags`

| Column Name | Data Type |
| :--- | :--- |
| `taxonomy` | `VARCHAR` |
| `tag_name` | `VARCHAR` |
| `label` | `VARCHAR` |
| `description` | `VARCHAR` |
| `rn` | `BIGINT` |

### `yf_fetch_status`

| Column Name | Data Type |
| :--- | :--- |
| `fetch_date` | `DATE` |
| `fetched_count` | `INTEGER` |
| `attempted_count` | `INTEGER` |
