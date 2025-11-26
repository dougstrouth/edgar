# Database Table Relationships (EDGAR Analytics)

This document summarizes the core tables and how they relate. It is intended to complement `DATA_DICTIONARY.md` and guide validations and query design.

## Core Entities

- Companies (`companies`)
  - Key: `cik` (unique per company)
  - Attributes: legal name, state, sic, etc.
- Filings (`filings`)
  - Key: `filing_id` (unique per filing)
  - Attributes: `cik`, `form_type`, `filed_date`, `period_of_report`, `accession`, etc.
- Former Names (`former_names`)
  - Key: composite unique across (`cik`, `name`, `from_date`)
  - Attributes: `cik`, `name`, `from_date`, `to_date`
- Tickers (`tickers`)
  - Key: `ticker` (upper-case, unique)
  - Attributes: `cik`, `name`, `exchange`, `active`, etc.
- XBRL Tags (`xbrl_tags`)
  - Key: `tag_id` (surrogate) or unique tag-qualified name
  - Attributes: `name`, `namespace`, `datatype`, etc.
- XBRL Facts (`xbrl_facts`)
  - Key: `fact_id` (surrogate)
  - Attributes: `cik`, `filing_id`, `tag_id`, `value`, `unit`, `fy`, `fp`, `asof_date`
- Stock History (`stock_history`)
  - Key: composite (`ticker`, `date`)
  - Attributes: `open`, `high`, `low`, `close`, `volume`, `adj_close`

## Relationships

- Company ↔ Filings
  - `filings.cik` references `companies.cik`
  - One company to many filings.
- Company ↔ Former Names
  - `former_names.cik` references `companies.cik`
  - One company to many former names with date ranges.
- Company ↔ Tickers
  - `tickers.cik` references `companies.cik`
  - One company to zero-or-many tickers (historical mappings), typically one active.
- Filings ↔ XBRL Facts
  - `xbrl_facts.filing_id` references `filings.filing_id`
  - `xbrl_facts.cik` should match `filings.cik` for the filing (consistency check).
- XBRL Facts ↔ XBRL Tags
  - `xbrl_facts.tag_id` references `xbrl_tags.tag_id`
  - Many facts per tag.
- Tickers ↔ Stock History
  - `stock_history.ticker` references `tickers.ticker`
  - One ticker to many daily bars.

## Validation Implications

The validation suite should ensure the following when tables are present:

- Keys & Uniqueness
  - `companies.cik` is unique and non-null.
  - `filings.filing_id` is unique and non-null.
  - `tickers.ticker` is unique and non-null; optional uniqueness across (`ticker`, `cik`) if modeled.
  - `stock_history` has no duplicates across (`ticker`, `date`).
  - `xbrl_tags` unique constraint on (`name`, `namespace`) if used instead of `tag_id`.

- Referential Integrity (FK-style checks)
  - Every `filings.cik` exists in `companies.cik`.
  - Every `former_names.cik` exists in `companies.cik`.
  - Every `tickers.cik` exists in `companies.cik` (where `cik` not null).
  - Every `xbrl_facts.filing_id` exists in `filings.filing_id`.
  - Every `xbrl_facts.tag_id` exists in `xbrl_tags.tag_id`.
  - If both tables exist: every `stock_history.ticker` exists in `tickers.ticker`.

- Consistency & Coverage
  - `xbrl_facts.cik` equals the `filings.cik` for its `filing_id` (join consistency).
  - No extreme orphan rates (e.g., `stock_history` tickers missing from `tickers` above a small threshold) — report count and sample.
  - Ticker-to-company mapping is consistent: if multiple tickers share a `cik`, active flags or dates should delineate history.

- Temporal/Data Quality Checks
  - `filings.filed_date` and `period_of_report` are valid dates; `filed_date >= period_of_report` when `period_of_report` provided.
  - `stock_history.high >= stock_history.low` and all OHLCV non-negative; `volume` integer-like.
  - Reasonable date ranges: `stock_history.date` within plausible market calendar windows; no future dates beyond a small grace.
  - `xbrl_facts.asof_date` aligns with `filings.filed_date` window depending on fact type.

## Notes on Optional Tables

- Validations are gated by table existence to avoid penalizing environments that only load EDGAR core without market data.
- Where surrogate keys are used (`filing_id`, `tag_id`, `fact_id`), prefer FK-style checks plus consistency checks on natural keys (`cik`).

## Query Patterns

- Latest Company Name:
  - Join `companies` with `former_names` using date ranges; prefer `companies.name` for current unless overlapping.
- Company Filings:
  - Filter `filings` by `cik` and `form_type`, order by `filed_date`.
- Ticker-to-Company:
  - Join `tickers` to `companies` on `cik`; consider active flag/date-effective where modeled.
- Daily Bars for Ticker:
  - Filter `stock_history` by `ticker`, order by `date`.
- Facts for Filing:
  - Join `xbrl_facts` to `filings` on `filing_id`, and to `xbrl_tags` on `tag_id`.

## Future Enhancements

- Add explicit unique indices where beneficial (DuckDB supports `UNIQUE` on create or `PRIMARY KEY` metadata in newer versions).
- Track effective dates for ticker–cik mappings to model historical changes explicitly.
- Introduce lightweight dimension tables (e.g., `exchanges`) if normalization improves clarity.