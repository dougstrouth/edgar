-- models/marts/fct_combined_financials.sql

-- This model combines company financial facts with macroeconomic and market risk data.
-- It serves as a foundational "fact" table for analysis and machine learning.

WITH xbrl_facts_filtered AS (
    -- Select key financial facts, focusing on annual reports for simplicity
    SELECT
        cik,
        accession_number,
        tag_name,
        period_end_date,
        filed_date,
        value_numeric
    FROM {{ source('edgar_source', 'xbrl_facts') }}
    WHERE form = '10-K'
      AND tag_name IN ('Revenues', 'NetIncomeLoss', 'Assets', 'Liabilities', 'StockholdersEquity')
),

macro_data AS (
    -- Select and pivot key macro indicators
    SELECT * FROM {{ source('edgar_source', 'macro_economic_data') }}
    PIVOT (MAX(value) FOR series_id IN ('GDP', 'CPIAUCSL', 'UNRATE'))
    AS p(date, gdp, cpi, unrate)
),

market_risk_data AS (
    -- Select daily market risk factors
    SELECT * FROM {{ source('edgar_source', 'market_risk_factors') }}
    WHERE factor_model = 'ff_5_factor_daily'
)

SELECT
    facts.cik,
    facts.accession_number,
    facts.tag_name,
    facts.period_end_date,
    facts.filed_date,
    facts.value_numeric,
    -- Use ASOF JOIN to get the most recent data point on or before the report date
    macro.gdp,
    macro.cpi,
    macro.unrate,
    risk.mkt_minus_rf,
    risk.smb,
    risk.hml
FROM xbrl_facts_filtered AS facts
ASOF LEFT JOIN macro_data AS macro ON facts.period_end_date >= macro.date
ASOF LEFT JOIN market_risk_data AS risk ON facts.period_end_date >= risk.date