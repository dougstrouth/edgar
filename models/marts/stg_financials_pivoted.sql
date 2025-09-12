-- models/staging/stg_financials_pivoted.sql
-- This staging model handles the complex task of pivoting the raw, long-format
-- xbrl_facts table into a wide format. This logic is isolated here so it can be
-- reused by multiple downstream models.

with facts as (
    select * from {{ source('edgar_raw', 'xbrl_facts') }}
),

-- Filter for common annual/quarterly tags from 10-K/10-Q
quarterly_facts as (
    select
        cik,
        period_end_date,
        tag_name,
        value_numeric
    from facts
    where
        form in ('10-K', '10-Q')
        and taxonomy = 'us-gaap'
        and tag_name in (
            'Assets', 'AssetsCurrent', 'AssetsNoncurrent',
            'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
            'StockholdersEquity', 'LiabilitiesAndStockholdersEquity',
            'Revenues', 'NetIncomeLoss'
        )
        and unit = 'USD'
    group by all
)

-- Use DuckDB's native PIVOT for a cleaner and more efficient transformation
PIVOT quarterly_facts
ON tag_name IN ('Assets', 'AssetsCurrent', 'AssetsNoncurrent', 'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent', 'StockholdersEquity', 'LiabilitiesAndStockholdersEquity', 'Revenues', 'NetIncomeLoss')
USING MAX(value_numeric)
GROUP BY cik, period_end_date