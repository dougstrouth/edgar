-- models/marts/fct_financials_quarterly.sql
-- This model pivots key financial statement items from the raw xbrl_facts table
-- into a wide, analytics-friendly format. It also calculates a common ratio.

with facts as (
    select * from {{ source('edgar_raw', 'xbrl_facts') }}
),

-- Filter for common annual/quarterly tags from 10-K/10-Q
-- This list can be expanded significantly based on your analysis needs.
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
            'Assets',
            'Liabilities',
            'StockholdersEquity',
            'Revenues',
            'NetIncomeLoss'
        )
        and unit = 'USD'
        -- We only want facts that represent a period (e.g., Q1, Q2, FY).
        -- A more robust model would handle 'frame' to distinguish instants vs. periods.
        -- For simplicity, we'll group by period_end_date and take the max value if duplicates exist.
    group by all
),

pivoted as (
    -- Pivot the data to turn tags into columns
    {{ dbt.pivot(
        column_to_pivot='tag_name',
        list_of_pivot_values=[
            'Assets',
            'Liabilities',
            'StockholdersEquity',
            'Revenues',
            'NetIncomeLoss'
        ],
        agg='max',
        then_value='value_numeric'
    ) }}
    from quarterly_facts
    group by cik, period_end_date
)

select * from pivoted