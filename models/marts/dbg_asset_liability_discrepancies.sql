-- models/marts/dbg_asset_liability_discrepancies.sql

{{
    config(
        materialized='table'
    )
}}

-- This model is designed for debugging. It isolates records from fct_financials_quarterly
-- where the fundamental accounting equation (Assets >= Liabilities) appears to be violated.
-- By materializing the failing rows along with all their component data, we can easily
-- analyze exactly why the calculation is failing for specific companies.

with final_financials as (
    -- We select from the final fact table to get the calculated totals
    select * from {{ ref('fct_financials_quarterly') }}
),

pivoted_components as (
    -- We join back to the pivoted staging data to get the raw components for analysis
    select * from {{ ref('stg_financials_pivoted') }}
)

select
    -- Key identifiers
    ff.cik,
    ff.period_end_date,

    -- The final, calculated values that are failing the test
    ff.total_assets,
    ff.total_liabilities,
    (ff.total_liabilities - ff.total_assets) as discrepancy_amount,

    -- The raw, underlying components from the XBRL data
    pc.* exclude (cik, period_end_date)

from final_financials ff
left join pivoted_components pc
    on ff.cik = pc.cik and ff.period_end_date = pc.period_end_date
where
    ff.total_assets < ff.total_liabilities