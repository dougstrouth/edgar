-- models/marts/dbg_zero_or_null_assets.sql

{{
    config(
        materialized='table'
    )
}}

-- This model is for debugging. It isolates records where the final
-- calculated `total_assets` is NULL or non-positive, which indicates
-- a potential data quality issue or a gap in our model's logic.

with final_financials as (
    select * from {{ ref('fct_financials_quarterly') }}
),

pivoted_components as (
    select * from {{ ref('stg_financials_pivoted') }}
)

select
    -- Key identifiers
    ff.cik,
    ff.period_end_date,

    -- The final, calculated values that are failing the test
    ff.total_assets,
    ff.total_liabilities,

    -- The raw, underlying components from the XBRL data for analysis
    pc.* exclude (cik, period_end_date)

from final_financials ff
left join pivoted_components pc
    on ff.cik = pc.cik and ff.period_end_date = pc.period_end_date
where
    ff.total_assets <= 0 or ff.total_assets is null