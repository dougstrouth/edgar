-- models/marts/fct_financials_quarterly.sql
-- This final "mart" model consumes the pivoted staging data and applies
-- robust business logic to calculate final, analytics-ready financial metrics.

with pivoted as (
    select * from {{ ref('stg_financials_pivoted') }}
),

final as (
    select
        cik,
        period_end_date,

        -- Robust asset calculation: Prioritize the most reliable tags.
        coalesce(
            LiabilitiesAndStockholdersEquity, -- Best source for Total Assets
            Assets,                           -- Second best source
            coalesce(AssetsCurrent, 0) + coalesce(AssetsNoncurrent, 0)
        ) as total_assets,

        -- Robust liabilities calculation.
        coalesce(
            Liabilities,
            coalesce(LiabilitiesCurrent, 0) + coalesce(LiabilitiesNoncurrent, 0)
        ) as total_liabilities,

        -- Robust equity calculation.
        coalesce(
            StockholdersEquity, -- Best source for Equity
            -- Fallback: Derive from the accounting equation using our most reliable asset and liability figures
            coalesce(LiabilitiesAndStockholdersEquity, Assets, coalesce(AssetsCurrent, 0) + coalesce(AssetsNoncurrent, 0)) -
            coalesce(Liabilities, coalesce(LiabilitiesCurrent, 0) + coalesce(LiabilitiesNoncurrent, 0))
        ) as total_equity,

        Revenues as revenues,
        NetIncomeLoss as net_income
    from pivoted
)

select * from final