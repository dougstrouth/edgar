-- tests/assert_assets_ge_liabilities.sql
--
-- A custom data test to ensure that for any given financial report,
-- the reported Assets are greater than or equal to Liabilities.
-- This is a fundamental accounting principle.
-- A test in dbt passes if it returns 0 rows.

select
    cik,
    period_end_date,
    Assets as assets_value,
    Liabilities as liabilities_value
from {{ ref('fct_financials_quarterly') }}
where
    Assets < Liabilities