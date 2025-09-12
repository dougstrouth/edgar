-- models/marts/dim_companies.sql
-- This model cleans up the company information and adds the primary ticker.

with companies as (
    select * from {{ source('edgar_raw', 'companies') }}
),

tickers as (
    select * from {{ source('edgar_raw', 'tickers') }}
),

primary_tickers as (
    select
        cik,
        ticker
    from tickers
    -- A simple way to pick one ticker: get the first one alphabetically.
    -- A more robust method might use other metadata if available.
    qualify row_number() over (partition by cik order by ticker) = 1
)

select
    c.*,
    pt.ticker as primary_ticker
from companies c
left join primary_tickers pt on c.cik = pt.cik