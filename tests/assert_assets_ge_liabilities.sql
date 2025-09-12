-- tests/assert_assets_ge_liabilities.sql
--
-- This test now checks our dedicated debugging model.
-- If the model `dbg_asset_liability_discrepancies` has any rows, this test will fail.
-- This makes debugging much easier: when the test fails, you can simply query
-- the `dbg_asset_liability_discrepancies` table to see all the details of the failing records.

select * from {{ ref('dbg_asset_liability_discrepancies') }}