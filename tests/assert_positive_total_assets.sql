-- tests/assert_positive_total_assets.sql
--
-- This test now checks our dedicated debugging model for zero or null assets.
-- If the model `dbg_zero_or_null_assets` has any rows, this test will fail.
-- This makes debugging much easier: when the test fails, you can simply query
-- the `dbg_zero_or_null_assets` table to see all the details of the failing records.

select * from {{ ref('dbg_zero_or_null_assets') }}