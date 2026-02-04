-- Custom data test: validates that all transaction amounts are positive.
-- dbt fails this test if any rows are returned.

select
    transaction_id,
    amount
from {{ ref('stg_transactions') }}
where amount <= 0
