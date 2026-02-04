{{ config(materialized='table') }}

with transactions as (

    select * from {{ ref('int_transactions_enriched') }}

),

customer_analytics as (

    select
        customer_id,
        count(*) as total_transactions,
        sum(total_value) as total_spent,
        avg(amount) as avg_transaction_value,
        min(transaction_timestamp) as first_transaction_date,
        max(transaction_timestamp) as last_transaction_date,
        extract(day from max(transaction_timestamp) - min(transaction_timestamp)) as customer_lifetime_days,
        count(distinct product_id) as unique_products_purchased,
        sum(case when is_high_value then 1 else 0 end) as high_value_transaction_count,
        sum(quantity) as total_units_purchased

    from transactions

    group by customer_id

)

select * from customer_analytics
