{{ config(materialized='table') }}

with transactions as (

    select * from {{ ref('int_transactions_enriched') }}

),

product_analytics as (

    select
        product_id,
        count(*) as total_transactions,
        sum(quantity) as total_units_sold,
        sum(total_value) as total_revenue,
        count(distinct customer_id) as unique_customers,
        avg(amount) as avg_price_per_unit,
        min(transaction_timestamp) as first_sale_date,
        max(transaction_timestamp) as last_sale_date,
        sum(case when is_high_value then 1 else 0 end) as high_value_transaction_count

    from transactions

    group by product_id

)

select * from product_analytics
