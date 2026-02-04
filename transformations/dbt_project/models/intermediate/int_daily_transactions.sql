{{ config(materialized='view') }}

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

daily_aggregation as (

    select
        date_trunc('day', transaction_timestamp)::date as transaction_date,
        count(*) as total_transactions,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        count(distinct customer_id) as unique_customers,
        count(distinct product_id) as unique_products,
        sum(amount * quantity) as total_revenue

    from transactions

    group by date_trunc('day', transaction_timestamp)::date

)

select * from daily_aggregation
