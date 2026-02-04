{{
    config(
        materialized='incremental',
        unique_key='date',
        incremental_strategy='delete+insert'
    )
}}

with daily as (

    select * from {{ ref('int_daily_transactions') }}

),

daily_summary as (

    select
        transaction_date as date,
        total_transactions,
        total_revenue,
        total_amount,
        avg_amount as avg_transaction_value,
        unique_customers,
        unique_products

    from daily

    {% if is_incremental() %}
    where transaction_date >= (select max(date) from {{ this }})
    {% endif %}

)

select * from daily_summary
