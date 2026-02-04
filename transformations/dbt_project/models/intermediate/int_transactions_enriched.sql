{{ config(materialized='view') }}

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

enriched as (

    select
        transaction_id,
        customer_id,
        product_id,
        amount,
        currency,
        quantity,
        transaction_timestamp,
        amount * quantity as total_value,
        extract(hour from transaction_timestamp) as transaction_hour,
        extract(dow from transaction_timestamp) as transaction_day_of_week,
        case
            when amount > 100 then true
            else false
        end as is_high_value,
        case
            when extract(hour from transaction_timestamp) between 9 and 17 then 'business_hours'
            else 'off_hours'
        end as time_category,
        _loaded_at

    from transactions

)

select * from enriched
