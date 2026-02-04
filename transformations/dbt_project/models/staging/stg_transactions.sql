{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'transactions') }}

),

cleaned as (

    select
        transaction_id,
        customer_id,
        product_id,
        cast(amount as numeric(18, 2)) as amount,
        upper(trim(currency)) as currency,
        cast(quantity as integer) as quantity,
        {{ convert_timestamp('timestamp') }} as transaction_timestamp,
        current_timestamp as _loaded_at

    from source

    where
        transaction_id is not null
        and amount > 0
        and quantity > 0

)

select * from cleaned
