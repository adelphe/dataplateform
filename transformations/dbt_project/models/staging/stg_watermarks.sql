{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_platform', 'watermarks') }}

),

cleaned as (

    select
        source_name,
        watermark_column,
        watermark_value,
        watermark_type,
        updated_at

    from source

)

select * from cleaned
