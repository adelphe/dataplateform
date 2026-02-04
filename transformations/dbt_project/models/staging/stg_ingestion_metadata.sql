{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_platform', 'ingestion_metadata') }}

),

cleaned as (

    select
        id as ingestion_id,
        source_name,
        source_type,
        execution_date,
        row_count,
        file_size_bytes,
        checksum,
        status,
        error_message,
        created_at,
        updated_at

    from source

)

select * from cleaned
