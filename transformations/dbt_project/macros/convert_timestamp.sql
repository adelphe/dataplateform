{% macro convert_timestamp(column_name) %}
    {#-
    Converts various timestamp formats to a proper TIMESTAMP type.
    Handles ISO 8601 strings, epoch seconds, and existing timestamp columns.

    Args:
        column_name: The name of the column to convert.

    Returns:
        A SQL expression that casts the column to TIMESTAMP.

    Supported input formats:
        - Numeric types (integer, bigint, numeric, double precision): treated as Unix epoch seconds.
        - Text/varchar containing a numeric value (e.g. '1609459200'): treated as epoch seconds.
        - Text/varchar containing an ISO 8601 string (e.g. '2021-01-01T00:00:00Z'): parsed via cast.
        - Existing timestamp/timestamptz columns: cast directly.

    Usage examples:
        {{ convert_timestamp('epoch_column') }}
        {{ convert_timestamp('iso_string_column') }}
        {{ convert_timestamp('existing_ts_column') }}
    -#}
    case
        when pg_typeof({{ column_name }})::text in ('integer', 'bigint', 'numeric', 'double precision')
            then to_timestamp({{ column_name }}::double precision)::timestamp
        when pg_typeof({{ column_name }})::text in ('text', 'character varying')
            then case
                when {{ column_name }} ~ '^\s*\d+\.?\d*\s*$'
                    then to_timestamp({{ column_name }}::double precision)::timestamp
                else {{ column_name }}::timestamp
            end
        else {{ column_name }}::timestamp
    end
{% endmacro %}
