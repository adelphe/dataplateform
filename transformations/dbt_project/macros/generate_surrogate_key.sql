{% macro generate_surrogate_key(columns) %}
    {#-
    Generates a surrogate key by hashing the concatenation of the given columns.
    Uses MD5 to create a deterministic unique identifier from composite keys.

    Args:
        columns: A list of column names to combine into the surrogate key.

    Returns:
        A SQL expression producing an MD5 hash string.
    -#}
    md5(
        {%- for column in columns %}
            coalesce(cast({{ column }} as varchar), '_null_')
            {%- if not loop.last %} || '|' || {% endif -%}
        {%- endfor %}
    )
{% endmacro %}
