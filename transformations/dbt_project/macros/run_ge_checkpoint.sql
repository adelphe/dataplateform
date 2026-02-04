{% macro run_ge_checkpoint(checkpoint_name) %}
    {#-
    Placeholder macro for triggering a Great Expectations checkpoint
    after dbt model runs. This serves as an integration point between
    dbt and GE validation layers.

    In production, this would invoke GE via a custom materialization
    or post-hook. Currently logs the intent for observability.

    Args:
        checkpoint_name: Name of the Great Expectations checkpoint to run.

    Usage:
        {{ config(post_hook=run_ge_checkpoint('transactions_checkpoint')) }}
    -#}
    {{ log("Great Expectations checkpoint triggered: " ~ checkpoint_name, info=True) }}
{% endmacro %}
