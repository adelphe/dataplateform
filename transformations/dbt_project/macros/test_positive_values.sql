{% test positive_values(model, column_name) %}
{#-
Custom schema test to validate that a numeric column contains only positive values.
Returns rows that violate the constraint (value <= 0).
dbt considers the test failed if any rows are returned.

Usage in YAML:
    columns:
      - name: amount
        tests:
          - positive_values
-#}

select
    {{ column_name }} as invalid_value
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
